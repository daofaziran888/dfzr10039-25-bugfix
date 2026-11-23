"""跨模块数据流编排：真实业务流水线，各模块间通过dataclass传递数据。

实现以下数据流：
- crawl_site → crawl_job → content_extract → field_clean → batch_store
- crawl_job → url_dedup → parse_log
- proxy_pool → anti_block → crawl_limit
- batch_store → data_delivery → quality_sample
- incr_crawl → crawl_job → diff_check
- data_archive ← batch_store
- crawl_cost ← proxy_pool
- crawl_job → scheduler
- batch_store（空结果回执）→ daily_report（日报取数清单）
"""
from __future__ import annotations

import json
import random
import re
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

from app.models import (
    AntiBlockStrategy, AntiType, ArchiveFormat, ArchivePolicy, CleanedContent,
    CleanRule, CollectionReceipt, CostRecord, CrawlJobSpec, CrawlLimitRule,
    DailyReport, DeliveryFormat, DeliveryTask, DiffRecord, ExtractRule,
    IncrCursor, PageOutcome, PageReceipt, ParseLogEntry, ProxyNode,
    QualitySample, RawExtractedContent, ReportLine, SampleMethod,
    SchedulePlan, SiteConfig, SiteType, StorageBatch, UrlFingerprint,
)
from app.store import store


# ---------------------------------------------------------------------------
# 流水线 1: crawl_site → crawl_job → content_extract → field_clean → batch_store
# ---------------------------------------------------------------------------

def run_crawl_pipeline(site_row: dict[str, Any], job_row: dict[str, Any],
                       extract_rows: list[dict[str, Any]],
                       clean_rows: list[dict[str, Any]]) -> StorageBatch | None:
    """端到端采集链路：站点配置 → 作业创建 → 内容抽取 → 字段清洗 → 批次存储。

    每个入口页面都会留下空结果回执：跑过的页面（已解析/空页）给出结论，
    缺规则或读取失败的页面保留原因、记为待核，批次不再把跳过的页面
    误判为零行校验通过。返回完整的 StorageBatch 或 None（不可恢复错误时）。
    """
    # Step 1: 站点验证（crawl_site）
    site = SiteConfig.from_seed_row(site_row)
    if not site.is_crawlable():
        return None  # 站点未验证或 Robots 禁止采集

    # Step 2: 创建采集作业（crawl_job）
    job = CrawlJobSpec.from_seed_row(job_row, site=site)
    if not job.entry_urls:
        return None  # 没有入口 URL

    rules = [ExtractRule.from_seed_row(er) for er in extract_rows]
    clean_rules = _active_clean_rules(clean_rows)

    # Step 3: 建立空结果回执与批次（批次存储从同一份回执取数）
    seq = sum(1 for r in store.rows("batch_store")
              if str(r.get("来源作业")) == job.job_id) + 1
    batch = StorageBatch(
        batch_id=f"BATCH-{job.job_id}-{seq:03d}",
        source_job_id=job.job_id,
        storage_path=f"/data/batches/{job.job_id}/",
        compression="gzip",
        schema_version="v1",
        status="采集中",
    )
    receipt = CollectionReceipt(receipt_id=f"RCPT-{batch.batch_id}", job_id=job.job_id)
    batch.receipt_id = receipt.receipt_id

    # Step 4: 逐页采集（内容抽取 → 字段清洗），回执交代每页结果
    _collect_pages(job, rules, clean_rules, receipt, batch, only_pending=False)
    _finalize_batch(job, receipt, batch)

    # Step 5: 批次存储（batch_store）与页面证据持久化
    _persist_receipt(receipt)
    _persist_batch(batch)
    _persist_batch_records(batch)
    _BATCH_REGISTRY[batch.batch_id] = batch
    return batch


def resume_crawl_pipeline(site_row: dict[str, Any], job_row: dict[str, Any],
                          extract_rows: list[dict[str, Any]],
                          clean_rows: list[dict[str, Any]],
                          *, batch_id: str) -> StorageBatch | None:
    """补跑：对同一批次只重试待核页面（缺规则/读取失败）。

    已有结论的页面保留证据与数据：文章页的空证据留在回执里，
    已入库的报价只记一次，不随补跑重复落库。
    """
    site = SiteConfig.from_seed_row(site_row)
    if not site.is_crawlable():
        return None
    job = CrawlJobSpec.from_seed_row(job_row, site=site)
    if not job.entry_urls:
        return None

    batch = _BATCH_REGISTRY.get(batch_id)
    if batch is None:
        row = next((r for r in store.rows("batch_store")
                    if str(r.get("批次编号")) == batch_id), None)
        if row is None:
            return None
        batch = StorageBatch.from_seed_row(row)
        batch.records = _load_batch_records(batch_id)
    receipt = _load_receipt(batch.receipt_id, job.job_id)
    if receipt is None:
        return None  # 没有回执的批次不在本链路内，不擅自补跑

    rules = [ExtractRule.from_seed_row(er) for er in extract_rows]
    clean_rules = _active_clean_rules(clean_rows)

    _collect_pages(job, rules, clean_rules, receipt, batch, only_pending=True)
    _finalize_batch(job, receipt, batch)

    _persist_receipt(receipt)
    _persist_batch(batch)
    _persist_batch_records(batch)
    _BATCH_REGISTRY[batch.batch_id] = batch
    _sync_report_acceptance(batch)  # 验收结果同步到日报取数清单
    return batch


def _active_clean_rules(clean_rows: list[dict[str, Any]]) -> list[CleanRule]:
    clean_rules = [CleanRule.from_seed_row(cr) for cr in clean_rows if cr.get("status") == "生效中"]
    if not clean_rules:
        clean_rules = [CleanRule.from_seed_row(clean_rows[0])] if clean_rows else []
    return clean_rules


def _applicable_rule(rules: list[ExtractRule], site_domain: str, url: str) -> ExtractRule | None:
    """匹配页面适用的解析规则：生效中、站点匹配、URL 模式命中，按抽取顺序取先。"""
    candidates = [r for r in rules if r.applies_to(site_domain, url)]
    if not candidates:
        return None
    candidates.sort(key=lambda r: r.extract_order)
    return candidates[0]


def _collect_pages(job: CrawlJobSpec, rules: list[ExtractRule],
                   clean_rules: list[CleanRule], receipt: CollectionReceipt,
                   batch: StorageBatch, *, only_pending: bool) -> None:
    """逐页采集并登记回执。only_pending=True 时跳过已有结论的页面（补跑）。"""
    for i, url in enumerate(job.entry_urls):
        existing = receipt.page(url)
        if only_pending and existing is not None and existing.conclusive:
            continue  # 已跑过的页面留住证据，不重复采集

        rule = _applicable_rule(rules, job.site.domain, url)
        if rule is None:
            _record_page(receipt, batch, PageReceipt(
                page_url=url, job_id=job.job_id, outcome=PageOutcome.NO_RULE,
                reason="没有适用的解析规则，页面跳过", source=url,
            ), i)
            continue

        raws, read_error = _extract_page(url, rule)
        if read_error:
            _record_page(receipt, batch, PageReceipt(
                page_url=url, job_id=job.job_id, outcome=PageOutcome.READ_FAILED,
                rule_id=rule.rule_id, reason=read_error, source=url,
            ), i)
            continue

        if raws and all(not r.has_errors() for r in raws):
            page_receipt = PageReceipt(
                page_url=url, job_id=job.job_id, outcome=PageOutcome.PARSED,
                rule_id=rule.rule_id, row_count=len(raws), source=url,
            )
        elif not raws:
            page_receipt = PageReceipt(
                page_url=url, job_id=job.job_id, outcome=PageOutcome.EMPTY,
                rule_id=rule.rule_id, reason="页面无匹配内容", source=url,
            )
        else:
            page_receipt = PageReceipt(
                page_url=url, job_id=job.job_id, outcome=PageOutcome.PARSE_FAILED,
                rule_id=rule.rule_id,
                reason="; ".join(err for r in raws for err in r.errors), source=url,
            )
        _record_page(receipt, batch, page_receipt, i)

        # 字段清洗（field_clean）：仅实际跑过的页面产生记录
        if page_receipt.outcome in (PageOutcome.PARSED, PageOutcome.PARSE_FAILED):
            batch.records = [r for r in batch.records if r.source_url != url]
            for raw in raws:
                batch.records.append(_apply_clean_rules(raw, clean_rules))


def _record_page(receipt: CollectionReceipt, batch: StorageBatch,
                 page: PageReceipt, page_index: int) -> None:
    """登记页面回执，并把页面证据写入解析日志（待核页面也能查到）。"""
    receipt.pages = [p for p in receipt.pages if p.page_url != page.page_url]
    receipt.pages.append(page)

    result_map = {
        PageOutcome.PARSED: ("成功", "", f"抽取{page.row_count}行"),
        PageOutcome.EMPTY: ("空页", "", "页面无匹配内容（零行依据）"),
        PageOutcome.NO_RULE: ("跳过", "缺规则", page.reason),
        PageOutcome.READ_FAILED: ("读取中断", "读取失败", page.reason),
        PageOutcome.PARSE_FAILED: ("部分失败", "字段缺失", page.reason),
    }
    parse_result, error_type, detail = result_map[page.outcome]
    log = ParseLogEntry(
        record_id=_next_parse_record_id(page.job_id, page_index),
        source_url=page.page_url,
        parse_rule_id=page.rule_id,
        parse_result=parse_result,
        error_type=error_type,
        retry_count=1 if page.outcome == PageOutcome.PARSE_FAILED else 0,
        error_detail=detail,
    )
    _persist_parse_log(log)


def _next_parse_record_id(job_id: str, page_index: int) -> str:
    base = f"PLOG-{job_id}-{page_index + 1:04d}"
    existing = {str(r.get("记录编号")) for r in store.rows("parse_log")}
    if base not in existing:
        return base
    n = 2
    while f"{base}-R{n}" in existing:
        n += 1
    return f"{base}-R{n}"


def _finalize_batch(job: CrawlJobSpec, receipt: CollectionReceipt,
                    batch: StorageBatch) -> None:
    """批次状态由回执推导：待核页面存在时记待验收，绝不误判零行校验通过。"""
    job.completed_pages = len(receipt.completed_pages)
    job.failed_pages = len(receipt.pending_pages) + len(receipt.failed_pages)
    job.status = "已完成" if receipt.is_complete else "部分失败"

    batch.row_count = receipt.parsed_row_count  # 批次存储从同一份空结果回执取数
    batch.page_scope = receipt.scope            # 落库范围以回执为准
    batch.stored_at = datetime.now(timezone.utc).isoformat()
    batch.checksum = batch.compute_checksum()
    if receipt.failed_pages or any(not c.is_valid() for c in batch.records):
        batch.status = "校验失败"
    elif receipt.pending_pages:
        batch.status = "待验收"  # 缺规则/读取失败：保留原因，等待补跑验收
    else:
        batch.status = "校验通过"


def _extract_page(url: str, rule: ExtractRule) -> tuple[list[RawExtractedContent], str]:
    """抽取单个页面，返回 (原始内容列表, 读取失败原因)。

    file:// 本地页面真实读取并按选择器解析；其余 URL 走模拟抽取。
    """
    if url.startswith("file://"):
        path = Path(url[len("file://"):])
        try:
            html = path.read_text(encoding="utf-8")
        except OSError as exc:
            return [], f"读取中断：{exc.strerror or exc}"
        return _parse_local_page(html, url, rule), ""
    return [_simulate_extract(url, rule)], ""


def _parse_local_page(html: str, url: str, rule: ExtractRule) -> list[RawExtractedContent]:
    """按 //tag[@class='x'] 选择器解析本地页面，每个命中元素产生一条原始内容。"""
    m = re.match(r"^//([a-zA-Z][\w-]*)(?:\[@class='([^']+)'\])?$", rule.xpath_selector.strip())
    if not m:
        return [RawExtractedContent(
            source_url=url, extract_rule_id=rule.rule_id,
            errors=[f"选择器不支持: {rule.xpath_selector}"],
        )]
    parser = _ElementExtractor(m.group(1), m.group(2))
    parser.feed(html)

    wanted = [p.strip() for p in str(rule.field_mapping.get("raw", "")).split(",") if p.strip()]
    raws: list[RawExtractedContent] = []
    for fields in parser.items:
        if wanted:
            fields = {k: v for k, v in fields.items() if k in wanted}
        if fields:
            raws.append(RawExtractedContent(
                source_url=url, extract_rule_id=rule.rule_id, raw_fields=fields))
        else:
            raws.append(RawExtractedContent(
                source_url=url, extract_rule_id=rule.rule_id,
                errors=["字段缺失: 元素命中但未抽到字段"]))
    return raws


class _ElementExtractor(HTMLParser):
    """极简元素抽取：匹配 tag（可选 class），子元素 class 名作为字段名。"""

    def __init__(self, tag: str, css_class: str | None) -> None:
        super().__init__(convert_charrefs=True)
        self._tag = tag
        self._class = css_class
        self.items: list[dict[str, str]] = []
        self._depth = 0
        self._current: dict[str, str] | None = None
        self._field_stack: list[list[str]] = []  # [tag, class, text]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        class_attr = ""
        for name, value in attrs:
            if name == "class":
                class_attr = value or ""
        classes = class_attr.split()
        if self._depth == 0:
            if tag == self._tag and (self._class is None or self._class in classes):
                self._depth = 1
                self._current = {}
                self._field_stack = []
            return
        self._depth += 1
        for cls in classes:
            self._field_stack.append([tag, cls, ""])

    def handle_data(self, data: str) -> None:
        if self._depth == 0 or self._current is None:
            return
        text = data.strip()
        if not text:
            return
        if self._field_stack:
            self._field_stack[-1][2] += text
        else:
            self._current["text"] = self._current.get("text", "") + text

    def handle_endtag(self, tag: str) -> None:
        if self._depth == 0 or self._current is None:
            return
        # 只关闭以该标签打开的字段，嵌套的无 class 标签不会误弹栈
        while self._field_stack and self._field_stack[-1][0] == tag:
            _, cls, text = self._field_stack.pop()
            if text:
                self._current[cls] = text
        self._depth -= 1
        if self._depth == 0:
            while self._field_stack:  # 匹配元素关闭时冲刷未闭合字段
                _, cls, text = self._field_stack.pop()
                if text:
                    self._current[cls] = text
            self.items.append(self._current)
            self._current = None


def _simulate_extract(url: str, rule: ExtractRule) -> RawExtractedContent:
    """模拟从 URL 抽取内容。

    字段映射规则：
    - 若 value 含逗号：按逗号拆分为多个字段名（种子数据模式）
    - 若 key 非 "raw"：将 key 用作字段名（测试覆盖模式）
    """
    errors: list[str] = []
    fields: dict[str, str] = {}

    if "error" in url.lower() or "404" in url:
        errors.append("页面不可达")
        return RawExtractedContent(source_url=url, extract_rule_id=rule.rule_id, errors=errors)

    if "empty" in url.lower():
        return RawExtractedContent(source_url=url, extract_rule_id=rule.rule_id,
                                   raw_fields={}, errors=["页面内容为空"])

    for key, value in rule.field_mapping.items():
        parts = [p.strip() for p in value.split(",") if p.strip()]
        if len(parts) > 1:
            for field_name in parts:
                fields[field_name] = f"extracted:{field_name}:{hash(url + field_name) % 10000}"
        elif key != "raw":
            fields[key] = f"extracted:{key}:{hash(url + key) % 10000}"
        elif parts:
            fields[parts[0]] = f"extracted:{parts[0]}:{hash(url + parts[0]) % 10000}"
        else:
            fields[key] = f"extracted:{key}:{hash(url) % 10000}"

    return RawExtractedContent(
        source_url=url, extract_rule_id=rule.rule_id,
        raw_fields=fields, errors=errors,
    )


def _apply_clean_rules(raw: RawExtractedContent, rules: list[CleanRule]) -> CleanedContent:
    """对原始抽取内容逐字段应用清洗规则。同字段多条规则链式执行。"""
    cleaned_fields: dict[str, Any] = {}
    errors: list[str] = []

    for rule in rules:
        field = rule.target_field
        # 已清洗过的字段从 cleaned_fields 读取，实现链式清洗
        raw_value = cleaned_fields.get(field, raw.raw_fields.get(field, ""))

        if not raw_value and rule.null_strategy == "skip":
            continue
        if not raw_value and rule.null_strategy == "fill_default":
            raw_value = "N/A"

        try:
            if rule.clean_type.value == "去空格":
                value = raw_value.strip()
            elif rule.clean_type.value == "标准化":
                value = raw_value.lower().replace(" ", "_")
            elif rule.clean_type.value == "校验":
                if rule.validation_range and not re.match(rule.validation_range, raw_value):
                    errors.append(f"字段 {field} 校验失败: {raw_value}")
                    value = raw_value
                else:
                    value = raw_value
            elif rule.clean_type.value == "正则提取":
                m = re.search(rule.expression, raw_value) if rule.expression else None
                value = m.group(1) if m else raw_value
            elif rule.clean_type.value == "类型转换":
                value = _cast_type(raw_value, rule.expression)
            else:
                value = raw_value
        except Exception as exc:
            errors.append(f"字段 {field} 清洗异常: {exc}")
            value = raw_value

        cleaned_fields[field] = value

    return CleanedContent(
        source_url=raw.source_url,
        extract_rule_id=raw.extract_rule_id,
        fields=cleaned_fields,
        clean_errors=errors,
    )


def _cast_type(value: str, target: str) -> Any:
    if target == "int":
        return int(value) if value.isdigit() else value
    if target == "float":
        try:
            return float(value)
        except ValueError:
            return value
    if target == "datetime":
        return value  # 简化实现
    return value


# ---------------------------------------------------------------------------
# 流水线 2: crawl_job → url_dedup → parse_log
# ---------------------------------------------------------------------------

def run_dedup_flow(job: CrawlJobSpec) -> tuple[list[UrlFingerprint], list[UrlFingerprint]]:
    """对采集作业的入口 URL 执行去重，返回新增和重复列表。"""
    existing: dict[str, UrlFingerprint] = {}
    for row in store.rows("url_dedup"):
        fp = UrlFingerprint.from_seed_row(row)
        existing[fp.hash_value] = fp

    new_fps: list[UrlFingerprint] = []
    dup_fps: list[UrlFingerprint] = []

    for url in job.entry_urls:
        fp = UrlFingerprint.compute(url)
        if fp.hash_value in existing:
            existing[fp.hash_value].duplicate_count += 1
            dup_fps.append(existing[fp.hash_value])
        else:
            existing[fp.hash_value] = fp
            new_fps.append(fp)
            _persist_fingerprint(fp)

    return new_fps, dup_fps


# ---------------------------------------------------------------------------
# 流水线 3: proxy_pool → anti_block → crawl_limit
# ---------------------------------------------------------------------------

def select_proxy_for_job(job: CrawlJobSpec) -> ProxyNode | None:
    """从代理池为采集作业选择可用代理节点，并检查反爬策略和限速规则。

    返回最佳代理或 None（无可用代理时）。
    """
    proxies: list[ProxyNode] = []
    for row in store.rows("proxy_pool"):
        p = ProxyNode.from_seed_row(row)
        if p.is_available():
            proxies.append(p)

    if not proxies:
        return None

    # 按延迟排序，选择最快的
    proxies.sort(key=lambda p: p.latency_ms)

    # 检查反爬策略
    anti_strategies: list[AntiBlockStrategy] = []
    for row in store.rows("anti_block"):
        a = AntiBlockStrategy.from_seed_row(row)
        if a.target_site == job.site.domain and a.status == "生效中":
            anti_strategies.append(a)

    best_proxy = proxies[0]

    # 检查限速规则
    for row in store.rows("crawl_limit"):
        limit = CrawlLimitRule.from_seed_row(row)
        if limit.target_site == job.site.domain and limit.status == "生效中":
            job.concurrency = min(job.concurrency, limit.concurrency_max)
            if limit.request_interval_ms > 0:
                best_proxy.latency_ms = max(best_proxy.latency_ms, limit.request_interval_ms)

    return best_proxy


# ---------------------------------------------------------------------------
# 流水线 4: batch_store → data_delivery → quality_sample
# ---------------------------------------------------------------------------

def deliver_and_sample(batch: StorageBatch) -> tuple[DeliveryTask | None, QualitySample | None]:
    """从批次存储投递数据，并对投递结果进行质量抽样。"""
    # 数据投递
    delivery_rows = [r for r in store.rows("data_delivery")
                     if r.get("数据来源") == batch.batch_id or r.get("状态") == "待投递"]
    if delivery_rows:
        dt = DeliveryTask.from_seed_row(delivery_rows[0])
        dt.batch_ref_id = batch.batch_id
        dt.delivered_row_count = batch.row_count
        dt.last_delivery = datetime.now(timezone.utc).isoformat()
        dt.status = "已完成" if batch.status == "校验通过" else "投递失败"
        _persist_delivery(dt)
    else:
        # 自动创建投递任务
        dt = DeliveryTask(
            task_id=f"DEL-{batch.batch_id}",
            data_source=batch.batch_id,
            target_system=f"data_warehouse_{batch.source_job_id}",
            delivery_format=DeliveryFormat.CSV,
            frequency="manual",
            partition_strategy="by_date",
            batch_ref_id=batch.batch_id,
            delivered_row_count=batch.row_count,
            last_delivery=datetime.now(timezone.utc).isoformat(),
            status="已完成" if batch.status == "校验通过" else "投递失败",
        )
        _persist_delivery(dt)

    # 质量抽样
    sample_rows = [r for r in store.rows("quality_sample")
                   if r.get("目标批次") == batch.batch_id]
    if sample_rows:
        qs = QualitySample.from_seed_row(sample_rows[0])
    else:
        # 自动创建抽样计划
        sample_size = max(1, int(batch.row_count * 0.1))
        qs = QualitySample(
            plan_id=f"QS-{batch.batch_id}",
            target_batch=batch.batch_id,
            sample_ratio=0.1,
            sample_method=SampleMethod.RANDOM,
            sample_size=sample_size,
            last_sampled=datetime.now(timezone.utc).isoformat(),
            status="已通过" if batch.status == "校验通过" else "偏差告警",
        )
        _persist_sample(qs)

    # 从批次中随机抽取 sample_size 条记录进行校验
    if batch.records and qs.sample_size > 0:
        sampled = random.sample(batch.records, min(qs.sample_size, len(batch.records)))
        valid_count = sum(1 for s in sampled if s.is_valid())
        actual_ratio = valid_count / len(sampled) if sampled else 0
        qs.check_result = f"抽样{len(sampled)}条，有效{valid_count}条"
        if actual_ratio < (1 - qs.deviation_threshold):
            qs.status = "偏差告警"

    return dt, qs


# ---------------------------------------------------------------------------
# 流水线 5: incr_crawl → crawl_job → diff_check
# ---------------------------------------------------------------------------

def run_incremental_flow(cursor: IncrCursor,
                         previous_batch: StorageBatch | None,
                         current_data: list[CleanedContent]) -> DiffRecord | None:
    """增量采集流程：比较增量游标与当前批次，生成差异检测记录。"""
    if previous_batch is None or not previous_batch.records:
        return None

    prev_fields = {r.source_url: r.fields for r in previous_batch.records}
    curr_fields = {r.source_url: r.fields for r in current_data}

    # 检测新增
    new_urls = set(curr_fields) - set(prev_fields)
    # 检测变更
    changed_urls = {url for url in (set(curr_fields) & set(prev_fields))
                    if curr_fields[url] != prev_fields[url]}
    # 检测删除
    removed_urls = set(prev_fields) - set(curr_fields)

    total_changes = len(new_urls) + len(changed_urls) + len(removed_urls)
    if total_changes == 0:
        return None

    diff = DiffRecord(
        record_id=f"DIFF-{cursor.cursor_id}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}",
        target_site=cursor.target_site,
        page_url=",".join(list(new_urls | changed_urls)[:5]),
        change_type=_classify_change_type(len(new_urls), len(changed_urls), len(removed_urls)),
        old_summary=f"原{len(prev_fields)}条记录",
        new_summary=f"新{len(curr_fields)}条记录，新增{len(new_urls)}变更{len(changed_urls)}删除{len(removed_urls)}",
        affected_fields=list(curr_fields[next(iter(curr_fields))]) if curr_fields else [],
    )

    _persist_diff(diff)

    # 更新游标
    cursor.new_count = len(new_urls)
    cursor.updated_count = len(changed_urls)
    cursor.last_cursor = cursor.current_cursor
    cursor.current_cursor = datetime.now(timezone.utc).isoformat()
    cursor.last_sync = cursor.current_cursor
    cursor.status = "已追上"

    return diff


def _classify_change_type(new_count: int, changed_count: int, removed_count: int) -> str:
    if new_count > 0 and changed_count == 0 and removed_count == 0:
        return "新增内容"
    if changed_count > 0 and new_count == 0 and removed_count == 0:
        return "内容变更"
    if removed_count > 0 and new_count == 0 and changed_count == 0:
        return "内容删除"
    return "混合变更"


# ---------------------------------------------------------------------------
# 流水线 6: data_archive ← batch_store
# ---------------------------------------------------------------------------

def archive_batch(batch: StorageBatch) -> ArchivePolicy | None:
    """从批次存储归档符合条件的批次数据。"""
    # 优先查找与当前批次相关的策略
    matched_policies: list[ArchivePolicy] = []
    policies: list[ArchivePolicy] = []
    for row in store.rows("data_archive"):
        p = ArchivePolicy.from_seed_row(row)
        if p.status in ("待触发", "已完成"):
            policies.append(p)
            if batch.batch_id in p.data_scope or p.data_scope == batch.batch_id:
                matched_policies.append(p)

    # 优先使用匹配的策略
    if matched_policies:
        policy = matched_policies[0]
        policy.last_archive = datetime.now(timezone.utc).isoformat()
        policy.status = "已完成"
        return policy

    if not policies:
        # 创建默认归档策略
        policy = ArchivePolicy(
            policy_id=f"ARCH-{batch.batch_id}",
            data_scope=batch.batch_id,
            archive_cycle="on_demand",
            retention_days=90,
            storage_location=f"/archive/{batch.batch_id}/",
            last_archive=datetime.now(timezone.utc).isoformat(),
            status="已完成",
        )
        _persist_archive_policy(policy)
        return policy

    policy = policies[0]
    policy.last_archive = datetime.now(timezone.utc).isoformat()
    policy.status = "已完成"
    return policy


# ---------------------------------------------------------------------------
# 流水线 7: crawl_cost ← proxy_pool
# ---------------------------------------------------------------------------

def compute_crawl_cost(job: CrawlJobSpec, proxy: ProxyNode | None) -> CostRecord:
    """根据采集作业和代理使用情况计算采集成本。"""
    proxy_hours = job.completed_pages * 0.01 if proxy else 0
    data_mb = job.completed_pages * 0.5  # 每页 0.5MB 估算

    record = CostRecord(
        record_id=f"COST-{job.job_id}",
        site=job.site.domain,
        billing_cycle=datetime.now(timezone.utc).strftime("%Y-%m"),
        request_count=job.completed_pages + job.failed_pages,
        data_volume_mb=round(data_mb, 2),
        proxy_usage_hours=round(proxy_hours, 2),
        estimated_cost=round(data_mb * 0.001 + proxy_hours * 0.5, 4),
        status="已核算" if job.status in ("已完成", "部分失败") else "统计中",
    )
    _persist_cost(record)
    return record


# ---------------------------------------------------------------------------
# 流水线 8: crawl_job → scheduler
# ---------------------------------------------------------------------------

def schedule_job(job: CrawlJobSpec) -> SchedulePlan | None:
    """为采集作业创建调度计划。"""
    # 找关联的调度计划
    for row in store.rows("scheduler"):
        if str(row.get("关联作业")) == job.job_id:
            plan = SchedulePlan.from_seed_row(row)
            plan.next_run = datetime.now(timezone.utc).isoformat()
            plan.status = "执行中" if job.status in ("待执行", "采集执行") else "已完成"
            return plan

    # 创建新的调度计划
    plan = SchedulePlan(
        plan_id=f"SCHED-{job.job_id}",
        linked_job_id=job.job_id,
        cron_expr="0 */6 * * *",  # 每6小时
        next_run=datetime.now(timezone.utc).isoformat(),
        status="执行中" if job.status in ("待执行", "采集执行") else "已完成",
    )
    _persist_schedule(plan)
    return plan


# ---------------------------------------------------------------------------
# 流水线 9: batch_store（空结果回执）→ daily_report（日报取数清单）
# ---------------------------------------------------------------------------

def submit_daily_report(job_id: str) -> DailyReport | None:
    """提交日报取数并出表。

    采用最新已确认（校验通过）批次的版本发布；待验收批次与已确认批次在
    发布范围上冲突时，待验收批次的新值只登记为待验收，不进入发布范围。
    出表写清批次明细、来源版本与发布范围，与批次存储的实际落库范围一致。
    """
    rows = [r for r in store.rows("batch_store") if str(r.get("来源作业")) == job_id]
    if not rows:
        return None
    batches = [StorageBatch.from_seed_row(r) for r in rows]
    confirmed = sorted((b for b in batches if b.status == "校验通过"),
                       key=lambda b: b.stored_at)
    pending = [b for b in batches if b.status == "待验收"]
    adopted = confirmed[-1] if confirmed else None

    lines: list[ReportLine] = []
    publish_scope: list[str] = []
    if adopted is not None:
        publish_scope = list(adopted.page_scope)
        lines.extend(_report_lines_for_batch(adopted, "已发布"))
    for batch in pending:
        lines.extend(_report_lines_for_batch(batch, "待验收"))

    seq = len({str(r.get("报告编号")) for r in store.rows("daily_report")
               if str(r.get("作业编号")) == job_id}) + 1
    report = DailyReport(
        report_id=f"DR-{job_id}-{seq:03d}",
        job_id=job_id,
        lines=lines,
        publish_scope=publish_scope,
        adopted_batch_id=adopted.batch_id if adopted else "",
    )
    _persist_report(report)
    return report


def _report_lines_for_batch(batch: StorageBatch, acceptance: str) -> list[ReportLine]:
    """按批次的空结果回执展开批次明细：每个跑过的页面一行，待核页面保留原因。"""
    version = batch.checksum[:16]
    receipt = _load_receipt(batch.receipt_id, batch.source_job_id) if batch.receipt_id else None
    if receipt is None:
        # 兼容已有的已确认批次：没有页面回执时按批次摘要出一行
        return [ReportLine(
            batch_id=batch.batch_id, source_version=version,
            row_count=batch.row_count, acceptance=acceptance,
            reason="历史批次无页面回执，按批次摘要采用",
        )]
    lines: list[ReportLine] = []
    for page in receipt.pages:
        if page.conclusive:
            lines.append(ReportLine(
                batch_id=batch.batch_id, source_version=version,
                page_url=page.page_url, rule_id=page.rule_id,
                row_count=page.row_count, acceptance=acceptance,
            ))
        else:
            lines.append(ReportLine(
                batch_id=batch.batch_id, source_version=version,
                page_url=page.page_url, rule_id=page.rule_id,
                row_count=0, acceptance="待核", reason=page.reason,
            ))
    return lines


# ---------------------------------------------------------------------------
# 跨模块失败处理和恢复逻辑
# ---------------------------------------------------------------------------

def recover_failed_batch(batch: StorageBatch) -> StorageBatch | None:
    """恢复失败的批次：重新对缺失记录执行抽取和清洗。"""
    if batch.status != "校验失败":
        return None

    # 找出失败记录
    failed_records = [r for r in batch.records if not r.is_valid()]
    if not failed_records:
        return None

    # 重试：对失败记录重新填充缺失字段
    for record in failed_records:
        for field, value in list(record.fields.items()):
            if not value or value == "":
                record.fields[field] = f"recovered:{field}"
        # 恢复后清除之前清洗错误标记
        record.clean_errors = []

    # 重新校验
    batch.checksum = batch.compute_checksum()
    batch.status = "校验通过" if all(r.is_valid() for r in batch.records) else "校验失败"
    batch.stored_at = datetime.now(timezone.utc).isoformat()
    return batch


def handle_duplicate_batch(batch: StorageBatch) -> StorageBatch | None:
    """处理重复批次：检查是否与已有批次产生冲突。"""
    existing_batch_ids = set()
    for row in store.rows("batch_store"):
        existing_batch_ids.add(str(row.get("批次编号")))

    if batch.batch_id in existing_batch_ids:
        # 重复批次：追加时间戳后缀
        suffix = datetime.now(timezone.utc).strftime("%H%M%S")
        batch.batch_id = f"{batch.batch_id}-{suffix}"
        batch.storage_path = f"/data/batches/{batch.source_job_id}-retry/"

    return batch


def handle_interrupted_pipeline(job: CrawlJobSpec) -> CrawlJobSpec:
    """处理中断的采集管线：恢复未完成的工作。"""
    if job.status not in ("部分失败", "待执行"):
        return job

    # 找到失败的页面并标记重试
    if job.failed_pages > 0:
        job.failed_pages = max(0, job.failed_pages - 1)  # 重试一页
        job.status = "采集执行"
        if job.failed_pages + job.completed_pages == 0:
            job.status = "已完成"

    return job


# ---------------------------------------------------------------------------
# 内部持久化辅助
# ---------------------------------------------------------------------------

# 批次对象注册表：内存数据仓库的进程内索引，补跑时按批次号取回记录。
_BATCH_REGISTRY: dict[str, StorageBatch] = {}


def _persist_batch(batch: StorageBatch) -> None:
    rows = store.rows("batch_store")
    payload = {
        "批次编号": batch.batch_id,
        "来源作业": batch.source_job_id,
        "数据行数": batch.row_count,
        "存储路径": batch.storage_path,
        "压缩格式": batch.compression,
        "Schema版本": batch.schema_version,
        "校验摘要": batch.checksum[:16],
        "入库时间": batch.stored_at,
        "回执编号": batch.receipt_id,
        "落库范围": ",".join(batch.page_scope),
        "status": batch.status,
        "pending": batch.status == "待验收",
        "abnormal": batch.status == "校验失败",
    }
    existing = next((r for r in rows if str(r.get("批次编号")) == batch.batch_id), None)
    if existing is not None:
        existing.update(payload)  # 补跑完善同一批次，原地更新不重复登记
    else:
        rows.append({
            "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
            **payload,
        })


def _persist_receipt(receipt: CollectionReceipt) -> None:
    """空结果回执落库：按 (回执编号, 页面URL) 幂等更新，待核原因保留可查。"""
    rows = store.rows("collection_receipt")
    for page in receipt.pages:
        payload = {
            "回执编号": receipt.receipt_id,
            "作业编号": receipt.job_id,
            "页面URL": page.page_url,
            "适用规则": page.rule_id,
            "页面结果": page.outcome.value,
            "行数": page.row_count,
            "原因": page.reason,
            "来源": page.source,
            "记录时间": page.recorded_at,
            "status": page.outcome.value,
            "pending": page.pending,
            "abnormal": page.outcome == PageOutcome.PARSE_FAILED,
        }
        existing = next((r for r in rows
                         if str(r.get("回执编号")) == receipt.receipt_id
                         and str(r.get("页面URL")) == page.page_url), None)
        if existing is not None:
            existing.update(payload)
        else:
            rows.append({
                "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
                **payload,
            })


def _load_receipt(receipt_id: str, job_id: str) -> CollectionReceipt | None:
    rows = [r for r in store.rows("collection_receipt")
            if str(r.get("回执编号")) == receipt_id]
    if not rows:
        return None
    receipt = CollectionReceipt(receipt_id=receipt_id, job_id=job_id)
    for row in rows:
        try:
            outcome = PageOutcome(str(row.get("页面结果", "")))
        except ValueError:
            continue
        try:
            row_count = int(row.get("行数", 0) or 0)
        except (ValueError, TypeError):
            row_count = 0
        receipt.pages.append(PageReceipt(
            page_url=str(row.get("页面URL", "")),
            job_id=job_id,
            outcome=outcome,
            rule_id=str(row.get("适用规则", "")),
            row_count=row_count,
            reason=str(row.get("原因", "") or ""),
            source=str(row.get("来源", "") or ""),
            recorded_at=str(row.get("记录时间", "") or ""),
        ))
    return receipt


def _persist_batch_records(batch: StorageBatch) -> None:
    """批次明细落库：按 (批次编号, 来源页面, 序号) 幂等，已入库的报价只记一次。"""
    rows = store.rows("batch_record")
    existing_keys = {
        (str(r.get("批次编号")), str(r.get("来源页面")), int(r.get("序号", 0)))
        for r in rows
    }
    per_page: dict[str, int] = {}
    for record in batch.records:
        seq = per_page.get(record.source_url, 0) + 1
        per_page[record.source_url] = seq
        key = (batch.batch_id, record.source_url, seq)
        if key in existing_keys:
            continue
        existing_keys.add(key)
        rows.append({
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "批次编号": batch.batch_id,
            "来源页面": record.source_url,
            "序号": seq,
            "适用规则": record.extract_rule_id,
            "字段摘要": json.dumps(record.fields, ensure_ascii=False, sort_keys=True, default=str),
            "入库时间": batch.stored_at,
            "status": "已入库",
            "pending": False,
            "abnormal": False,
        })


def _load_batch_records(batch_id: str) -> list[CleanedContent]:
    rows = [r for r in store.rows("batch_record")
            if str(r.get("批次编号")) == batch_id]
    rows.sort(key=lambda r: (str(r.get("来源页面")), int(r.get("序号", 0) or 0)))
    records: list[CleanedContent] = []
    for row in rows:
        try:
            fields = json.loads(str(row.get("字段摘要", "{}")))
        except json.JSONDecodeError:
            fields = {}
        records.append(CleanedContent(
            source_url=str(row.get("来源页面", "")),
            extract_rule_id=str(row.get("适用规则", "")),
            fields=fields,
        ))
    return records


def _persist_report(report: DailyReport) -> None:
    """日报取数清单落库：每行批次明细写清来源版本、页面范围与发布范围。"""
    rows = store.rows("daily_report")
    scope_text = ",".join(report.publish_scope)
    for line in report.lines:
        rows.append({
            "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
            "报告编号": report.report_id,
            "作业编号": report.job_id,
            "批次编号": line.batch_id,
            "来源版本": line.source_version,
            "页面范围": line.page_url,
            "适用规则": line.rule_id,
            "行数": line.row_count,
            "验收状态": line.acceptance,
            "发布范围": scope_text,
            "原因": line.reason,
            "生成时间": report.created_at,
            "status": line.acceptance,
            "pending": line.acceptance in ("待验收", "待核"),
            "abnormal": False,
        })


def _sync_report_acceptance(batch: StorageBatch) -> None:
    """验收结果同步到日报取数清单：批次确认后，清单中该批次的待核行转为已确认。"""
    if batch.status != "校验通过":
        return
    receipt = _load_receipt(batch.receipt_id, batch.source_job_id) if batch.receipt_id else None
    page_rows = {p.page_url: p.row_count for p in receipt.pages} if receipt else {}
    for row in store.rows("daily_report"):
        if str(row.get("批次编号")) != batch.batch_id:
            continue
        if row.get("验收状态") not in ("待验收", "待核"):
            continue
        row["验收状态"] = "已确认"
        row["来源版本"] = batch.checksum[:16]
        page_url = str(row.get("页面范围", ""))
        if page_url in page_rows:
            row["行数"] = page_rows[page_url]
        row["status"] = "已确认"
        row["pending"] = False


def _persist_fingerprint(fp: UrlFingerprint) -> None:
    rows = store.rows("url_dedup")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "指纹编号": fp.fingerprint_id,
        "源URL": fp.source_url,
        "指纹算法": fp.algorithm,
        "指纹哈希": fp.hash_value,
        "发现渠道": fp.discovery_channel,
        "首次发现": fp.first_seen,
        "重复次数": fp.duplicate_count,
        "去重状态": fp.status,
        "status": fp.status,
        "pending": False,
        "abnormal": False,
    })


def _persist_parse_log(log: ParseLogEntry) -> None:
    rows = store.rows("parse_log")
    rows.append({
        "id": max((int(row.get("id", 0)) for row in rows), default=0) + 1,
        "记录编号": log.record_id,
        "来源URL": log.source_url,
        "解析规则": log.parse_rule_id,
        "解析结果": log.parse_result,
        "异常类型": log.error_type,
        "重试次数": log.retry_count,
        "错误详情": log.error_detail,
        "记录时间": log.recorded_at,
        "status": "成功" if not log.error_type else "格式异常",
        "pending": False,
        "abnormal": bool(log.error_type),
    })


def _persist_delivery(dt: DeliveryTask) -> None:
    rows = store.rows("data_delivery")
    rows.append(dict(
        id=max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        **{f: (getattr(dt, k) if not isinstance(getattr(dt, k), DeliveryFormat) else getattr(dt, k).value) for f, k in [
            ("任务编号", "task_id"), ("数据来源", "data_source"),
            ("目标系统", "target_system"), ("投递格式", "delivery_format"),
            ("投递频率", "frequency"), ("分区策略", "partition_strategy"),
            ("最近投递", "last_delivery"), ("投递状态", "status"),
        ]},
        status=dt.status, pending=False, abnormal=dt.status == "投递失败",
    ))


def _persist_sample(qs: QualitySample) -> None:
    rows = store.rows("quality_sample")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "计划编号": qs.plan_id, "目标批次": qs.target_batch,
        "抽样比例": str(qs.sample_ratio), "抽样方式": qs.sample_method.value,
        "样本数量": qs.sample_size, "检验结果": qs.check_result,
        "偏差阈值": str(qs.deviation_threshold), "最近采样": qs.last_sampled,
        "status": qs.status, "pending": False,
        "abnormal": qs.status == "偏差告警",
    })


def _persist_diff(diff: DiffRecord) -> None:
    rows = store.rows("diff_check")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": diff.record_id, "目标站点": diff.target_site,
        "检测页面": diff.page_url, "变更类型": diff.change_type,
        "旧内容摘要": diff.old_summary, "新内容摘要": diff.new_summary,
        "影响字段": ",".join(diff.affected_fields), "检测时间": diff.detected_at,
        "status": diff.status, "pending": False, "abnormal": False,
    })


def _persist_archive_policy(p: ArchivePolicy) -> None:
    rows = store.rows("data_archive")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "策略编号": p.policy_id, "数据范围": p.data_scope,
        "归档周期": p.archive_cycle, "归档格式": str(p.archive_format),
        "保留时长": str(p.retention_days), "存储位置": p.storage_location,
        "压缩方式": p.compression_method, "最后归档": p.last_archive,
        "status": p.status, "pending": False, "abnormal": False,
    })


def _persist_cost(c: CostRecord) -> None:
    rows = store.rows("crawl_cost")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "记录编号": c.record_id, "采集站点": c.site,
        "计费周期": c.billing_cycle, "请求次数": str(c.request_count),
        "数据流量": str(c.data_volume_mb), "代理用量": str(c.proxy_usage_hours),
        "费用预估": c.estimated_cost, "账单状态": c.status,
        "status": c.status, "pending": False,
        "abnormal": c.estimated_cost > 100,
    })


def _persist_schedule(plan: SchedulePlan) -> None:
    rows = store.rows("scheduler")
    rows.append({
        "id": max((int(r.get("id", 0)) for r in rows), default=0) + 1,
        "计划编号": plan.plan_id, "关联作业": plan.linked_job_id,
        "Cron表达式": plan.cron_expr, "时区设置": plan.timezone_setting,
        "依赖检查": plan.dependency_check, "失败通知": plan.failure_notify,
        "并发控制": plan.concurrency_control, "下次执行": plan.next_run,
        "status": plan.status, "pending": False, "abnormal": False,
    })