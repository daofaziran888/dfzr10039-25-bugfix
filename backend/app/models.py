"""领域数据模型：跨模块传递的真实业务对象，不只dict传递。

每个数据类描述在一个数据流中由一个模块传给下一个模块的数据结构，
包含校验、序列化和失败处理能力。
"""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any
from urllib.parse import urlparse


# ---------------------------------------------------------------------------
# 枚举
# ---------------------------------------------------------------------------

class SiteType(str, Enum):
    NEWS = "新闻门户"
    CONTENT_SITE = "内容平台"
    BBS = "社区论坛"
    SEARCH = "搜索引擎"
    SOCIAL = "社交媒体"
    GOVERNMENT = "政府公示"
    FINANCE = "金融资讯"


class ProxyType(str, Enum):
    HTTP = "HTTP"
    HTTPS = "HTTPS"
    SOCKS5 = "SOCKS5"


class AntiType(str, Enum):
    IP_LIMIT = "IP限频"
    UA_CHECK = "UA检测"
    COOKIE_CHECK = "Cookie校验"
    CAPTCHA = "验证码"
    JS_CHALLENGE = "JS挑战"
    FINGERPRINT = "浏览器指纹"


class CleanType(str, Enum):
    TRIM = "去空格"
    NORMALIZE = "标准化"
    VALIDATE = "校验"
    DEFAULT_FILL = "默认填充"
    REGEX_EXTRACT = "正则提取"
    TYPE_CAST = "类型转换"


class DeliveryFormat(str, Enum):
    CSV = "CSV"
    JSON = "JSON"
    PARQUET = "Parquet"
    AVRO = "Avro"


class SampleMethod(str, Enum):
    RANDOM = "随机抽样"
    STRATIFIED = "分层抽样"
    SYSTEMATIC = "等距抽样"
    TOP_N = "头N条"


class ArchiveFormat(str, Enum):
    GZIP = "gzip"
    LZ4 = "lz4"
    SNAPPY = "snappy"
    ZSTD = "zstd"


# ---------------------------------------------------------------------------
# 跨模块数据类
# ---------------------------------------------------------------------------

@dataclass
class SiteConfig:
    """采集站点配置：由 crawl_site 生成，crawl_job 消费。"""
    site_id: str
    domain: str
    site_type: SiteType
    robots_policy: str
    language: str
    region: str
    access_method: str
    registered_at: str
    verified: bool = False
    last_validated: str | None = None

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> SiteConfig:
        site_type_str = str(row.get("站点类型", SiteType.NEWS.value))
        try:
            st = SiteType(site_type_str)
        except ValueError:
            st = SiteType.NEWS
        return cls(
            site_id=str(row.get("站点编号", "")),
            domain=str(row.get("站点域名", "")),
            site_type=st,
            robots_policy=str(row.get("Robots协议", "allow")),
            language=str(row.get("站点语言", "zh-CN")),
            region=str(row.get("地域归属", "CN")),
            access_method=str(row.get("接入方式", "direct")),
            registered_at=str(row.get("注册时间", "")),
            verified=row.get("status") == "已验证",
            last_validated=str(row.get("注册时间", "")),
        )

    def is_crawlable(self) -> bool:
        return self.verified and self.robots_policy.lower() != "disallow"


@dataclass
class CrawlJobSpec:
    """采集作业规格：由 crawl_job 生成，scheduler/interactor 消费。"""
    job_id: str
    site: SiteConfig
    entry_urls: list[str] = field(default_factory=list)
    max_depth: int = 3
    concurrency: int = 4
    schedule_policy: str = "interval"
    completed_pages: int = 0
    failed_pages: int = 0
    status: str = "待执行"
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @classmethod
    def from_seed_row(cls, row: dict[str, Any], site: SiteConfig | None = None) -> CrawlJobSpec:
        raw_url = str(row.get("入口URL", ""))
        # 支持逗号分隔的多 URL
        urls = [u.strip() for u in raw_url.split(",") if u.strip()] if raw_url else []
        try:
            md = int(row.get("最大深度", 3))
        except (ValueError, TypeError):
            md = 3
        try:
            cc = int(row.get("并发线程", 4))
        except (ValueError, TypeError):
            cc = 4
        try:
            cp = int(row.get("完成页数", 0))
        except (ValueError, TypeError):
            cp = 0
        try:
            fp = int(row.get("失败页数", 0))
        except (ValueError, TypeError):
            fp = 0
        return cls(
            job_id=str(row.get("作业编号", "")),
            site=site or SiteConfig(site_id="", domain="", site_type=SiteType.NEWS,
                                     robots_policy="allow", language="zh-CN",
                                     region="CN", access_method="direct", registered_at=""),
            entry_urls=urls,
            max_depth=md,
            concurrency=cc,
            schedule_policy=str(row.get("调度策略", "interval")),
            completed_pages=cp,
            failed_pages=fp,
            status=str(row.get("status", "待执行")),
        )


@dataclass
class UrlFingerprint:
    """URL 去重指纹：由 url_dedup 生成，crawl_job 在调度前检查。"""
    fingerprint_id: str
    source_url: str
    algorithm: str
    hash_value: str
    discovery_channel: str
    first_seen: str
    duplicate_count: int = 0
    status: str = "已记录"

    @classmethod
    def compute(cls, url: str, algorithm: str = "sha256") -> UrlFingerprint:
        parsed = urlparse(url)
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        params = "&".join(sorted(parsed.query.split("&"))) if parsed.query else ""
        if params:
            normalized += f"?{params}"
        h = hashlib.new(algorithm, normalized.encode()).hexdigest()
        return cls(
            fingerprint_id=f"FP-{h[:12]}",
            source_url=url,
            algorithm=algorithm,
            hash_value=h,
            discovery_channel="crawl_job",
            first_seen=datetime.now(timezone.utc).isoformat(),
        )

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> UrlFingerprint:
        try:
            dup = int(row.get("重复次数", 0))
        except (ValueError, TypeError):
            dup = 0
        return cls(
            fingerprint_id=str(row.get("指纹编号", "")),
            source_url=str(row.get("源URL", "")),
            algorithm=str(row.get("指纹算法", "sha256")),
            hash_value=str(row.get("指纹哈希", "")),
            discovery_channel=str(row.get("发现渠道", "")),
            first_seen=str(row.get("首次发现", "")),
            duplicate_count=dup,
            status=str(row.get("status", "已记录")),
        )


@dataclass
class ExtractRule:
    """内容抽取规则：由 content_extract 管理，crawl_job 在下载后调用。"""
    rule_id: str
    target_site: str
    xpath_selector: str
    field_mapping: dict[str, str] = field(default_factory=dict)
    data_type: str = "string"
    clean_regex: str = ""
    extract_order: int = 0
    status: str = "草稿"
    url_pattern: str = ""  # 适用URL模式（fnmatch），空串表示适用于站点全部页面

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> ExtractRule:
        return cls(
            rule_id=str(row.get("规则编号", "")),
            target_site=str(row.get("目标站点", "")),
            xpath_selector=str(row.get("XPath选择器", "")),
            field_mapping={"raw": str(row.get("字段映射", ""))},
            data_type=str(row.get("数据类型", "string")),
            clean_regex=str(row.get("清洗正则", "")),
            extract_order=int(row.get("抽取顺序", 0)),
            status=str(row.get("status", "草稿")),
            url_pattern=str(row.get("适用URL模式", "")),
        )

    def applies_to(self, site_domain: str, url: str) -> bool:
        """判断规则是否适用指定页面：生效中、站点匹配且 URL 模式命中。"""
        if self.status != "生效中":
            return False
        if self.target_site and site_domain and self.target_site != site_domain:
            return False
        if self.url_pattern:
            from fnmatch import fnmatchcase
            if not fnmatchcase(url, self.url_pattern):
                return False
        return True


@dataclass
class RawExtractedContent:
    """原始抽取内容：content_extract 输出，field_clean 输入。"""
    source_url: str
    extract_rule_id: str
    raw_fields: dict[str, str] = field(default_factory=dict)
    extracted_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    errors: list[str] = field(default_factory=list)

    def has_errors(self) -> bool:
        return len(self.errors) > 0


@dataclass
class CleanRule:
    """字段清洗规则：field_clean 管理，对 RawExtractedContent 逐字段清洗。"""
    rule_id: str
    target_field: str
    clean_type: CleanType
    expression: str = ""
    null_strategy: str = "skip"
    validation_range: str = ""
    clean_order: int = 0
    status: str = "草稿"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> CleanRule:
        return cls(
            rule_id=str(row.get("规则编号", "")),
            target_field=str(row.get("目标字段", "")),
            clean_type=CleanType(str(row.get("清洗类型", CleanType.TRIM.value))),
            expression=str(row.get("处理表达式", "")),
            null_strategy=str(row.get("空值策略", "skip")),
            validation_range=str(row.get("校验范围", "")),
            clean_order=int(row.get("清洗顺序", 0)),
            status=str(row.get("status", "草稿")),
        )


@dataclass
class CleanedContent:
    """清洗后内容：field_clean 输出，batch_store 输入。"""
    source_url: str
    extract_rule_id: str
    fields: dict[str, Any] = field(default_factory=dict)
    clean_errors: list[str] = field(default_factory=list)
    cleaned_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    def is_valid(self) -> bool:
        if len(self.clean_errors) > 0:
            return False
        if len(self.fields) == 0:
            return False
        return all(v for v in self.fields.values())


@dataclass
class StorageBatch:
    """存储批次：batch_store 管理，data_delivery/data_archive 消费。"""
    batch_id: str
    source_job_id: str
    row_count: int = 0
    storage_path: str = ""
    compression: str = "gzip"
    schema_version: str = "v1"
    checksum: str = ""
    stored_at: str = ""
    status: str = "采集中"
    records: list[CleanedContent] = field(default_factory=list)
    receipt_id: str = ""           # 空结果回执编号：批次行数与落库范围以回执为准
    page_scope: list[str] = field(default_factory=list)  # 实际落库的页面范围

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> StorageBatch:
        scope_raw = str(row.get("落库范围", "") or "")
        return cls(
            batch_id=str(row.get("批次编号", "")),
            source_job_id=str(row.get("来源作业", "")),
            row_count=int(row.get("数据行数", 0)),
            storage_path=str(row.get("存储路径", "")),
            compression=str(row.get("压缩格式", "gzip")),
            schema_version=str(row.get("Schema版本", "v1")),
            checksum=str(row.get("校验摘要", "")),
            stored_at=str(row.get("入库时间", "")),
            status=str(row.get("status", "采集中")),
            receipt_id=str(row.get("回执编号", "") or ""),
            page_scope=[u for u in scope_raw.split(",") if u],
        )

    def compute_checksum(self) -> str:
        payload = json.dumps([r.fields for r in self.records], sort_keys=True, default=str)
        return hashlib.sha256(payload.encode()).hexdigest()


# ---------------------------------------------------------------------------
# 空结果回执与日报取数
# ---------------------------------------------------------------------------

class PageOutcome(str, Enum):
    """页面级采集结论：只有用过适用规则并留有来源的页面才能认定为空或有结果。"""
    PARSED = "已解析"        # 适用规则命中并产出数据行
    EMPTY = "空页"           # 适用规则命中但页面确无匹配内容（零行依据）
    NO_RULE = "缺规则"       # 没有适用的解析规则，页面被跳过（保留原因，不算完成）
    READ_FAILED = "读取失败"  # 页面读取中断/失败（保留原因，不算完成）
    PARSE_FAILED = "解析失败"  # 规则适用但解析产生错误


@dataclass
class PageReceipt:
    """空结果回执的页面条目：交代一个入口页面跑没跑过、结果如何、原因是什么。"""
    page_url: str
    job_id: str
    outcome: PageOutcome
    rule_id: str = ""       # 实际适用的解析规则（来源证据）
    row_count: int = 0
    reason: str = ""        # 缺规则/读取失败等原因
    source: str = ""        # 页面来源（本地路径或 URL）
    recorded_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def conclusive(self) -> bool:
        """是否有结论：只有实际用过适用规则的页面（有结果或确为空）才算跑过。"""
        return self.outcome in (PageOutcome.PARSED, PageOutcome.EMPTY)

    @property
    def pending(self) -> bool:
        """是否待核：缺规则或读取失败，等待补规则或恢复后重试。"""
        return self.outcome in (PageOutcome.NO_RULE, PageOutcome.READ_FAILED)


@dataclass
class CollectionReceipt:
    """空结果回执：一次采集运行全部入口页面的交代，批次存储与日报取数共用。"""
    receipt_id: str
    job_id: str
    pages: list[PageReceipt] = field(default_factory=list)
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @property
    def completed_pages(self) -> list[PageReceipt]:
        """跑过的页面：有结论（已解析或空页）。"""
        return [p for p in self.pages if p.conclusive]

    @property
    def pending_pages(self) -> list[PageReceipt]:
        """还没完成的页面：缺规则或读取失败，原因已保留。"""
        return [p for p in self.pages if p.pending]

    @property
    def failed_pages(self) -> list[PageReceipt]:
        return [p for p in self.pages if p.outcome == PageOutcome.PARSE_FAILED]

    @property
    def is_complete(self) -> bool:
        return not self.pending_pages and not self.failed_pages

    @property
    def parsed_row_count(self) -> int:
        return sum(p.row_count for p in self.pages if p.outcome == PageOutcome.PARSED)

    @property
    def scope(self) -> list[str]:
        """落库范围：实际跑过并有来源的页面。"""
        return [p.page_url for p in self.pages if p.conclusive]

    def page(self, page_url: str) -> PageReceipt | None:
        for p in self.pages:
            if p.page_url == page_url:
                return p
        return None


@dataclass
class ReportLine:
    """日报取数清单的一行：采用的批次明细、来源版本、页面范围与验收状态。"""
    batch_id: str
    source_version: str     # 来源版本：批次校验摘要
    page_url: str = ""      # 页面范围（空串表示批次级兼容行）
    rule_id: str = ""
    row_count: int = 0
    acceptance: str = "待验收"  # 已发布 / 待验收 / 待核 / 已确认
    reason: str = ""


@dataclass
class DailyReport:
    """日报出表结果：使用的批次明细、来源版本与发布范围的快照，生成后不可改。"""
    report_id: str
    job_id: str
    lines: list[ReportLine] = field(default_factory=list)
    publish_scope: list[str] = field(default_factory=list)  # 发布范围 = 采用批次的落库范围
    adopted_batch_id: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status: str = "已出表"

    @property
    def published_rows(self) -> int:
        return sum(l.row_count for l in self.lines if l.acceptance == "已发布")

    @property
    def pending_rows(self) -> int:
        return sum(l.row_count for l in self.lines if l.acceptance in ("待验收", "待核"))


@dataclass
class DeliveryTask:
    """数据投递任务：data_delivery 管理，从 batch_store 读取批次投递。"""
    task_id: str
    data_source: str
    target_system: str
    delivery_format: DeliveryFormat
    frequency: str
    partition_strategy: str
    last_delivery: str = ""
    status: str = "待投递"
    batch_ref_id: str = ""
    delivered_row_count: int = 0

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> DeliveryTask:
        df_str = str(row.get("投递格式", DeliveryFormat.CSV.value))
        try:
            df = DeliveryFormat(df_str)
        except ValueError:
            df = DeliveryFormat.CSV
        return cls(
            task_id=str(row.get("任务编号", "")),
            data_source=str(row.get("数据来源", "")),
            target_system=str(row.get("目标系统", "")),
            delivery_format=df,
            frequency=str(row.get("投递频率", "daily")),
            partition_strategy=str(row.get("分区策略", "")),
            last_delivery=str(row.get("最近投递", "")),
            status=str(row.get("status", "待投递")),
        )


@dataclass
class QualitySample:
    """质量抽样计划：quality_sample 管理，从 batch_store 提取样本检测。"""
    plan_id: str
    target_batch: str
    sample_ratio: float
    sample_method: SampleMethod
    sample_size: int = 0
    check_result: str = ""
    deviation_threshold: float = 0.05
    last_sampled: str = ""
    status: str = "待采样"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> QualitySample:
        ratio_str = str(row.get("抽样比例", "0.1"))
        try:
            ratio = float(ratio_str.rstrip("%")) / 100 if "%" in ratio_str else float(ratio_str)
        except (ValueError, TypeError):
            ratio = 0.1
        sm_str = str(row.get("抽样方式", SampleMethod.RANDOM.value))
        try:
            sm = SampleMethod(sm_str)
        except ValueError:
            sm = SampleMethod.RANDOM
        try:
            ss = int(row.get("样本数量", 0))
        except (ValueError, TypeError):
            ss = 0
        try:
            dt = float(row.get("偏差阈值", 0.05))
        except (ValueError, TypeError):
            dt = 0.05
        return cls(
            plan_id=str(row.get("计划编号", "")),
            target_batch=str(row.get("目标批次", "")),
            sample_ratio=ratio,
            sample_method=sm,
            sample_size=ss,
            check_result=str(row.get("检验结果", "")),
            deviation_threshold=dt,
            last_sampled=str(row.get("最近采样", "")),
            status=str(row.get("status", "待采样")),
        )


@dataclass
class ProxyNode:
    """代理节点：proxy_pool 管理，anti_block/crawl_cost 消费。"""
    node_id: str
    address: str
    proxy_type: ProxyType
    region_tag: str
    operator: str
    latency_ms: float = 0.0
    success_rate: float = 1.0
    last_checked: str = ""
    status: str = "待校验"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> ProxyNode:
        pt_str = str(row.get("代理类型", ProxyType.HTTP.value))
        try:
            pt = ProxyType(pt_str)
        except ValueError:
            pt = ProxyType.HTTP
        try:
            lat = float(row.get("响应延迟", 0))
        except (ValueError, TypeError):
            lat = 0.0
        try:
            sr = float(row.get("成功率", 1.0))
        except (ValueError, TypeError):
            sr = 1.0
        return cls(
            node_id=str(row.get("节点编号", "")),
            address=str(row.get("代理地址", "")),
            proxy_type=pt,
            region_tag=str(row.get("地域标签", "")),
            operator=str(row.get("运营商", "")),
            latency_ms=lat,
            success_rate=sr,
            last_checked=str(row.get("最后校验", "")),
            status=str(row.get("status", "待校验")),
        )

    def is_available(self) -> bool:
        return self.status == "可用" and self.success_rate >= 0.8 and self.latency_ms < 3000


@dataclass
class AntiBlockStrategy:
    """反爬对抗策略：anti_block 管理，结合 proxy_pool 提供代理轮换。"""
    strategy_id: str
    target_site: str
    anti_type: AntiType
    ua_pool: list[str] = field(default_factory=list)
    cookie_rotation: bool = False
    request_interval_ms: int = 1000
    captcha_engine: str = ""
    status: str = "待配置"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> AntiBlockStrategy:
        at_str = str(row.get("对抗类型", AntiType.IP_LIMIT.value))
        try:
            at = AntiType(at_str)
        except ValueError:
            at = AntiType.IP_LIMIT
        try:
            interval = int(row.get("请求间隔", 1000))
        except (ValueError, TypeError):
            interval = 1000
        return cls(
            strategy_id=str(row.get("策略编号", "")),
            target_site=str(row.get("目标站点", "")),
            anti_type=at,
            ua_pool=[str(row.get("UserAgent池", ""))] if row.get("UserAgent池") else [],
            cookie_rotation=bool(row.get("Cookie轮换", False)),
            request_interval_ms=interval,
            captcha_engine=str(row.get("验证码识别", "")),
            status=str(row.get("status", "待配置")),
        )


@dataclass
class CrawlLimitRule:
    """采集限速规则：crawl_limit 管理，控制 crawl_job 执行节奏。"""
    rule_id: str
    target_site: str
    request_interval_ms: int
    concurrency_max: int = 1
    daily_limit: int = 10000
    time_window: str = ""
    limit_type: str = "fixed"
    status: str = "草稿"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> CrawlLimitRule:
        try: interval = int(row.get("请求间隔", 1000))
        except (ValueError, TypeError): interval = 1000
        try: conc = int(row.get("并发上限", 1))
        except (ValueError, TypeError): conc = 1
        try: daily = int(row.get("日请求量", 10000))
        except (ValueError, TypeError): daily = 10000
        return cls(
            rule_id=str(row.get("策略编号", "")),
            target_site=str(row.get("目标站点", "")),
            request_interval_ms=interval,
            concurrency_max=conc,
            daily_limit=daily,
            time_window=str(row.get("时段窗口", "")),
            limit_type=str(row.get("限速类型", "fixed")),
            status=str(row.get("status", "草稿")),
        )


@dataclass
class ParseLogEntry:
    """解析日志记录：parse_log 管理，记录每次抽取的解析结果。"""
    record_id: str
    source_url: str
    parse_rule_id: str
    parse_result: str = ""
    error_type: str = ""
    retry_count: int = 0
    error_detail: str = ""
    recorded_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> ParseLogEntry:
        return cls(
            record_id=str(row.get("记录编号", "")),
            source_url=str(row.get("来源URL", "")),
            parse_rule_id=str(row.get("解析规则", "")),
            parse_result=str(row.get("解析结果", "")),
            error_type=str(row.get("异常类型", "")),
            retry_count=int(row.get("重试次数", 0)),
            error_detail=str(row.get("错误详情", "")),
            recorded_at=str(row.get("记录时间", "")),
        )


@dataclass
class IncrCursor:
    """增量采集游标：incr_crawl 管理，记录每个站点的增量位置。"""
    cursor_id: str
    target_site: str
    incr_field: str
    last_cursor: str = ""
    current_cursor: str = ""
    new_count: int = 0
    updated_count: int = 0
    last_sync: str = ""
    status: str = "待初始化"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> IncrCursor:
        return cls(
            cursor_id=str(row.get("标记编号", "")),
            target_site=str(row.get("目标站点", "")),
            incr_field=str(row.get("增量字段", "")),
            last_cursor=str(row.get("上次游标", "")),
            current_cursor=str(row.get("当前游标", "")),
            new_count=int(row.get("新增数量", 0)),
            updated_count=int(row.get("更新数量", 0)),
            last_sync=str(row.get("最后同步", "")),
            status=str(row.get("status", "待初始化")),
        )


@dataclass
class DiffRecord:
    """差异检测记录：diff_check 管理，比较增量采集与历史数据差异。"""
    record_id: str
    target_site: str
    page_url: str
    change_type: str
    old_summary: str = ""
    new_summary: str = ""
    affected_fields: list[str] = field(default_factory=list)
    detected_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    status: str = "已检测"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> DiffRecord:
        return cls(
            record_id=str(row.get("记录编号", "")),
            target_site=str(row.get("目标站点", "")),
            page_url=str(row.get("检测页面", "")),
            change_type=str(row.get("变更类型", "")),
            old_summary=str(row.get("旧内容摘要", "")),
            new_summary=str(row.get("新内容摘要", "")),
            affected_fields=[str(row.get("影响字段", ""))] if row.get("影响字段") else [],
            detected_at=str(row.get("检测时间", "")),
            status=str(row.get("status", "已检测")),
        )


@dataclass
class CostRecord:
    """采集计费记录：crawl_cost 管理，统计代理和请求的成本。"""
    record_id: str
    site: str
    billing_cycle: str
    request_count: int = 0
    data_volume_mb: float = 0.0
    proxy_usage_hours: float = 0.0
    estimated_cost: float = 0.0
    status: str = "统计中"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> CostRecord:
        return cls(
            record_id=str(row.get("记录编号", "")),
            site=str(row.get("采集站点", "")),
            billing_cycle=str(row.get("计费周期", "")),
            request_count=int(row.get("请求次数", 0)),
            data_volume_mb=float(row.get("数据流量", 0)),
            proxy_usage_hours=float(row.get("代理用量", 0)),
            estimated_cost=float(row.get("费用预估", 0)),
            status=str(row.get("status", "统计中")),
        )


@dataclass
class ArchivePolicy:
    """数据归档策略：data_archive 管理，定期归档批次存储数据。"""
    policy_id: str
    data_scope: str
    archive_cycle: str
    archive_format: ArchiveFormat
    retention_days: int = 90
    storage_location: str = ""
    compression_method: str = "gzip"
    last_archive: str = ""
    status: str = "待触发"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> ArchivePolicy:
        af_str = str(row.get("归档格式", ArchiveFormat.GZIP.value))
        try:
            af = ArchiveFormat(af_str)
        except ValueError:
            af = ArchiveFormat.GZIP
        try:
            rd = int(row.get("保留时长", 90))
        except (ValueError, TypeError):
            rd = 90
        return cls(
            policy_id=str(row.get("策略编号", "")),
            data_scope=str(row.get("数据范围", "")),
            archive_cycle=str(row.get("归档周期", "monthly")),
            archive_format=af,
            retention_days=rd,
            storage_location=str(row.get("存储位置", "")),
            compression_method=str(row.get("压缩方式", "gzip")),
            last_archive=str(row.get("最后归档", "")),
            status=str(row.get("status", "待触发")),
        )


@dataclass
class SchedulePlan:
    """调度计划：scheduler 管理，编排 crawl_job 的定时执行。"""
    plan_id: str
    linked_job_id: str
    cron_expr: str
    timezone_setting: str = "Asia/Shanghai"
    dependency_check: str = ""
    failure_notify: str = ""
    concurrency_control: str = ""
    next_run: str = ""
    status: str = "待激活"

    @classmethod
    def from_seed_row(cls, row: dict[str, Any]) -> SchedulePlan:
        return cls(
            plan_id=str(row.get("计划编号", "")),
            linked_job_id=str(row.get("关联作业", "")),
            cron_expr=str(row.get("Cron表达式", "")),
            timezone_setting=str(row.get("时区设置", "Asia/Shanghai")),
            dependency_check=str(row.get("依赖检查", "")),
            failure_notify=str(row.get("失败通知", "")),
            concurrency_control=str(row.get("并发控制", "")),
            next_run=str(row.get("下次执行", "")),
            status=str(row.get("status", "待激活")),
        )