"""空结果回执与日报取数测试：用本地页面复现缺规则误报、读取中断补跑、日报出表。

场景：日报作业的文章页确实没有匹配内容；报价清单起初缺规则，补齐后
第一页抽到 7 条、第二页读取中断；值班员提交日报取数后，恢复第二页
抽到 12 条并重复补跑，再次出表核对批次明细、来源版本与发布范围。
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from app.integration import (
    run_crawl_pipeline, resume_crawl_pipeline, submit_daily_report,
)
from app.models import PageOutcome
from app.store import store

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "daily_report"

# ---------------------------------------------------------------------------
# 测试数据
# ---------------------------------------------------------------------------

SITE = {
    "id": 901, "status": "已验证", "pending": False, "abnormal": False,
    "站点编号": "CRAW-DR-SITE", "站点域名": "daily.example.com",
    "站点类型": "金融资讯", "Robots协议": "allow",
    "站点语言": "zh-CN", "地域归属": "CN",
    "接入方式": "direct", "注册时间": "2026-11-01",
}

ARTICLE_RULE = {
    "id": 901, "status": "生效中", "pending": False, "abnormal": False,
    "规则编号": "CONT-DR-ARTICLE", "目标站点": "daily.example.com",
    "XPath选择器": "//div[@class='daily-quote']", "字段映射": "title,body",
    "数据类型": "string", "清洗正则": "", "抽取顺序": "1",
    "规则状态": "生效中", "适用URL模式": "*/article*",
}

QUOTE_RULE = {
    "id": 902, "status": "生效中", "pending": False, "abnormal": False,
    "规则编号": "CONT-DR-QUOTE", "目标站点": "daily.example.com",
    "XPath选择器": "//li[@class='quote']", "字段映射": "name,price",
    "数据类型": "string", "清洗正则": "", "抽取顺序": "2",
    "规则状态": "生效中", "适用URL模式": "*/quotes*",
}

CLEAN_RULES = [
    {
        "id": 901, "status": "生效中", "pending": False, "abnormal": False,
        "规则编号": "FIEL-DR-NAME", "目标字段": "name",
        "清洗类型": "去空格", "处理表达式": "", "空值策略": "skip",
        "校验范围": "", "清洗顺序": "1", "规则状态": "生效中",
    },
    {
        "id": 902, "status": "生效中", "pending": False, "abnormal": False,
        "规则编号": "FIEL-DR-PRICE", "目标字段": "price",
        "清洗类型": "去空格", "处理表达式": "", "空值策略": "skip",
        "校验范围": "", "清洗顺序": "2", "规则状态": "生效中",
    },
]


def _job_row(job_id: str, urls: list[str]) -> dict:
    return {
        "id": 901, "status": "待执行", "pending": True, "abnormal": False,
        "作业编号": job_id, "目标站点": "daily.example.com",
        "入口URL": ",".join(urls), "最大深度": "1", "并发线程": "1",
        "调度策略": "interval", "完成页数": "0", "失败页数": "0",
    }


@pytest.fixture()
def local_pages(tmp_path: Path) -> dict[str, str]:
    """把本地页面复制到独立临时目录，返回 file:// 入口地址。"""
    pages: dict[str, str] = {}
    for name in ("article.html", "quotes_p1.html", "quotes_p2.html"):
        target = tmp_path / name
        shutil.copy(FIXTURES / name, target)
        pages[name] = f"file://{target}"
    return pages


def _receipt_rows(receipt_id: str) -> list[dict]:
    return [r for r in store.rows("collection_receipt")
            if str(r.get("回执编号")) == receipt_id]


def _report_rows(report_id: str) -> list[dict]:
    return [r for r in store.rows("daily_report")
            if str(r.get("报告编号")) == report_id]


def _batch_row(batch_id: str) -> dict:
    return next(r for r in store.rows("batch_store")
                if str(r.get("批次编号")) == batch_id)


def _parse_logs(job_id: str) -> list[dict]:
    return [r for r in store.rows("parse_log")
            if str(r.get("记录编号", "")).startswith(f"PLOG-{job_id}-")]


# ---------------------------------------------------------------------------
# 核心修复：缺规则的跳过页面不能再误判为零行校验通过
# ---------------------------------------------------------------------------

def test_missing_rule_not_counted_as_zero_row_pass(local_pages: dict[str, str]) -> None:
    """已验证站点的采集入口缺适用规则时，跳过的页面保留原因，批次记待验收。"""
    job_id = "CRAW-DR-NORULE"
    urls = [local_pages["article.html"], local_pages["quotes_p1.html"],
            local_pages["quotes_p2.html"]]

    batch = run_crawl_pipeline(
        site_row=SITE, job_row=_job_row(job_id, urls),
        extract_rows=[ARTICLE_RULE],  # 清单没有规则
        clean_rows=CLEAN_RULES,
    )

    assert batch is not None
    assert batch.status == "待验收", \
        f"缺规则的跳过页面不得误判为零行校验通过，实际: {batch.status}"
    assert batch.row_count == 0, "没有跑过的页面不产生数据行"
    assert batch.page_scope == [urls[0]], "落库范围只含实际跑过的文章页"

    # 空结果回执交代：文章页跑过且确为空，清单两页缺规则还没完成
    receipt = {r["页面URL"]: r for r in _receipt_rows(batch.receipt_id)}
    assert receipt[urls[0]]["页面结果"] == PageOutcome.EMPTY.value
    assert receipt[urls[0]]["适用规则"] == "CONT-DR-ARTICLE", "空页须留有来源规则"
    for url in urls[1:]:
        assert receipt[url]["页面结果"] == PageOutcome.NO_RULE.value
        assert "没有适用的解析规则" in receipt[url]["原因"], "缺规则须保留原因"
        assert receipt[url]["pending"] is True, "缺规则页面应待核"

    # 待核页面证据可在解析日志查到
    logs = {r["来源URL"]: r for r in _parse_logs(job_id)}
    assert logs[urls[0]]["解析结果"] == "空页", "文章页零行依据应可查"
    for url in urls[1:]:
        assert logs[url]["异常类型"] == "缺规则"

    # 批次存储从同一份空结果回执取数
    row = _batch_row(batch.batch_id)
    assert int(row["数据行数"]) == 0
    assert row["落库范围"] == urls[0]
    assert row["回执编号"] == batch.receipt_id


# ---------------------------------------------------------------------------
# 完整场景：缺规则 → 补规则（7/中断）→ 提交日报 → 恢复补跑（12）→ 再出表
# ---------------------------------------------------------------------------

def test_daily_report_backfill_scenario(local_pages: dict[str, str]) -> None:
    job_id = "CRAW-DR-JOB"
    urls = [local_pages["article.html"], local_pages["quotes_p1.html"],
            local_pages["quotes_p2.html"]]
    p2_path = Path(local_pages["quotes_p2.html"][len("file://"):])

    # -- 第 0 步：上一个完整批次（全部规则齐备、页面可读）------------------
    previous = run_crawl_pipeline(
        site_row=SITE, job_row=_job_row(job_id, urls),
        extract_rows=[ARTICLE_RULE, QUOTE_RULE], clean_rows=CLEAN_RULES,
    )
    assert previous is not None
    assert previous.status == "校验通过"
    assert previous.row_count == 19, f"上一完整批次应为 7+12=19 行，实际: {previous.row_count}"
    previous_checksum = _batch_row(previous.batch_id)["校验摘要"]

    # -- 第 1 步：清单缺规则，文章页没有匹配内容 ----------------------------
    current = run_crawl_pipeline(
        site_row=SITE, job_row=_job_row(job_id, urls),
        extract_rows=[ARTICLE_RULE], clean_rows=CLEAN_RULES,
    )
    assert current is not None
    assert current.status == "待验收"
    assert current.row_count == 0

    # -- 第 2 步：补齐清单规则后补跑，第一页抽到 7，第二页读取中断 ----------
    p2_off = p2_path.with_suffix(".off")
    p2_path.rename(p2_off)  # 模拟读取中断
    current = resume_crawl_pipeline(
        site_row=SITE, job_row=_job_row(job_id, urls),
        extract_rows=[ARTICLE_RULE, QUOTE_RULE], clean_rows=CLEAN_RULES,
        batch_id=current.batch_id,
    )
    assert current is not None
    assert current.status == "待验收", f"第二页读取中断不得放行，实际: {current.status}"
    assert current.row_count == 7, f"第一页应抽到 7 行，实际: {current.row_count}"

    receipt = {r["页面URL"]: r for r in _receipt_rows(current.receipt_id)}
    assert receipt[urls[0]]["页面结果"] == PageOutcome.EMPTY.value, \
        "重试后文章页的空证据要留住"
    assert receipt[urls[1]]["页面结果"] == PageOutcome.PARSED.value
    assert int(receipt[urls[1]]["行数"]) == 7
    assert receipt[urls[2]]["页面结果"] == PageOutcome.READ_FAILED.value
    assert "读取中断" in receipt[urls[2]]["原因"], "读取失败须保留原因"

    # 当前批次首条报价已有解析证据；报价明细已入库 7 条
    logs = [r for r in _parse_logs(job_id) if r["来源URL"] == urls[1]]
    assert any(r["解析结果"] == "成功" for r in logs), "首条报价应有解析证据"
    stored = [r for r in store.rows("batch_record")
              if str(r.get("批次编号")) == current.batch_id]
    assert len(stored) == 7

    # -- 第 3 步：值班员提交日报取数 ----------------------------------------
    report1 = submit_daily_report(job_id)
    assert report1 is not None
    # 两个可供采用的报价版本在发布范围上冲突：采用上一个完整批次发布
    assert report1.adopted_batch_id == previous.batch_id
    assert report1.publish_scope == previous.page_scope
    # 发布范围与批次存储的实际落库范围对得上
    assert ",".join(report1.publish_scope) == _batch_row(previous.batch_id)["落库范围"]
    # 新报价（当前批次的 7 条）登记为待验收，不是已发布
    new_quote_lines = [l for l in report1.lines
                       if l.batch_id == current.batch_id and l.page_url == urls[1]]
    assert len(new_quote_lines) == 1
    assert new_quote_lines[0].acceptance == "待验收"
    assert new_quote_lines[0].row_count == 7
    # 待核的第二页在清单里保留原因
    pending_lines = [l for l in report1.lines
                     if l.batch_id == current.batch_id and l.page_url == urls[2]]
    assert len(pending_lines) == 1
    assert pending_lines[0].acceptance == "待核"
    assert "读取中断" in pending_lines[0].reason
    # 出表明细：采用批次的每个页面一行，来源版本是批次校验摘要
    adopted_lines = [l for l in report1.lines if l.batch_id == previous.batch_id]
    assert {l.page_url for l in adopted_lines} == set(urls)
    assert all(l.acceptance == "已发布" for l in adopted_lines)
    assert all(l.source_version == previous_checksum for l in adopted_lines)

    # -- 第 4 步：恢复第二页后补跑抽到 12，重复运行同一补跑 ------------------
    p2_off.rename(p2_path)  # 恢复第二页
    current = resume_crawl_pipeline(
        site_row=SITE, job_row=_job_row(job_id, urls),
        extract_rows=[ARTICLE_RULE, QUOTE_RULE], clean_rows=CLEAN_RULES,
        batch_id=current.batch_id,
    )
    assert current is not None
    assert current.status == "校验通过", f"补齐后应确认，实际: {current.status}"
    assert current.row_count == 19, f"应为 7+12=19 行，实际: {current.row_count}"

    receipt = {r["页面URL"]: r for r in _receipt_rows(current.receipt_id)}
    assert receipt[urls[0]]["页面结果"] == PageOutcome.EMPTY.value, \
        "补跑后文章页的空证据仍要留住"
    assert int(receipt[urls[2]]["行数"]) == 12

    # 重复运行同一补跑：已入库的报价只记一次
    again = resume_crawl_pipeline(
        site_row=SITE, job_row=_job_row(job_id, urls),
        extract_rows=[ARTICLE_RULE, QUOTE_RULE], clean_rows=CLEAN_RULES,
        batch_id=current.batch_id,
    )
    assert again is not None and again.status == "校验通过"
    stored = [r for r in store.rows("batch_record")
              if str(r.get("批次编号")) == current.batch_id]
    assert len(stored) == 19, f"报价只记一次，应为 19 条，实际: {len(stored)}"
    stored_p1 = [r for r in stored if str(r.get("来源页面")) == urls[1]]
    stored_p2 = [r for r in stored if str(r.get("来源页面")) == urls[2]]
    assert len(stored_p1) == 7 and len(stored_p2) == 12

    # 验收结果同步到日报取数清单：前一次清单里的待验收/待核行转为已确认
    r1_rows = _report_rows(report1.report_id)
    synced = [r for r in r1_rows if str(r.get("批次编号")) == current.batch_id]
    assert synced and all(r["验收状态"] == "已确认" for r in synced)
    synced_p2 = next(r for r in synced if str(r.get("页面范围")) == urls[2])
    assert int(synced_p2["行数"]) == 12

    # -- 第 5 步：再次出表 ----------------------------------------------------
    report2 = submit_daily_report(job_id)
    assert report2 is not None
    assert report2.report_id != report1.report_id
    assert report2.adopted_batch_id == current.batch_id, "新确认批次应被采用"
    assert report2.publish_scope == current.page_scope
    assert ",".join(report2.publish_scope) == _batch_row(current.batch_id)["落库范围"]

    # 新确认的 7 和 12 各有一行
    published = [l for l in report2.lines if l.acceptance == "已发布"]
    line_p1 = [l for l in published if l.page_url == urls[1]]
    line_p2 = [l for l in published if l.page_url == urls[2]]
    assert len(line_p1) == 1 and line_p1[0].row_count == 7, "7 条报价应各占一行"
    assert len(line_p2) == 1 and line_p2[0].row_count == 12, "12 条报价应各占一行"
    assert line_p1[0].source_version == _batch_row(current.batch_id)["校验摘要"]

    # 前一次日报仍能查到它采用的版本
    r1_rows = _report_rows(report1.report_id)
    r1_adopted = [r for r in r1_rows if str(r.get("批次编号")) == previous.batch_id]
    assert r1_adopted, "前一次日报采用的版本应仍可查"
    assert all(str(r.get("来源版本")) == previous_checksum for r in r1_adopted)
    assert all(r["验收状态"] == "已发布" for r in r1_adopted)

    # 文章的零行依据和先前确认的批次摘要仍可查
    article_logs = [r for r in _parse_logs(job_id) if r["来源URL"] == urls[0]]
    assert any(r["解析结果"] == "空页" for r in article_logs), "文章零行依据应可查"
    assert _batch_row(previous.batch_id)["校验摘要"] == previous_checksum, \
        "先前确认的批次摘要应保持可查"
    assert _batch_row(previous.batch_id)["status"] == "校验通过"


def test_report_compatible_with_legacy_confirmed_batch() -> None:
    """兼容已有的已确认批次：没有页面回执的历史批次按批次摘要采用。"""
    job_id = "CRAW-DR-LEGACY"
    store.rows("batch_store").append({
        "id": 9901, "批次编号": "BATCH-LEGACY-001", "来源作业": job_id,
        "数据行数": 25, "存储路径": "/data/batches/legacy/", "压缩格式": "gzip",
        "Schema版本": "v1", "校验摘要": "legacychecksum01", "入库时间": "2026-11-20",
        "status": "校验通过", "pending": False, "abnormal": False,
    })

    report = submit_daily_report(job_id)

    assert report is not None
    assert report.adopted_batch_id == "BATCH-LEGACY-001"
    assert len(report.lines) == 1
    assert report.lines[0].acceptance == "已发布"
    assert report.lines[0].row_count == 25
    assert report.lines[0].source_version == "legacychecksum01"
