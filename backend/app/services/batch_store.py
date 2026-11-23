"""__NAME__领域服务：字段、状态和动作来自业务配置，数据处理骨架由基类复用。"""
from __future__ import annotations

from app.services.base import BaseModuleService


class BatchStoreService(BaseModuleService):
    MODULE = "batch_store"
    ENTITY = "采集批次"
    OPERATION_TERM = "采集"
    LIST_FIELDS = ["批次编号", "来源作业", "数据行数", "存储路径", "压缩格式", "Schema版本", "校验摘要", "入库时间"]
    REQUIRED_FIELDS = ["批次编号", "来源作业", "数据行数"]
    STATUS_ORDER = ["采集中", "已落盘", "待验收", "校验通过", "校验失败"]
    ACTION_RULES = {"触发校验": "已落盘", "标记通过": "校验通过", "标记失败": "校验失败"}
    NEGATIVE_ACTIONS = []
