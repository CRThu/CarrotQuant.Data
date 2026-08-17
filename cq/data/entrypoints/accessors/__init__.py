"""
cqdata/entrypoints/accessors/__init__.py

OOP 便捷访问层包导出。
向外导出 default, ashare, aindex 单例与主要访问类。
"""

from cq.data.entrypoints.accessors.base import DefaultConfig, default
from cq.data.entrypoints.accessors.ashare import (
    AShare,
    AShareKline,
    AShareAdjFactor,
    AShareConcept,
    AShareIndustry,
    AShareDragonTiger,
    AShareInstTrade
)
from cq.data.entrypoints.accessors.aindex import AIndex, AIndexKline

# 实例化命名空间单例
ashare = AShare(parent_default=default)
aindex = AIndex(parent_default=default)

__all__ = [
    "DefaultConfig",
    "default",
    "ashare",
    "aindex",
    "AShare",
    "AIndex",
    "AShareKline",
    "AShareAdjFactor",
    "AShareConcept",
    "AShareIndustry",
    "AShareDragonTiger",
    "AShareInstTrade",
    "AIndexKline",
]
