"""发布目录的异常类型。

冲突分两类，调用方需要区分：
- ValidationConflict / CommitConflict：读集与当前映射不一致，候选可以重基后重试；
- IdempotencyConflict：同一操作ID绑定了不同内容，必须换操作ID。
"""


class CatalogError(Exception):
    """目录操作的基类异常。"""


class SpecError(CatalogError):
    """候选规范自身不合法（写表缺读集、缺约束依赖、隐式删表等）。"""


class StagingError(CatalogError):
    """暂存不合法：文件身份不符、行数不符或暂存未完成就提交元数据。"""


class ValidationFailed(CatalogError):
    """发布前核对失败：引用文件、行数或固定外键约束不满足。"""


class ValidationConflict(CatalogError):
    """验证时读集与当时当前映射已不一致，候选需要重基。"""


class CommitConflict(CatalogError):
    """提交时CAS失败：验证之后另一个提交改了读集依赖，旧验证结论作废。"""

    def __init__(self, message, comparison=None):
        super().__init__(message)
        self.comparison = comparison or []


class IdempotencyConflict(CatalogError):
    """同一操作ID提交了不同内容摘要。"""


class UnknownSnapshot(CatalogError):
    """引用了目录中不存在的快照。"""


class UnknownRelease(CatalogError):
    """引用了目录中不存在的release。"""


class UnknownTable(CatalogError):
    """句柄映射中不存在该表。"""
