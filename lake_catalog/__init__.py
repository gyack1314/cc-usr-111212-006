"""多表发布目录：一组不可变表快照作为同一仓库版本原子切换。"""
from .catalog import Catalog, Receipt, StagedSnapshot, ValidationTicket
from .errors import (CatalogError, CommitConflict, IdempotencyConflict,
                     SpecError, StagingError, UnknownRelease, UnknownSnapshot,
                     UnknownTable, ValidationConflict, ValidationFailed)
from .reader import ReleaseHandle
from .spec import CandidateSpec

__all__ = [
    "Catalog", "Receipt", "StagedSnapshot", "ValidationTicket",
    "CandidateSpec", "ReleaseHandle",
    "CatalogError", "CommitConflict", "IdempotencyConflict", "SpecError",
    "StagingError", "UnknownRelease", "UnknownSnapshot", "UnknownTable",
    "ValidationConflict", "ValidationFailed",
]
