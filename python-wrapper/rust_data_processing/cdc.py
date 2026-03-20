"""CDC boundary types mirroring ``rust_data_processing::cdc`` (no connector shipped in Phase 1a).

These are plain Python types for contracts and documentation; they are not produced by the
native extension yet. See the Rust module for field semantics.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Optional


class CdcOp(Enum):
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    TRUNCATE = "truncate"


@dataclass
class TableRef:
    name: str
    schema: Optional[str] = None

    @classmethod
    def with_schema(cls, schema: str, name: str) -> TableRef:
        return cls(name=name, schema=schema)


@dataclass
class CdcCheckpoint:
    token: str


@dataclass
class SourceMeta:
    source: Optional[str] = None
    checkpoint: Optional[CdcCheckpoint] = None


@dataclass
class RowImage:
    """Ordered (column_name, value) pairs."""

    values: list[tuple[str, Any]] = field(default_factory=list)

    @classmethod
    def new(cls, values: list[tuple[str, Any]]) -> RowImage:
        return cls(values=values)


@dataclass
class CdcEvent:
    meta: SourceMeta
    table: TableRef
    op: CdcOp
    before: Optional[RowImage] = None
    after: Optional[RowImage] = None


__all__ = [
    "CdcCheckpoint",
    "CdcEvent",
    "CdcOp",
    "RowImage",
    "SourceMeta",
    "TableRef",
]
