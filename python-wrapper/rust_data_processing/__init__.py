"""Python bindings for the `rust-data-processing` Rust crate.

The native extension is built with PyO3 and maturin. Prefer the functions and classes
exported here rather than importing ``rust_data_processing._rust_data_processing`` directly.
"""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError, version

from ._rust_data_processing import (
    DataSet,
    extension_version,
    ingest_from_path,
    ingest_from_path_infer,
    infer_schema_from_path,
)

try:
    __version__ = version("rust-data-processing")
except PackageNotFoundError:
    __version__ = extension_version()

__all__ = [
    "DataSet",
    "__version__",
    "extension_version",
    "ingest_from_path",
    "ingest_from_path_infer",
    "infer_schema_from_path",
]
