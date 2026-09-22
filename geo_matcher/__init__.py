from importlib.metadata import PackageNotFoundError, version

from . import bboxes
from .config import (
    AnySource,
    BBox,
    CsvSource,
    DatasetConfig,
    GeometryType,
    LhsKind,
    MatchSource,
    OgrSource,
    ParquetSource,
    UprnSource,
    UsrnSource,
)
from .join import (
    FilteredMode,
    JoinFn,
    JoinMode,
    LineJoinPhases,
    NationalMode,
    execute_join,
    get_join,
)
from .matcher import GeoMatcher

try:
    __version__ = version("geo-matcher")
except PackageNotFoundError:
    __version__ = "unknown"

__all__ = [
    "GeoMatcher",
    "__version__",
    "AnySource",
    "BBox",
    "CsvSource",
    "DatasetConfig",
    "GeometryType",
    "LhsKind",
    "MatchSource",
    "OgrSource",
    "ParquetSource",
    "UprnSource",
    "UsrnSource",
    "FilteredMode",
    "JoinMode",
    "LineJoinPhases",
    "NationalMode",
    "JoinFn",
    "execute_join",
    "get_join",
    "bboxes",
]
