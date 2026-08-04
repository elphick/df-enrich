"""
df-enrich: Pandas DataFrame accessor for schema-driven validation, derived columns, lookups, and profiling.
"""

from .accessor import EnrichAccessor

from importlib import metadata

try:
    __version__ = metadata.version('df-enrich')
except metadata.PackageNotFoundError:
    # Package is not installed
    pass

__all__ = ["EnrichAccessor"]
