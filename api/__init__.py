"""Thin read API over the aggregate marts.

Connects as ``airspace_reader`` only, so it is structurally incapable of
serving per-airframe data. See CONSTRAINTS.md.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
