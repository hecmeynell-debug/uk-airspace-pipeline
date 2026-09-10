"""Ingestion of public ADS-B state vectors from the OpenSky Network.

Scope is deliberately narrow: fetch a bounded region, validate, and land rows
in the ``raw`` schema with provenance. No scoring, classification, enrichment
or per-airframe analysis happens here or anywhere downstream. See
CONSTRAINTS.md.
"""

__all__ = ["__version__"]

__version__ = "0.1.0"
