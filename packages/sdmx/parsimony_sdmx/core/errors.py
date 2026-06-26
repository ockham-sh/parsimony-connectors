"""Typed exception hierarchy for provider and worker code.

Providers raise specific subclasses; the per-dataset worker catches them
at its top-level boundary and classifies into a ``FailureKind`` on the
returned ``DatasetOutcome`` so the parent driver can sort retryable from
permanent failures without log-scraping.
"""


class ProviderError(Exception):
    """Base class for any provider-layer failure."""


class SdmxFetchError(ProviderError):
    """Failure fetching SDMX metadata from an agency endpoint."""


class TitleBuildError(ProviderError):
    """Failure composing a series title (missing DSD, malformed codelist)."""


class CodelistMissingError(TitleBuildError):
    """Required codelist is absent or empty for a dataset."""


class ParquetWriteError(ProviderError):
    """Failure writing a parquet output."""
