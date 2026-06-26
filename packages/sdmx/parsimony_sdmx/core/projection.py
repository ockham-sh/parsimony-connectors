"""Stream series-key tuples into ``SeriesRecord`` with no intermediate DataFrame.

We iterate the series generator, compute each title inline via the
small pre-resolved codelist map, and yield one ``SeriesRecord`` at a
time so the downstream ``ParquetWriter`` can consume it
batch-by-batch. This keeps peak memory bounded by the codelist map
(typically a few MB) rather than by the cardinality of the dataflow's
series enumeration (which routinely runs to millions of rows on dense
flows).
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

from parsimony_sdmx.core.errors import TitleBuildError
from parsimony_sdmx.core.models import DimensionValue, SeriesRecord
from parsimony_sdmx.core.titles import compose_series_title

SeriesTitleProvider = Callable[[str], str | None]
"""Hook: ``source_title(series_id) -> title`` for providers with per-series titles."""

SERIES_KEY_SEP = "."


def project_series(
    dataset_id: str,
    series_dim_values: Iterable[Mapping[str, str]],
    dsd_order: Sequence[str],
    labels: Mapping[str, Mapping[str, str]],
    source_title: SeriesTitleProvider | None = None,
) -> Iterator[SeriesRecord]:
    """Yield one ``SeriesRecord`` per series key.

    Parameters
    ----------
    dataset_id:
        The dataset this stream belongs to; stamped on every record.
    series_dim_values:
        Stream of ``{dim_id: code}`` mappings, one per series. Usually a
        generator drained from ``sdmx1.series_keys()`` so peak memory
        stays bounded.
    dsd_order:
        Non-``TIME_PERIOD`` dimension IDs in DSD order. This is the
        caller's responsibility — the projection neither filters time
        nor reorders.
    labels:
        Pre-resolved ``{dim_id: {code_id: label}}``. See
        :mod:`parsimony_sdmx.core.codelists`.
    source_title:
        Optional hook invoked per-series for SDMX sources that expose a
        human title per series. Signature: ``series_id -> title | None``.

    Raises
    ------
    TitleBuildError
        If any series is missing a value for a dimension in
        ``dsd_order`` (malformed SDMX series key — an impossible state
        for well-formed input).
    """
    if not dsd_order:
        raise TitleBuildError(f"dsd_order is empty for dataset {dataset_id!r} — cannot build series keys")

    for dim_values in series_dim_values:
        series_id = _series_id(dim_values, dsd_order, dataset_id)
        fallback_title = compose_series_title(dim_values, dsd_order, labels)
        title = source_title(series_id) if source_title is not None else None
        yield SeriesRecord(
            id=series_id,
            dataset_id=dataset_id,
            title=title.strip() if title and title.strip() else fallback_title,
            dimensions=_dimension_values(dim_values, dsd_order, labels),
        )


def _dimension_values(
    dim_values: Mapping[str, str],
    dsd_order: Sequence[str],
    labels: Mapping[str, Mapping[str, str]],
) -> tuple[DimensionValue, ...]:
    """Return SDMX-native dimension values in DSD order."""

    out: list[DimensionValue] = []
    for dim_id in dsd_order:
        code = dim_values.get(dim_id)
        if not code:
            continue
        label = labels.get(dim_id, {}).get(code) or None
        out.append(DimensionValue(id=dim_id, code=code, label=label))
    return tuple(out)


def _series_id(
    dim_values: Mapping[str, str],
    dsd_order: Sequence[str],
    dataset_id: str,
) -> str:
    parts: list[str] = []
    for dim_id in dsd_order:
        code = dim_values.get(dim_id)
        if not code:
            raise TitleBuildError(
                f"Dataset {dataset_id!r}: series missing value for dimension {dim_id!r}; "
                f"got dim_values={dict(dim_values)!r}"
            )
        parts.append(code)
    return SERIES_KEY_SEP.join(parts)
