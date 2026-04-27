"""Stream series-key tuples into ``SeriesRecord`` with no intermediate DataFrame.

Dominant memory in the legacy flow came from
``sdmx.to_pandas(series_keys_list).astype('string')`` materialising the
full enumeration before projection. Here we iterate the series
generator, compute each title inline via the small pre-resolved
codelist map, and yield one ``SeriesRecord`` at a time so the downstream
``ParquetWriter`` can consume it batch-by-batch.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence

from parsimony_sdmx.core.errors import TitleBuildError
from parsimony_sdmx.core.models import SeriesRecord
from parsimony_sdmx.core.titles import compose_series_title

SeriesIdAugment = Callable[[str, str], str]
"""Hook: ``augment(base_title, series_id) -> augmented_title``. ECB uses it."""

SeriesFragmentsAugment = Callable[[str], tuple[str, ...]]
"""Hook: ``augment_fragments(series_id) -> extra_fragments``.

ECB uses it to inject the per-series ``TITLE`` attribute as an additional
fragment. Closes the §3 escalation gap discovered in Phase 3 EXR eval:
codelist labels alone don't bridge colloquial-vocabulary queries
("yen daily exchange rate") to the rich natural-language overlay
embedded in ECB's TITLE ("Japanese yen/Euro ECB reference exchange rate").

Returned tuple is concatenated to the codelist-derived fragment tuple
without dedup — a fragment list with two near-duplicates is no worse for
mean-pooling than one without, and forcing dedup here would obscure
provider intent.
"""

SERIES_KEY_SEP = "."


def project_series(
    dataset_id: str,
    series_dim_values: Iterable[Mapping[str, str]],
    dsd_order: Sequence[str],
    labels: Mapping[str, Mapping[str, str]],
    augment: SeriesIdAugment | None = None,
    augment_fragments: SeriesFragmentsAugment | None = None,
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
    augment:
        Optional hook invoked per-series (ECB uses it to append
        ``TITLE`` / ``TITLE_COMPL``). Signature:
        ``(base_title, series_id) -> augmented_title``.

    Raises
    ------
    TitleBuildError
        If any series is missing a value for a dimension in
        ``dsd_order`` (malformed SDMX series key — an impossible state
        for well-formed input).
    """
    if not dsd_order:
        raise TitleBuildError(
            f"dsd_order is empty for dataset {dataset_id!r} — cannot build series keys"
        )

    for dim_values in series_dim_values:
        series_id = _series_id(dim_values, dsd_order, dataset_id)
        base_title = compose_series_title(dim_values, dsd_order, labels)
        title = augment(base_title, series_id) if augment is not None else base_title
        fragments = _fragments_from_labels(dim_values, dsd_order, labels)
        if augment_fragments is not None:
            extra = augment_fragments(series_id)
            if extra:
                fragments = fragments + tuple(extra)
        yield SeriesRecord(
            id=series_id,
            dataset_id=dataset_id,
            title=title,
            fragments=fragments,
        )


def _fragments_from_labels(
    dim_values: Mapping[str, str],
    dsd_order: Sequence[str],
    labels: Mapping[str, Mapping[str, str]],
) -> tuple[str, ...]:
    """Per-dim labels as atomic embedding fragments.

    One string per dimension, natural-language label when a codelist
    lookup succeeds, raw code as the fallback when no label is resolved.
    Mean-pooling these vectors is the compositional-embedding hypothesis
    documented in ``PLAN-sdmx-catalog-search.md`` §7b: deduping
    ``"Monthly"`` / ``"Spain"`` across the 500k-row catalog collapses
    embedder work by ~100-1000× with retrieval quality inside the
    Phase 3 eval budget.

    Codes are the fallback so dims without a codelist (e.g. plain
    numeric timestamps) still contribute *some* signal rather than
    dropping silently.
    """
    out: list[str] = []
    for dim_id in dsd_order:
        code = dim_values.get(dim_id)
        if not code:
            continue
        label = labels.get(dim_id, {}).get(code)
        out.append(label if label else code)
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
