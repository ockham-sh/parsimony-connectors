"""Error-mapping contract for parsimony-finnhub.

Finnhub's status semantics differ from the canonical table on a single point,
verified live: an **invalid key returns 401** (→ ``UnauthorizedError``) and a
**premium-only endpoint on a free plan returns 403** (→ ``PaymentRequiredError``).
The canonical ``check_status`` table folds 403 into ``UnauthorizedError``, so
``finnhub_get`` special-cases 403 with a plain ``if`` on the response status
*before* ``check_status``. 401/402/429/5xx all follow the canonical table; only
403 is special-cased.

The full canonical status table (401, 402, 429, 500, 503) is exercised via
``ErrorMappingSuite`` because ``finnhub_get`` delegates everything except 403 to
``check_status``. The 403 → PaymentRequired case has its own assertion below,
and an explicit 401 → Unauthorized assertion pins the dual-meaning distinction.
"""

from __future__ import annotations

import httpx
import pytest
import respx
from parsimony.errors import PaymentRequiredError, UnauthorizedError
from parsimony_test_support import CANARY_KEY, ErrorMappingSuite, assert_no_secret_leak

from parsimony_finnhub import enumerate_finnhub, finnhub_search

_ROUTE = "https://finnhub.io/api/v1/search"
_SYMBOL_ROUTE = "https://finnhub.io/api/v1/stock/symbol"


class TestFinnhubSearchErrorMapping(ErrorMappingSuite):
    connector = finnhub_search
    call_kwargs = {"query": "apple"}
    route_url = _ROUTE
    provider = "finnhub"
    # Canonical table applies: 401→Unauthorized, 402→Payment, 429→RateLimit,
    # 5xx→Provider. 403 is the one finnhub-specific divergence (asserted below).


@respx.mock
def test_finnhub_403_maps_to_payment_required() -> None:
    """A premium-only endpoint returns 403 → PaymentRequiredError (plan gate)."""
    respx.get(_ROUTE).mock(return_value=httpx.Response(403, text="You don't have access to this resource."))

    bound = finnhub_search.bind(api_key=CANARY_KEY)
    with pytest.raises(PaymentRequiredError) as exc_info:
        bound(query="apple")

    assert_no_secret_leak(exc_info.value)
    assert exc_info.value.provider == "finnhub"


@respx.mock
def test_finnhub_401_maps_to_unauthorized_not_payment() -> None:
    """An invalid key returns 401 → UnauthorizedError (NOT Payment).

    This pins the dual-meaning distinction: finnhub uses 401 for bad creds and
    403 for plan restrictions, so a status-only 403→Payment mapping is correct
    here and must NOT swallow a 401.
    """
    respx.get(_ROUTE).mock(return_value=httpx.Response(401, json={"error": "Invalid API key."}))

    bound = finnhub_search.bind(api_key=CANARY_KEY)
    with pytest.raises(UnauthorizedError) as exc_info:
        bound(query="apple")

    assert_no_secret_leak(exc_info.value)
    assert exc_info.value.provider == "finnhub"


@respx.mock
def test_enumerator_403_maps_to_payment_required() -> None:
    """The enumerator path is now mapped too (it had zero error mapping before)."""
    respx.get(_SYMBOL_ROUTE).mock(return_value=httpx.Response(403, text="no access"))

    bound = enumerate_finnhub.bind(api_key=CANARY_KEY)
    with pytest.raises(PaymentRequiredError) as exc_info:
        bound(exchange="US")

    assert_no_secret_leak(exc_info.value)
    assert exc_info.value.provider == "finnhub"
