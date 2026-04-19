# Commercial Provider Licence Audit

**Status:** PLACEHOLDER — awaits legal review.
**Binding document:** this audit is the authoritative record for whether a
commercial-provider connector ships from this monorepo (Apache 2.0) or as a
separate external distribution.

---

## Purpose

For every commercial data provider whose connector has been proposed for
this monorepo, this file records:

- Provider name
- Connector name (`parsimony-<name>`)
- Provider's terms-of-service URL + audit date
- Audit outcome: **in-monorepo** or **external-only**
- Audit rationale

See [GOVERNANCE.md §6](../GOVERNANCE.md#6-licence) for the policy this
audit enforces.

---

## Providers covered by the current audit round

These providers are candidates for inclusion in the monorepo. Every entry is currently **pending** until legal review completes.

| Provider | Connector | ToS reviewed | Outcome | Rationale |
|---|---|---|---|---|
| Alpha Vantage | `parsimony-alpha-vantage` | pending | pending | — |
| Bank for International Settlements | `parsimony-bis` | pending | pending | via SDMX |
| Bank of Canada | `parsimony-boc` | pending | pending | — |
| Bank of Japan | `parsimony-boj` | pending | pending | — |
| Banco de la República (Colombia) | `parsimony-bdp` | pending | pending | — |
| Banque de France | `parsimony-bdf` | pending | pending | — |
| Banco de España | `parsimony-bde` | pending | pending | — |
| Bureau of Labor Statistics | `parsimony-bls` | pending | pending | U.S. government — expected Apache-compatible |
| CoinGecko | `parsimony-coingecko` | pending | pending | commercial API, free tier ToS |
| Destatis | `parsimony-destatis` | pending | pending | German federal statistics |
| Energy Information Administration | `parsimony-eia` | pending | pending | U.S. government — expected Apache-compatible |
| EODHD | `parsimony-eodhd` | pending | pending | commercial subscription |
| European Central Bank | `parsimony-ecb` | pending | pending | via SDMX |
| Eurostat | `parsimony-eurostat` | pending | pending | via SDMX |
| Federal Reserve Economic Data | `parsimony-fred` | pending | pending | U.S. government — already shipping |
| Financial Modeling Prep | `parsimony-fmp` | pending | pending | commercial subscription |
| Financial Modeling Prep Screener | `parsimony-fmp-screener` | pending | pending | same as FMP |
| FinancialReports.eu | `parsimony-financial-reports` | pending | pending | commercial |
| Finnhub | `parsimony-finnhub` | pending | pending | commercial subscription |
| International Labour Organization | `parsimony-ilo` | pending | pending | via SDMX |
| International Monetary Fund | `parsimony-imf` | pending | pending | via SDMX |
| OECD | `parsimony-oecd` | pending | pending | via SDMX |
| Polymarket | `parsimony-polymarket` | pending | pending | — |
| Reserve Bank of Australia | `parsimony-rba` | pending | pending | — |
| Riksbank | `parsimony-riksbank` | pending | pending | — |
| SEC Edgar | `parsimony-sec-edgar` | pending | pending | U.S. government — expected Apache-compatible |
| Schweizerische Nationalbank | `parsimony-snb` | pending | pending | — |
| Tiingo | `parsimony-tiingo` | pending | pending | commercial subscription |
| U.S. Treasury | `parsimony-treasury` | pending | pending | U.S. government — expected Apache-compatible |
| World Bank | `parsimony-world-bank` | pending | pending | via SDMX |

SDMX-protocol providers are grouped under `parsimony-sdmx` in practice
(the connector package ships as one distribution servicing multiple
agencies), but each agency's individual ToS is audited separately because
redistribution rights differ per-agency.

---

## Audit procedure

For each unresolved entry above:

1. Fetch the provider's current terms of service (record URL + date).
2. Identify any clause forbidding:
   - Redistribution of example responses or endpoint documentation
   - Commercial use of the client (affects Apache 2.0 compatibility)
   - Sub-licensing (an Apache 2.0 requirement)
3. If all three are clear → outcome **in-monorepo**.
4. If any fails → outcome **external-only**, with a note on which clause.
5. Update the table with `YYYY-MM-DD` audit date and the outcome.

---

## When an audit result changes

A provider's ToS may change between audit and connector shipping. If the
outcome flips:

- **in-monorepo → external-only:** the connector is scheduled for removal
  per GOVERNANCE.md §4 (deprecation). An external replacement package may
  be spun up.
- **external-only → in-monorepo:** a new PR may ingest the external
  package following the normal CONTRIBUTING.md flow.

---

## Audit record

Filled in as audits complete. Each entry below supersedes the
corresponding row in the table above once added.

*(no audits completed yet)*
