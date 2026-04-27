# Governance — parsimony-connectors

**Status:** Pre-launch, v1. Written before the first external contributor
arrives because writing governance after the fact is rude and too late.

This document covers the **policies** of the monorepo: what PRs are accepted,
who maintains what, how abandoned code gets handled, how a connector can
graduate out of the monorepo. For the **mechanics** of contributing — setup,
commands, checklists — see [CONTRIBUTING.md](CONTRIBUTING.md).

---

## 1. Acceptance criteria

A new connector PR is merged when, and only when, all of the following are
satisfied:

1. **Passes the conformance suite.** `parsimony list --strict` exits with
   code 0 with the package installed. This is the objective gate; CI enforces it.
2. **Belongs in the monorepo by the binary rule.** The connector's source
   can be shared publicly under Apache 2.0 (see §6).
3. **Has an active maintainer.** The PR author commits to acting as the
   connector's first steward for at least 90 days after merge, or names
   another contributor who does. Recorded in `CODEOWNERS`.
4. **Has tests.** At minimum a conformance test plus unit tests for any
   non-trivial parameter handling, date parsing, or pagination logic.
5. **Is documented.** The PR description names the provider, its API docs,
   its pricing model, and any ToS caveats. The connector's `__init__.py`
   module docstring summarises what the connector covers.
6. **Does not leak secrets.** No API keys, `.env` files, or personal
   credentials committed.
7. **Respects the contract.** Imports only from the surface enumerated in
   the kernel's `docs/contract.md`. Stable surface only, unless the PR
   description justifies a provisional-surface dependency.

The founder is the merge button until per-connector stewards emerge. No
founder judgement is applied to the six criteria above — they are
objective.

---

## 2. Stewardship

Stewardship is **emergent, not appointed**.

### How stewards emerge

When someone contributes a new connector, they become its first steward
automatically. `CODEOWNERS` gets the new assignment as part of the merge.

### How stewards earn authority

A steward is responsible for:

- Reviewing PRs touching their connector (primary reviewer; founder is fallback)
- Responding to issues tagged with the connector name within 14 days
- Keeping the connector green against kernel releases (or explicitly quarantining — see §4)

After 90 days of consistent stewardship with at least two merged PRs from
non-steward contributors reviewed by the steward, the steward gains the
ability to self-merge after one additional founder review. After 365 days
with the same track record, the steward may merge without founder review.

Track record is measured per-connector. Someone is a senior steward on
`parsimony-fred` and a new contributor on `parsimony-sdmx` at the same time.

### Taking over an abandoned connector

A connector is **abandoned** when its steward has not responded to issues
or PRs touching the connector for **90 consecutive days**, and the kernel
CI matrix reports the connector as broken or skipped.

Takeover process:

1. Open a GitHub issue titled `Steward takeover: parsimony-<name>` naming
   the current steward, the last response date, and evidence of
   unresponsiveness (linked issues / PRs).
2. Wait 14 days. If the current steward responds and engages, the takeover
   is withdrawn.
3. Otherwise, the founder confirms the takeover and updates `CODEOWNERS`.
   The new steward inherits all responsibilities in the preceding
   subsection.

The departing steward is not blamed. Maintaining a connector is real work
and people's availability changes. The takeover process exists so the
connector stays healthy, not to penalise anyone.

---

## 3. Review authority

Until a connector has its own senior steward:

- **Founder** is the merge button.
- **New steward** is the primary reviewer; founder does a second review.
- **Non-steward contributors** open PRs; the steward (or founder) reviews.

Founder authority is delegated, not retained. Self-merge rights transfer
to the steward as described in §2; the founder steps back.

---

## 4. Deprecation policy

A connector gets **quarantined** — excluded from the next release but not
deleted — when any of the following is true:

1. The conformance suite fails on `main` for 14 consecutive days and no
   open PR fixes it.
2. The upstream provider has announced or executed a breaking API change
   that no PR has adapted to within 30 days.
3. The connector's steward has been absent for 90 days and no takeover
   has been opened.

Quarantine mechanics:

- The connector's `packages/<name>/pyproject.toml` keeps `version` unchanged.
- The release workflow skips the quarantined package (does not bump, does
  not publish).
- A `QUARANTINE.md` is added at `packages/<name>/QUARANTINE.md` explaining
  why and when. The file is removed once the connector is green again.
- After **180 days** in quarantine with no open takeover PR, the connector
  is **deleted** from the monorepo. Its PyPI package is left in place at
  its last-published version; the repo README notes the deletion.

Deletion is not reversible easily. A subsequent contributor may re-add the
connector under a new steward via the normal PR path.

---

## 5. Graduation policy

An officially-maintained connector may leave the monorepo and move to its
own external repository. This is rare; the monorepo is the gravitational
centre for officially-maintained code. Graduation is the **exception**.

### When graduation is appropriate

- The connector's primary steward is outside Ockham and wants to release
  on their own cadence, outside the monorepo's weekly release rhythm.
- The connector's scope has grown to where monorepo-level CI is slower
  than useful (its test suite takes >15 minutes, or its dependency tree
  conflicts with other connectors).
- The connector needs a fundamentally different CI / release pipeline
  that doesn't fit the matrix workflow.

### What graduation preserves

- **PyPI name.** `parsimony-fred` remains `parsimony-fred` whether it
  publishes from this monorepo or its own repo.
- **Contract.** The graduated connector continues to implement the kernel
  entry-point contract identically.
- **Discoverability.** A graduated connector continues to be discovered
  at runtime by consumers (MCP hosts, agent frameworks) through the
  kernel's `parsimony.discover` surface — entry-point metadata on the
  installed distribution is the authoritative source, so moving the
  repository does not affect discoverability.

### What graduation changes

- **Release ownership.** The steward controls release timing, OIDC
  trusted-publisher config, and CI infrastructure.
- **Source location.** Moves out of this monorepo to a new repository.
- **Review process.** Governed by the graduated repo's own CONTRIBUTING /
  GOVERNANCE; this document no longer applies to it.

### Graduation procedure

1. Steward opens a graduation PR on this repo including: the new
   repository URL, evidence that the new repo has CI green on the
   conformance suite, the new repo's OIDC trusted-publisher config is in
   place, and a release note for users.
2. Founder reviews for contract compliance and PyPI-naming continuity.
3. On merge, this monorepo's `packages/<name>/` is deleted (its
   pyproject is auto-discovered by the CI matrix via
   `find packages/*/pyproject.toml`, so no workflow edit is needed)
   and the README notes the graduation.
4. The next publish event comes from the new repo, not this one. The PyPI
   trusted publisher switches in PyPI's project settings at the same time.

### Ungraduation

A graduated connector may rejoin the monorepo if the conditions that
prompted graduation no longer apply. Same process in reverse.

---

## 6. Licence

This monorepo is **Apache 2.0**. Every file contributed agrees to Apache
2.0 redistribution. Contributors certify, by submitting a PR, that they
have the right to release the contribution under Apache 2.0.

Connectors wrap providers' documented HTTP APIs. The library we ship is
Apache-licensed; the provider's terms bind the end user (the API-key
holder), not this repository. What the repository requires from every
connector is structural, not contractual:

- **No provider SDK code.** Every HTTP client is written here — no
  copy-paste from the provider's official library.
- **No recorded responses.** Response cassettes are gitignored
  (`packages/*/tests/fixtures/**`); respx mocks are hand-authored from
  upstream API documentation.
- **Trademark use is nominative.** "FRED connector" / "Tiingo connector"
  is fine. "Official FRED client" / "Authorized by Tiingo" is not.
- **No affiliation claims.** Unless an explicit agreement exists,
  connectors do not imply endorsement or partnership.

---

## 7. Changes to this document

Governance changes happen by PR. The PR title starts with `governance:`.
The founder is the merge button on governance PRs; stewards may weigh in
and their input is given weight, but governance is not majority-vote.

The bar for a governance change is: a concrete situation that the current
rules handle badly, the proposed rule, and the rule's cost. Aesthetic
preferences and abstract improvements are not reasons to change
governance.

---

*This is governance v1. Revisit whenever a trigger from the kernel's
`DESIGN-distribution-model.md` §11 fires, or whenever a concrete situation
arises that this document handles badly.*
