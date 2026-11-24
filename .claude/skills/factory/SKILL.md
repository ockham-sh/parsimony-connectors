---
name: factory
description: Connector factory. One unattended step per run on one factory PR — build a queued source into a connector package, review a finished build, or fix review findings. State lives in PR labels. Invoked as /factory by the scheduled task.
---

# /factory

Read AGENTS.md first.

## Rules

1. One provider per PR.
2. Never merge.
3. Push only to `factory/*` branches, never to main or any other branch.
4. Use `gh api`, not `gh pr` or `gh issue` (GraphQL is blocked in cloud sessions).

Each run does one step on one PR, then ends with a one-line summary of what it did.
"End" below means: print that summary and stop.

## Setup

```bash
R=repos/ockham-sh/parsimony-connectors
```

```bash
git fetch origin main
```

```bash
make sync
```

The kernel must be checked out at `../parsimony` (the workspace resolves it there).
Work from the main checkout; the session starts on main.

## gh api reference

List open PRs from `factory/*` branches, oldest first, with labels:

```bash
gh api "$R/pulls?state=open&sort=created&direction=asc&per_page=100" --paginate --jq '.[] | select(.head.ref | startswith("factory/")) | [.number, .head.ref, .head.sha, ([.labels[].name] | join(","))] | @tsv'
```

Last commit date of one PR (pass the head sha from the list):

```bash
gh api "$R/commits/<sha>" --jq .commit.committer.date
```

Add a label (to a PR or an issue):

```bash
gh api -X POST "$R/issues/<n>/labels" -f 'labels[]=factory:building' --jq '[.[].name]'
```

Remove a label (`:` is written `%3A`):

```bash
gh api -X DELETE "$R/issues/<n>/labels/factory%3Abuilding" --jq '[.[].name]'
```

Comment (write the body to a file outside the repo first):

```bash
gh api -X POST "$R/issues/<n>/comments" -F body=@/tmp/factory/comment.md --jq .html_url
```

Open a PR:

```bash
gh api -X POST "$R/pulls" -f title='feat(<pkg>): add <Provider name> connector' -f head='factory/<pkg>' -f base=main -F body=@/tmp/factory/pr-body.md --jq .number
```

List open issues with a label, oldest first:

```bash
gh api "$R/issues?state=open&labels=factory:queued&sort=created&direction=asc&per_page=100" --jq '.[] | select(.pull_request | not) | [.number, .created_at, .title] | @tsv'
```

Count earlier `factory:changes-requested` rounds on a PR:

```bash
gh api "$R/issues/<n>/events?per_page=100" --jq '[.[] | select(.event == "labeled" and .label.name == "factory:changes-requested")] | length'
```

Read a PR's comments (review findings, owner answers):

```bash
gh api "$R/issues/<n>/comments?per_page=100" --jq '.[] | "--- \(.user.login) \(.created_at)\n\(.body)"'
```

Labels: `factory:queued` (issues), `factory:building`, `factory:changes-requested`,
`factory:approved`, `factory:blocked` (PRs).

## Pick work

List open PRs from `factory/*` branches. Ignore those labelled `factory:blocked`.
Go through the rules in order and take the first that matches any remaining PR
(oldest PR first within a rule):

1. Labelled `factory:building`, last commit under 3 hours old: end (another run is on it).
2. Labelled `factory:building`, older last commit: continue that build (see Build, resume).
3. No factory label: review it.
4. Labelled `factory:changes-requested`: fix it.
5. Labelled `factory:approved`: end (waiting for the owner to merge).
6. No PR left: build the oldest issue labelled `factory:queued`. No such issue: end.

## The check script

`factory/check.py` is the acceptance gate. It runs four checks, always all four:

```bash
python3 factory/check.py <pkg> --root <checkout dir> --base origin/main --out /tmp/factory/check-<pkg>
```

- **scope**: files changed against `--base` are only under `packages/<pkg>/`,
  `docs/_guidebook/providers/<pkg>.md`, `README.md`, `docs/index.md`, `connectors.json`
  (the `make readme-roster` output) and `uv.lock`.
- **verify**: `make verify PKG=<pkg>`.
- **live**: `pytest packages/<pkg> -m integration`. Fails on any failure, any skip, zero tests,
  or a verb with no passing live test named after it.
- **catalog**: builds the catalog with `packages/<pkg>/scripts/build_catalog.py --save`,
  validates it with `scripts/validate_catalog.py` against `catalog_tests/queries.yaml`,
  requires recall 1.0 on the required probes, and requires the entry count to equal the
  dossier's `Expected count:` unless a `Count difference:` line is present. A package with
  no build script passes only if dossier §4 says `Strategy: none` (native search).

It prints one line per check, writes `report.json` in `--out`, and exits non-zero on any
failure. Paste its summary lines verbatim wherever this skill asks for them.

**Live-test naming convention.** A live test covers verb `V` when its function name is
`test_V` or starts with `test_V_` (for example `test_eia_fetch_petroleum_spot_prices`
covers `eia_fetch`). When verbs share a prefix, a test counts for the longest verb it
matches (`test_eia_fetch_series_legacy_id` covers `eia_fetch_series`, not `eia_fetch`).
Verbs are the names in `parsimony_<pkg>.CONNECTORS`. A verb that cannot be tested live
(for example a paid-plan endpoint) is listed in dossier §5 on its own line:

```
No live test: `<verb>` — <reason>
```

Credentials for live tests come from environment variables (the package's `requires=`
names, read by `require_env`). A test that skips for a missing key fails the live check:
that is an owner ask, not something to work around.

## Build

New build from issue `#<issue>`. `<pkg>` is the provider key (package directory name,
snake_case). For a registration inside a protocol package (an agency on `sdmx`) the
branch is `factory/<platform>-<agency>`.

1. Create the branch with the dossier template as the first commit, open the PR, label it:

   ```bash
   git checkout -b factory/<pkg> origin/main
   ```

   Copy `docs/_guidebook/providers/_TEMPLATE.md` to `docs/_guidebook/providers/<pkg>.md`,
   fill the header lines, then:

   ```bash
   git add docs/_guidebook/providers/<pkg>.md
   ```

   ```bash
   git commit -m "docs(<pkg>): open provider dossier"
   ```

   ```bash
   git push -u origin factory/<pkg>
   ```

   Open the PR with a body containing `Closes #<issue>`, a one-line description of the
   source, and its pricing model and API docs link. Then add `factory:building` to the PR
   and remove `factory:queued` from the issue.

   `factory/check.py` does not support protocol packages yet. For a `sdmx` registration,
   stop after step 2: comment that the check script does not cover registrations inside
   `sdmx`, add `factory:blocked`, end.

2. Survey the source and write dossier §1–2 and §4–5, following
   `docs/_guidebook/README.md` §1 (process), §3 (decision tree), §5 (auth) and §7 (catalogs).
   Put the authoritative universe count in §1.4 as `Expected count: <n>`, plus
   `Count difference: <reason>` only if the catalog will deliberately differ.
   List every verb in §5, and any verb that cannot be tested live as
   ``No live test: `<verb>` — <reason>``. Commit and push.

3. If the owner is needed — an API key, a licence decision, a paid plan, an anti-bot wall,
   a kernel change, or a host the session's network blocks — comment the exact ask on the
   PR (what, why, the env var name or host, where to get it), add `factory:blocked`, end.
   Keep `factory:building` on it: when the owner removes `factory:blocked`, rule 2 resumes it.

4. Build the package from the matching exemplar (guidebook §11 archetype index; copy its
   structure, not its provider code). Follow CONTRIBUTING.md §3–4: offline respx tests,
   conformance, error mapping, credential declaration, `catalog_tests/queries.yaml` and
   `scripts/build_catalog.py --save` for a catalog package, one live test per verb named per
   the convention above, `make readme-roster`. Finish the dossier (§0, §3, §6–8).
   Commit and push after every step (dossier, package skeleton, each verb, tests, catalog).

5. Run `factory/check.py` against the working checkout, fix, and repeat — three rounds at
   most:

   ```bash
   python3 factory/check.py <pkg> --root . --base origin/main --out /tmp/factory/check-<pkg>
   ```

   On a pass, push, then remove `factory:building` (the next run reviews it). Still failing
   after three rounds: push, comment the script's summary and why it fails, add
   `factory:blocked`, end.

**Resume (rule 2).** Check out the existing branch (`git checkout -B factory/<pkg> origin/factory/<pkg>`),
read the dossier and the PR comments (an owner answer to a block may be there), work out
which build step is unfinished, and continue from it. Do not start over.

## Fix

For a PR labelled `factory:changes-requested`:

1. Add `factory:building`. Check out the branch (`git checkout -B factory/<pkg> origin/factory/<pkg>`).
2. Read the latest review comment. Fix each numbered finding; commit and push after each.
3. Run `factory/check.py` (same command as Build step 5, three rounds at most).
4. Comment one line per finding number saying what changed, plus the script's summary.
5. Remove `factory:building` and `factory:changes-requested`. The next run reviews it again.
   If the script still fails after three rounds, comment why and add `factory:blocked`
   instead of removing `factory:building`.

## Review

For a PR with no factory label. Never push to the branch, never edit its files.
Keep notes under `/tmp/factory/review-<n>/`, not in the repo.

1. **Before checking out the PR**, from the issue text and the source's own site and API only:
   - Count the source's universe (series, datasets, tables — whatever its addressable unit is)
     and write down how you counted.
   - Write five questions an analyst would ask of this source, each with the series codes
     that answer it. Save both to `/tmp/factory/review-<n>/expected.md`.

2. Check out the PR in a separate folder next to this checkout (so `../parsimony` still
   resolves) and run main's `factory/check.py` on it:

   ```bash
   git fetch origin factory/<pkg>
   ```

   ```bash
   git worktree add --detach ../review-<n> origin/factory/<pkg>
   ```

   ```bash
   python3 factory/check.py <pkg> --root ../review-<n> --base origin/main --out /tmp/factory/review-<n>/check
   ```

3. Call every verb live with non-default parameters, using the PR checkout's environment
   (`uv run --project ../review-<n> python ...`), and check the results carry real values.
   Then start a subagent that gets only the five questions, the path to the installed
   package's Python (`../review-<n>/.venv/bin/python`) and the parsimony skill
   (`../parsimony/skills/parsimony/SKILL.md`). Tell it not to read the dossier, the package
   source or the PR, to discover and fetch through the connectors only, and to answer each
   question with the series codes it used. Compare its answers with the expected codes.

4. Compare dossier §1.3 (endpoints), §1.4 (universe, `Expected count:`) and §5 (verbs,
   exclusions) with the code and with your own count from step 1.

5. Post one comment: the script's summary lines verbatim, then numbered findings (each one
   concrete: what is wrong, where, what would fix it), then the subagent's score (n/5).
   Approve only when the script passes, every verb returned real data, the subagent's answers
   match the expected codes, and the counts agree or the difference is explained. Otherwise
   request changes.
   - Approve: add `factory:approved`.
   - Changes: count earlier `factory:changes-requested` rounds (events call above), add
     `factory:changes-requested`, and if this is the third, also add `factory:blocked`.

   Remove the worktree (`git worktree remove --force ../review-<n>`), end.
