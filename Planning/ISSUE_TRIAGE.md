# Issue triage and prioritization (short-term process)

This document defines how we handle GitHub Issues for **rust-data-processing** before adopting heavier automation (Projects rules, custom bots, etc.). It complements the bug templates under [`.github/ISSUE_TEMPLATE/`](../.github/ISSUE_TEMPLATE/) and [`SECURITY.md`](../SECURITY.md).

## Where to report

- **Bugs and features:** [GitHub Issues](https://github.com/vihangdesai2018-png/rust-data-processing/issues) using the **Bug Report** or **Feature Request** form when possible.
- **Security vulnerabilities:** follow [`SECURITY.md`](../SECURITY.md); do **not** use public issues.

## Labels (convention)

| Label | Meaning |
| --- | --- |
| `bug` | Something is broken or behavior contradicts documented intent. |
| `enhancement` | Feature or improvement (from template or maintainer). |
| `needs-triage` | Not yet reviewed by a maintainer (optional; add when unclear). |
| `confirmed` | Reproduced or accepted as a real defect / valid request. |
| `help wanted` | Suitable for external contributors (scope and expectations are clear). |

Maintainers may add **priority** labels (`priority:high`, etc.) if the team adopts them; until then use milestones and release notes.

## Cadence

- **Weekly (or each release prep):** spend ~30 minutes on triage.
  1. New issues without a response: skim, ask for missing template fields, add `needs-triage` if blocking.
  2. **`confirmed` bugs:** assign a **milestone** (target release) when a fix is planned, or leave unassigned until scheduled.
  3. **Hotlist:** run the [popular bugs query](#repeatable-popular-bugs-query) and ensure top items are either prioritized or explicitly deferred (comment with rationale).

## “Top” and popular issues (signal, not automatic priority)

We **do not** auto-set priority from 👍 counts alone. We use user demand as **one input** next to severity, data loss risk, and maintenance cost.

### Repeatable “popular bugs” query

Open this search in the repository (adjust `repo:` if the repo is forked or renamed):

```text
is:issue is:open label:bug sort:reactions-+1-desc
```

Optional narrow filters:

```text
is:issue is:open label:bug label:confirmed sort:reactions-+1-desc
```

Bookmark the search URL; it is the short-term substitute for a script.

### Comments-only ranking (alternative)

```text
is:issue is:open label:bug sort:comments-desc
```

Use for threads with heavy discussion; treat noise (off-topic) case-by-case.

### From signals to work

1. Pick the top **5–10** open bugs by 👍 (and any **severity-1** issues regardless of 👍).
2. For each: either milestone + assignee, or a short comment (“deferred: reason”).
3. When work starts, link **PRs** with `Fixes #123` so releases and release notes stay traceable.

## Release linkage

- **Milestones** name target versions (e.g. `v0.2.0`) where possible.
- **CHANGELOG** and GitHub Releases should reference fixed issues so users can see “which release contains the fix.”

## Future extensions (not required to start)

- GitHub **Projects** with Priority and Status fields, plus light automation (label → column).
- Scheduled Action or script: GraphQL to list `label:bug` sorted by reaction count and post a weekly digest comment (optional).

## Related documentation

- [Documentation and published docs](DOCUMENTATION.md) — where API docs live and how they are built in CI.
- Root [README.md](../README.md) — “Reporting bugs” for contributors and end users.
