# Benchmarks

Every pull request runs the **Benchmark comparison** check on Ubuntu 24.04 and
Python 3.11, comparing the candidate merge commit with the PR's exact base commit.
The two checkouts have separate environments, so dependency changes are included.
The same candidate benchmark harness measures both versions on the same runner.

The check measures end-to-end CLI time (including imports, worker startup, hashing,
manifest export, and shutdown) for small files, large files, verbose output,
sequence aggregates, fresh caches, and populated caches. Each workload runs with
one and four workers. Cache scenarios include verbose output to verify cache hits.
Every run validates MD5 manifests; sequence aggregates, verbose record counts,
and cache contents/hits receive additional checks. Incorrect output fails the job.

Fixtures are deterministic and shared between checkouts. A warmup precedes six
paired samples per workload, alternating which implementation runs first. Results
include medians and raw timings in a JSON artifact and a Markdown job summary.
The smoke dataset has 256 files of 4 KiB and four files of 8 MiB.
Cache workloads use a separate 1,024-file dataset to reduce startup overhead relative
to measured cache work. `--full` uses
4,096 files of 4 KiB and eight files of 64 MiB.

These are **warm filesystem-cache benchmarks**, not measurements of cold disk
throughput. Fresh-cache tests refer to SQLite cache misses, not OS cache misses.
Hosted runner noise and the fixed worker shutdown timeout can obscure small
changes. Initially the check fails only when a workload's median slows by both
**more than 20% and more than 0.1 seconds**, using median paired deltas and requiring
both thresholds to be exceeded in at least 75% of sample pairs. This is a
repeatability heuristic, not a confidence interval. The table shows medians of
individual runtimes but computes percent changes from the paired ratios, so those
columns need not agree exactly.

Each report includes median absolute deviation (MAD) as a percentage of the median
for each implementation, plus the count of slow pairs. A slowdown without enough
consistent pairs, or dispersion above 10%, is marked **inconclusive**, not a confirmed
regression. Inconclusive results do not fail the check: reviewers should inspect
and rerun them or use a larger workload before approving a performance-sensitive PR.
Do not treat a green check with inconclusive rows as proof of unchanged performance.
Calibrate these provisional thresholds from repeated CI results.

## Local use

With the dependencies installed for both checkouts:

```bash
python scripts/benchmark.py --baseline ../hashio-base --json benchmark.json
```

Use `--baseline-python /path/to/base-venv/bin/python` and
`--candidate-python /path/to/candidate-venv/bin/python` for separate environments.
Other options include `--full`, `--iterations 8`, and `--procs 1 4 10`.
Iteration counts must be even and at least four. Use `--scenarios cache-cold`
for a focused cache-miss investigation.
Outputs and databases live in a temporary directory and are cleaned up afterward.
The JSON report records commits, interpreter versions, workload sizes and timings;
local uncommitted changes are included in measurements but not in the commit IDs.

## Required pull request check

After this workflow is merged and has run, add **Benchmark comparison** to the
required status checks in the ruleset or branch protection for `master`.
The workflow has no path filters, so documentation-only PRs also produce the check.
Fork PRs use the normal read-only `pull_request` workflow; maintainer approval may
be needed before GitHub starts a first-time contributor's jobs. Manual runs compare
the selected branch against master.

## Pull request comments

A separate **Publish benchmark comparison** workflow runs after benchmarks finish
and updates one bot comment on the associated PR, including fork PRs. It links the
run and shows the comparison table, dispersion, slow-pair counts, and status. When
benchmarks fail before writing a valid report, the comment links the failed run and
states that results are unavailable. Results for superseded PR commits are skipped.

The publisher keeps its reporting logic inline, like pyseq, in a separate
`workflow_run` workflow so it can also comment on approved fork runs. It never
executes or checks out PR code. It reads only a size-limited JSON artifact member,
validates fixed workload labels and numeric fields, and builds the Markdown itself.
Only this reporting workflow gets permission to write PR comments. No custom token
is needed. Both workflows must be merged into the default
branch before automatic comments can work; this cannot be end-to-end tested locally.
Local reporter tests use mocked GitHub API calls and do not post comments.
