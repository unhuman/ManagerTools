# Design: Incomplete Cache Integrity

## Problem

When Jira or GitHub requests fail mid-sprint (DNS errors, connection drops, 403/404 responses),
the tool has two failure modes, both bad:

- **Connection errors**: `get_issue_category_information()` raises `RuntimeError`, the sprint
  crashes entirely, and nothing is cached. The entire sprint must be reprocessed from scratch on
  the next run — including all issues that had already succeeded.

- **403/404 errors**: The failed issues are silently dropped (no entry in `processing_errors`),
  and the cache is written with those issues absent. The incomplete data looks complete, and the
  missing issues are never retried.

Both cases lead to either wasted reprocessing time or silently corrupt cached data.

## Solution Overview

**Always cache successfully-fetched data.** When a sprint's processing encounters failures:

1. Write the partial data (successful issues only) to the cache as normal.
2. Mark the cache as `"complete": false` and record the specific ticket keys that failed in
   `"failed_issues"`.
3. At end of run, print a summary of which sprints are incomplete.
4. On the next run, detect the incomplete cache, load the partial data back into the database,
   and re-fetch **only the previously-failed issues** — not the entire sprint.
5. If all previously-failed issues succeed, overwrite the cache as `"complete": true`.
6. If some still fail, update the `"failed_issues"` list and keep iterating.

The sprint converges toward complete incrementally. No `--force-refresh` flag is needed.

## Cache File Format

### Complete cache (existing behavior, now with explicit `complete` field)

```json
{
  "version": "1.0",
  "complete": true,
  "failed_issues": [],
  "teamName": "MyTeam",
  "sprintName": "MyTeam Sprint 42",
  "startDate": "2025-01-06",
  "endDate": "2025-01-19",
  "timestamp": 1736000000000,
  "data": {
    "rows": [ ... ]
  }
}
```

### Incomplete cache (new)

```json
{
  "version": "1.0",
  "complete": false,
  "failed_issues": ["TICKET-101", "TICKET-207", "TICKET-334"],
  "teamName": "MyTeam",
  "sprintName": "MyTeam Sprint 42",
  "startDate": "2025-01-06",
  "endDate": "2025-01-19",
  "timestamp": 1736000000000,
  "data": {
    "rows": [ ...rows for the 97 issues that succeeded... ]
  }
}
```

### Backward compatibility

Old cache files without a `complete` field are treated as **complete**. No migration needed.

## Processing Logic

### Three states in `process_potentially_cached_sprint_data()` / `process_kanban_cycle()`

```
has_cached_data?
  └─ NO  → process all issues
           → save_to_cache(is_complete, failed_issues)

  └─ YES, is_cache_complete?
           └─ YES → load into DB, done (fast path — no network calls)

           └─ NO  → load partial rows into DB
                    filter sprint's issue list to only prev failed_issues
                    process only those N issues (network calls for N, not all)
                    save_to_cache(new_is_complete, new_failed_issues)
```

## Failure Classification

Both **connection errors** (DNS failure, timeout, disconnect) and **403/404 HTTP errors** are
treated as failures that add the ticket key to `failed_issues`.

- **Connection errors** are transient (VPN dropped, network blip) — retrying next run is correct.
- **403/404 errors** may be permanent (restricted tickets) or transient (expired auth token).
  Both are tracked the same way for simplicity. A sprint with a permanently 403'd ticket will
  remain `"complete": false` indefinitely; future work could add a "give up after N runs" policy
  if this becomes a real operational issue.

## Files Changed

| File | Changes |
|---|---|
| `python/managertools/util/sprint_data_cache.py` | `save_to_cache()` adds `is_complete` + `failed_issues` params; new `is_cache_complete()` method; `load_cached_data()` returns `(data, failed_issues)` tuple |
| `python/managertools/sprint_report_team_analysis.py` | `process_issue()` records failed ticket keys; `get_issue_category_information()` returns `(bool, List[str])` instead of raising; `process_potentially_cached_sprint_data()` and `process_kanban_cycle()` implement three-state logic; `aggregate_data()` collects incomplete sprint names and prints end-of-run summary |

## End-of-Run Summary

When incomplete sprints exist after a run completes:

```
*** WARNING: The following sprints/cycles have incomplete cached data ***
   - MyTeam Sprint 42
   - MyTeam Sprint 43
Re-run the same command to retry fetching the missing issues.
Only the previously-failed issues will be re-fetched.
```

## What Does NOT Change

- Complete cache files are loaded exactly as before — no extra Jira calls, no slowdown.
- The cache key format is unchanged.
- The `CACHE_VERSION` field is unchanged (old caches remain valid).
- The `--force-refresh` flag is not needed and not added; incomplete caches self-trigger re-fetch.
