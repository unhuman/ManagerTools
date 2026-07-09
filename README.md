# Manager Tools

## Implementations
- **Python** (current) — `python/`
- **Groovy** (deprecated) — `groovy/`

## Python

### Features
- **GetTeamSprints** - Report of Sprints for a specified Team
- **SprintReportTeamAnalysis** - Report of Sprint Team members (combined) over specified Sprints
- **SprintReportIndividualAnalysis** - Reports of Individual Team members over specified sprints
- **SprintReportTeamCarryoverHours** - Report Carryover Hours from Sprints of a specified Team

### Scripts
- `GetTeamSprints.py` - Generates sprint reports for a team
- `SprintReportIndividualAnalysis.py` - Generates reports of individual users' metrics
- `SprintReportTeamAnalysis.py` - Generates report of team metrics
- `SprintReportTeamCarryoverHours.py` - Generates report of team's carryover hours

### Execution
From the `python/` directory:
```bash
python -m managertools.SCRIPT [parameters...]
```

Cookie values are required to have these scripts function. Copy them from your browser's Development Tools / Request Headers / Cookie.
- https://github.com/unhuman/browser-cookies-copier makes it easy to copy the cookies needed by this tool

GitHub integration requires a Personal Access Token. To create one, go to Account / Settings / Developer Settings.
- Permissions (Classic): repo (all), read:user, read:discussion
- Configure SSO: Authorize your organization

### Report Notes
- PR_ADDED, PR_REMOVED, and PR_MODIFIED values are always attributed to the author, regardless of the committer.
- PR_ADDED, PR_REMOVED, and PR_MODIFIED values are not time-checked against the sprints, so those values will duplicate for carryover tickets.
- Nested comments could be mis-attributed to the wrong sprint. Nested comments are attributed to the same sprint as the initial comment.
- Supports Kanban. All activity on a ticket (regardless of time) is contained within the time period that the ticket is resolved.
- **SCRUM sprint windows are back-filled to close inter-sprint gaps.** Jira's sprint start/complete timestamps don't abut — there are typically ~1-day gaps between one sprint's end and the next sprint's start, and commits/activities landing in a gap would otherwise fall outside every window and be dropped. Each sprint's window is therefore `[predecessor_sprint_end, this_sprint_end)` rather than `[this_sprint_start, this_sprint_end)`. The predecessor is taken from the full board-wide sprint sequence, so a given sprint's window is identical regardless of how many sprints a run requested (`-l N`) — deterministic and cache-safe. The earliest reported sprint uses the real prior sprint (even one outside the reported range) for its boundary; when no prior sprint exists at all (the board's first sprint, `-s` with no board, or Kanban) the window falls back to **00:00 of the sprint's start day**. Run with `--debug` to see each back-fill logged (`sprint window back-fill: '<sprint>' start <- predecessor '<sprint>' end <ts> [closing <gap>]`).
- PR_ADDED/PR_REMOVED are capped at the same row's COMMIT_ADDED/COMMIT_REMOVED, so a developer is never credited more PR lines than authored-commit lines. This prevents outsized credit from the whole-PR net diff (which is author-attributed and not sprint-scoped) — e.g. work merged in from another branch.
- COMMITS counts only the individual's own commits whose lines are counted — it excludes merge commits, brought-in (merged-from-another-branch) commits, and commits at/above `maxCommitSize`.
- **Any value capped/reduced by the above is flagged with a trailing `*`** (e.g. `120*`) on PR_ADDED, PR_REMOVED, and COMMITS. The Sprint Totals / Overall Totals rows are likewise flagged with `*` on any column whose components were capped. The visualizer strips the `*` for charting and instead marks the affected sprint with a `*` plus a footnote.
- **Work source** (`--workSource` / `workSource` config) selects where work is sourced: `pr` (default; pull requests only), `commit` (commits from the Jira dev-status Commits view, no PR processing), or `both` (PRs plus any commits not already counted by a PR, de-duped by commit SHA). In `commit`/`both` mode, commits with no associated PR are grouped per ticket under a synthetic PR id of `(commits)` and are **excluded from TOTAL_PRS / NON_DECLINED_PRS** (they aren't PRs). Their line counts come from per-commit diff fetches, so `both` is slower than `pr` but corrects undercounting when work lands outside (or unlinked from) PRs.
- **How PRs and commits are reconciled in `both` mode.** The full commit set from each source is always collected and cached (de-dupe is *not* done at collection time, which keeps cache files mode-independent). Reconciliation happens at render time and is **per-ticket, SHA-keyed, PR-wins**: for each ticket the SHAs already counted by its PR commits are recorded, and any commit-view entry with a matching SHA is skipped (counted once, via the PR — not flagged as capped/excluded). Note the commit-view path attributes by the git author *display name* mapped to an SCM login (PRs use the login directly), and never assigns the `brought-in` classification (that needs a PR parent walk the loose commits lack).
- **The cache is additive across work sources.** Each sprint's cache file records which sources it contains; switching `--workSource` fetches only the source that's missing and merges it into the same file (e.g. a `pr` run then a `commit` run fetches only commits, leaving the cached PR data untouched; a later `both` run is a pure cache hit). The active mode determines what is rendered, so cached data for a source you aren't currently viewing is preserved, not shown.

## Groovy (Deprecated)

> ⚠️ **The Groovy implementation is deprecated.** Use the Python implementation for new work.

### Features
- **GetTeamSprints** - Report of Sprints for a specified Team
- **SprintReportTeamAnalysis** - Report of Sprint Team members (combined) over specified Sprints
- **SprintReportIndividualAnalysis** - Reports of Individual Team members over specified sprints
- **SprintReportTeamCarryoverHours** - Report Carryover Hours from Sprints of a specified Team

### Scripts
- `JiraCopyTicketEstimates` - Copies time estimates from tickets matching criteria
- `SprintReportIndividualAnalysis` - Generates reports of individual users' metrics
- `SprintReportTeamAnalysis` - Generates report of team metrics
- `SprintReportTeamCarryoverHours` - Generates report of team's carryover

### Execution
From the `groovy/src/` directory:
```bash
groovy com/unhuman/managertools/SCRIPT.groovy [parameters...]
```

Cookie values are required to have these scripts function. Copy them from your browser's Development Tools / Request Headers / Cookie.
- https://github.com/unhuman/browser-cookies-copier makes it easy to copy the cookies needed by this tool

GitHub integration requires a Personal Access Token. To create one, go to Account / Settings / Developer Settings.
- Permissions (Classic): repo (all), read:user, read:discussion
- Configure SSO: Authorize your organization

### Report Notes
- PR_ADDED, PR_REMOVED, and PR_MODIFIED values are always attributed to the author, regardless of the committer.
- PR_ADDED, PR_REMOVED, and PR_MODIFIED values are not time-checked against the sprints, so those values will duplicate for carryover tickets.
- Nested comments could be mis-attributed to the wrong sprint. Nested comments are attributed to the same sprint as the initial comment.
- Supports Kanban. All activity on a ticket (regardless of time) is contained within the time period that the ticket is resolved.

### Developer Setup
#### IntelliJ must add Ivy
1. Project Structure / Modules
2. Get Apache Ivy with dependencies
3. Add both the Ivy binary and all the dependencies of Ivy (groovy/lib subfolder)
4. Note the issues with unit tests and Grape Grab, below

### Tests
1. IntelliJ has problems: `No suitable ClassLoader found for grab`
   Adding `-Dgroovy.grape.enable=false` to the test configuration may help.
2. Command Line, from the `groovy/src/` directory:
   ```bash
   groovy -Dgroovy.grape.report.downloads=true ../tests/com/unhuman/flexidb/FlexiDBTests.groovy
   ```

### Configuration

Configuration is stored in `~/.managerTools.cfg` as a JSON file. The following options are supported:

#### Work Source

- **`workSource`** — Where sprint work is sourced. One of `"pr"` (default; pull requests via Jira dev-status), `"commit"` (commits via the Jira dev-status Commits view; no PR processing), or `"both"` (PRs plus any commits a PR didn't already count, de-duped by commit SHA). Overridden by the `--workSource {pr,commit,both}` CLI flag. Commits with no PR are grouped per ticket under a synthetic `(commits)` PR id and excluded from `TOTAL_PRS`/`NON_DECLINED_PRS`; their line counts come from per-commit diff fetches.
  ```json
  "workSource": "both"
  ```

#### Code Metrics Filtering

- **`maxCommitSize`** — Exclude individual commits larger than this line threshold (additions + removals) from metrics. Defaults to 2000 if not specified. Helps filter out large auto-generated changes and downmerges. Applied per commit at report-generation time against the per-commit data stored in the cache: each commit whose own size meets or exceeds this threshold is dropped from the `COMMIT_ADDED`/`COMMIT_REMOVED` line counts, while smaller commits on the same PR still contribute. The row still appears in reports showing `OPENED`/`MERGED`/review activity. Because filtering happens at report time, changing this value re-shapes the totals without needing to rebuild the cache.
  ```json
  "maxCommitSize": 2000
  ```

- **`maxFileChangeSize`** — ⚠️ Documented for future use; not yet implemented. Intended to exclude individual file changes larger than this line threshold from metrics.
  ```json
  "maxFileChangeSize": 5000
  ```

#### Content-Based Filtering

- **`ignoreFilenames`** — Array of glob patterns to exclude files from metrics. Useful for excluding generated code or configuration files.
  ```json
  "ignoreFilenames": ["*.generated.js", "**/dist/**"]
  ```

- **`ignorePRTitleContent`** — Array of regex patterns. If a PR title matches any pattern, the entire PR and all its commits are excluded from metrics.
  ```json
  "ignorePRTitleContent": ["(?i)automated|downmerge", "(?i)^WIP"]
  ```

- **`ignoreCommitMessageContent`** — Array of regex patterns. If a commit message matches any pattern, that individual commit is excluded from metrics.
  ```json
  "ignoreCommitMessageContent": ["(?i)merge|revert", "^bump version"]
  ```

- **`downMergePRTitlePatterns`** — Array of regex patterns matched (substring `re.search`) against the PR title to detect down-merge / bulk-merge PRs and **skip them at collection time** (before the expensive commit fetch). Unlike `ignorePRTitleContent` (which excludes a PR at report time but still fetches it), a matched PR is never fetched and instead gets a visible `[skipped]` marker row. The title is available pre-fetch (from Jira dev-status), so this rule costs no extra API calls. Default (used when the key is absent): `["(?i).*down\\s*merge.*"]` — matches `downmerge`/`down merge` but not bare "merge". Set to `[]` to disable the title rule.
  ```json
  "downMergePRTitlePatterns": ["(?i).*down\\s*merge.*"]
  ```

- **`downMergeTrunkBranches`** — Array of regex patterns (full-match) for "trunk" branch names. A PR whose **source** (`from`) branch matches is treated as a down-merge and **skipped at collection time** with a visible `[skipped]` marker. The source branch comes from the Jira dev-status payload (`source.branch`), so this rule costs no extra API calls and applies on every PR (cache hit or miss). This is the **preferred** rule — it is checked before the title rule. Default (used when the key is absent): `["main", "master", "develop", "release/.*"]` (note: `hotfix/*` is intentionally excluded because real work is authored there). Set to `[]` to disable the branch rule. Both rules can be disabled for a single run with `--includeDownMergePRs`. Run with `DEBUG` to log each PR's source branch and the keep/skip decision, which is the easiest way to tune this list to your repo's branch names.
  ```json
  "downMergeTrunkBranches": ["main", "master", "develop", "release/.*"]
  ```

#### Backstage Catalog Integration

- **`backstageServer`** — Backstage software catalog server FQDN for fetching team member role/title data. Used by the dashboard to enable "Compare by Title" peer analysis across the organization. Example: `"backstage.core.cvent.org"`. No default; if not configured, Backstage integration is skipped gracefully.
  ```json
  "backstageServer": "backstage.core.cvent.org"
  ```

- **`backstageAuth`** — Optional authentication token or cookies for Backstage API access. Can be empty for unauthenticated access. Auto-detects Bearer token (standard format) vs. Cookies (format: `key=value`). If both `backstageServer` and `backstageAuth` are configured, the dashboard will load role data for cross-team peer comparison.
  ```json
  "backstageAuth": ""
  ```

- **`backstageCacheDays`** — TTL in days for cached Backstage roster data. Role data changes infrequently, so caching reduces API calls on dashboard startup. Default: `7`.
  ```json
  "backstageCacheDays": 7
  ```

#### GitHub API Rate Limiting

- **`graphqlPointsReserved`** — Number of GraphQL rate-limit points to keep in reserve. When the remaining point count drops to or below this value, processing pauses (with a countdown display) until the rate-limit window resets. Increase this value when running multiple simultaneous processes that share the same GitHub token. Default: `5`.
  ```json
  "graphqlPointsReserved": 100
  ```

#### Precedence

When the same setting is provided in both the configuration file and CLI flags:
- CLI flags take highest priority (e.g., `--maxCommitSize N`, `--workSource both`)
- Configuration file values are used if no CLI flag is provided
- Built-in defaults are used if neither is specified (e.g., `workSource` defaults to `pr`)

#### Example Configuration File

```json
{
  "jiraServer": "https://jira.company.com",
  "bitbucketServer": "https://bitbucket.company.com",
  "backstageServer": "backstage.core.cvent.org",
  "backstageAuth": "",
  "backstageCacheDays": 7,
  "workSource": "pr",
  "maxCommitSize": 2000,
  "maxFileChangeSize": 5000,
  "graphqlPointsReserved": 5,
  "ignoreFilenames": ["*.min.js", "*.bundle.js"],
  "ignorePRTitleContent": ["(?i)automated", "(?i)downmerge"],
  "ignoreCommitMessageContent": ["(?i)bump version"],
  "downMergePRTitlePatterns": ["(?i).*down\\s*merge.*"],
  "downMergeTrunkBranches": ["main", "master", "develop", "release/.*"]
}
```

### Developer Notes
- Data is stored in `~/.managerTools.cfg`
- There was difficulty getting long input from the terminal, so for that input, `zsh/vared` is used to accept input
