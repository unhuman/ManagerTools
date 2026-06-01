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

#### Code Metrics Filtering

- **`maxCommitSize`** — Exclude commits larger than this line threshold (additions + removals) from metrics. Defaults to 2000 if not specified. Helps filter out large auto-generated changes and downmerges.
  ```json
  "maxCommitSize": 2000
  ```

- **`maxFileChangeSize`** — Exclude individual file changes larger than this line threshold from metrics.
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

#### GitHub API Rate Limiting

- **`graphqlPointsReserved`** — Number of GraphQL rate-limit points to keep in reserve. When the remaining point count drops to or below this value, processing pauses (with a countdown display) until the rate-limit window resets. Increase this value when running multiple simultaneous processes that share the same GitHub token. Default: `5`.
  ```json
  "graphqlPointsReserved": 100
  ```

#### Precedence

When the same setting is provided in both the configuration file and CLI flags:
- CLI flags take highest priority (e.g., `--maxCommitSize N`)
- Configuration file values are used if no CLI flag is provided
- Built-in defaults are used if neither is specified

#### Example Configuration File

```json
{
  "jiraServer": "https://jira.company.com",
  "bitbucketServer": "https://bitbucket.company.com",
  "maxCommitSize": 2000,
  "maxFileChangeSize": 5000,
  "graphqlPointsReserved": 5,
  "ignoreFilenames": ["*.min.js", "*.bundle.js"],
  "ignorePRTitleContent": ["(?i)automated", "(?i)downmerge"],
  "ignoreCommitMessageContent": ["(?i)bump version"]
}
```

### Developer Notes
- Data is stored in `~/.managerTools.cfg`
- There was difficulty getting long input from the terminal, so for that input, `zsh/vared` is used to accept input
