# Project Instructions for AI Agents

This file provides instructions and context for AI coding agents working on this project.

<!-- BEGIN BEADS INTEGRATION v:1 profile:minimal hash:ca08a54f -->
## Beads Issue Tracker

This project uses **bd (beads)** for issue tracking. Run `bd prime` to see full workflow context and commands.

### Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work
bd close <id>         # Complete work
```

### Rules

- Use `bd` for ALL task tracking — do NOT use TodoWrite, TaskCreate, or markdown TODO lists
- Run `bd prime` for detailed command reference and session close protocol
- Use `bd remember` for persistent knowledge — do NOT use MEMORY.md files

## Session Completion

**When ending a work session**, complete these steps:

**WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **Commit changes** - Stage and commit code changes to git:
   ```bash
   git add <files>
   git commit -m "..."
   ```
5. **Push when ready** - User should push to remote when they're ready:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   ```

**RULES:**
- **Do NOT push commits** — Stage and commit only. The developer decides when to push.
- Always commit code changes so they're not lost
- Provide clear commit messages
- Clean up stashes and branches as needed
<!-- END BEADS INTEGRATION -->


## Running Tools

### Sprint Report Tools

All sprint analysis tools are Python modules that can be run with `python -m`:

#### Team Analysis (single CSV per team)
```bash
python -m managertools.sprint_report_team_analysis \
  -t "Team Name" \
  -l 5 \
  -o team-report.csv
```

#### Individual Analysis (one CSV per user)
```bash
python -m managertools.sprint_report_individual_analysis \
  -t "Team Name" \
  -l 5 \
  -o individual-report.csv
```

**Common options:**
- `-t, --teamName` — Team name
- `-b, --boardId` — Jira board ID (alternative to team name)
- `-l, --limit` — Number of recent sprints to process
- `-s, --sprintIds` — Specific sprint IDs (comma-separated)
- `-o, --outputCSV` — Output filename (required)
- `-q, --quietMode` — Use defaults without prompting

**Optional:**
- `-p, --prompt` — Prompt for interactive input
- `--includeMergeCommits` — Include merge commits in metrics
- `--includeDownMergePRs` — Process down-merge PRs instead of skipping them at collection time
- `--maxCommitSize N` — Limit commit size for metrics
- `-m, --multithread N` — Number of threads (* for CPU count)

## Build & Test

_Add your build and test commands here_

```bash
# Example:
# npm install
# npm test
```

## Architecture Overview

_Add a brief overview of your project architecture_

## Conventions & Patterns

### Documentation

**Whenever a configuration value is added, removed, or changed**, update the `### Configuration` section of `README.md` to reflect:
- The key name (camelCase, matching `~/.managerTools.cfg`)
- What it controls
- Its default value
- A one-line JSON example

Also update the **Example Configuration File** block in that same section.

This applies to values loaded via `ConfigFileManager.get_value(...)` anywhere in the codebase.

**Important:** `ConfigFileManager.get_value()` raises `RuntimeError` for missing keys — always guard optional config values with `contains_key()` before calling `get_value()`:
```python
config_mgr = self.command_line_helper.get_config_file_manager()
value = int(config_mgr.get_value("myKey")) if config_mgr.contains_key("myKey") else default_value
```

### Cache Versioning

`SprintDataCache.CACHE_VERSION` is defined in `python/managertools/util/sprint_data_cache.py`.

**Bump the version whenever a change causes existing cache files to produce incomplete or incorrect data**, including:
- Adding new issue categories to sprint processing (e.g., `puntedIssues`)
- Changing what fields are fetched or stored per issue/PR
- Fixing bugs where previously cached data is known to be wrong

Bumping the version causes old cache files to be ignored and re-fetched on the next run. Use semantic versioning (`1.0` → `1.1` for additive/fix changes, `2.0` for structural changes).

**After bumping the cache version, offer to delete the stale cache files.** Old files are harmless but waste disk space and can cause confusion. The cache directory is `python/cacheData/` (or `cacheData/` relative to the working directory). To remove only the outdated versioned files:
```bash
# Preview what would be deleted
grep -rl '"version": "1.0"' python/cacheData/ --include="*.json"

# Delete them
grep -rl '"version": "1.0"' python/cacheData/ --include="*.json" | xargs rm
```
Replace `1.0` with the version being superseded.
