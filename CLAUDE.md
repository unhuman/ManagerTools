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
- Do NOT automatically push commits - let the user decide when to push
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

_Add your project-specific conventions here_
