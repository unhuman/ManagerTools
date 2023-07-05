# Manager Tools

## Features
- ### GetTeamSprints
  - Report of Sprints for a specified Team
- ### SprintReportTeamAnalysis
  - Report of Sprint Team members (combined) over specified Sprints
  ### SprintReportIndividualAnalysis
  - Reports of Individual Team members over specified sprints 
- ### SprintReportTeamCarryoverHours
  - Report Carryover Hours from Sprints of a specified Team 

## Scripts
- `JiraCopyTicketEstimates` - Copies time estimates from tickets matching criteria
- `SprintReportIndividualAnalysis` - Generates reports of individual users' metrics
- `SprintReportTeamAnalysis` - Generates report of team metrics
- `SprintReportTeamCarryoverHours` - Generates report of team's carryover

## Report Notes
- PR_ADDED, PR_REMOVED, and PR_MODIFIED values are always attributed to the author, regardless of the committer.
- PR_ADDED, PR_REMOVED, and PR_MODIFIED values are not time-checked against the sprints, so those values will duplicate for carryover tickets.
- Nested comments could be mis-attributed to the wrong sprint.  Nested comments are attributed to the same sprint as the initial comment. 

## Execution
- From the `src/` directory:
  - `groovy com/unhuman/managertools/SCRIPT.groovy parameters...`
- Cookie values are required to have these scripts function.  Copy Value them from your browsers Development Tools / Request Headers / Cookie
- https://github.com/unhuman/browser-cookies-copier makes it easy to copy the cookies needed by this tool
- GitHub integration requires a Personal Access Token.  To create one, go to Account / Settings / Developer Settings.
  - Permissions (Classic): repo (all), read:user, read:discussion
  - Configure SSO: Authorize your organization

## Developer Setup
### IntelliJ must add Ivy
1. Project Structure / Modules
1. Get Apache Ivy with dependencies 
1. Add both the Ivy binary and all the dependencies of Ivy (lib subfolder)
1. Note the issues with unit tests and Grape Grab, below

## Tests
1. IntelliJ has problems: `No suitable ClassLoader found for grab`
1. From the `src/` directory
1. Run: `groovy -Dgroovy.grape.report.downloads=true ../tests/com/unhuman/flexidb/FlexiDBTests.groovy`

## Developer Notes
- Data is stored in `~/.managerTools.cfg`
- There was difficulty getting long input from the terminal, so for that input, `zsh/vared` is used to accept input
