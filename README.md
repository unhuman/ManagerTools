# Manager Tools

## Features
- ### GetTeamSprints
  - Report of Sprints for a specified Team
- ### SprintReportTeamAnalysis
  - Report of Sprint Team members (combined) over specified Sprints
  ### SprintReportIndividualAnalysis
  - Reports of Individual Team members over specified sprints 
- ### SprintReportTeamCarryoverHours
  - Report Carrover Hours from Sprints of a specified Team 

## Scripts
- `JiraCopyTicketEstimates` - Copies time estimates from tickets matching criteria
- `SprintReportIndividualAnalysis` - Generates reports of individual users' metrics
- `SprintReportTeamAnalysis` - Generates report of team metrics
- `SprintReportTeamCarryoverHours` - Generates report of team's carryover

## Execution
- From the `src/` directory:
  - `groovy com/unhuman/managertools/SCRIPT.groovy parameters...`

## Developer Setup
### IntelliJ must add Ivy
1. Project Structure / Modules.
1. Get Apache Ivy with dependencies. 
1. Add both the Ivy binary and all the dependencies of Ivy (lib subfolder).
1. Note the issues with unit tests and Grape Grab, below

## Tests
1. IntelliJ has problems: `No suitable ClassLoader found for grab`
1. From the `src/` directory
1. Run: `groovy -Dgroovy.grape.report.downloads=true ../tests/com/unhuman/flexidb/FlexiDBTests.groovy`

## Notes
- data is stored in ~/.managerTools.cfg
- there was difficulty getting long input, so for that input, zsh/vared is used to accept input