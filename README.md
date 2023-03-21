# Manager Tools

## Features
- ### GetTeamSprints
  - Report of Sprints for a specified Team
- ### SprintReportTeamAnalysis
  - Report of Sprint Team members over specified Sprints
- ### SprintReportTeamCarryoverHours
  - Report Carrover Hours from Sprints of a specified Team 

## Architecture
- AbstractSprintReport - Base class for many reports based on Team Sprints
- CommandLineHelper - Tracks configuration data for Reports 
- RestService - Utility class to make REST requests
- BitbucketREST - Rest interface for Bitbucket (pretty obvious)
- JiraREST - Rest interface for Jira (pretty obvious)

## Setup
### IntelliJ must add Ivy
1. Project Structure / Modules.
1. Get Apache Ivy with dependencies. 
1. Add both the Ivy binary and all the dependencies of Ivy (lib subfolder).

## Notes
- data is stored in ~/.managerTools.cfg