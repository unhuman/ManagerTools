package com.unhuman.managertools

import com.unhuman.flexidb.FlexiDBQueryColumn
import com.unhuman.flexidb.data.FlexiDBRow
import com.unhuman.managertools.data.DBIndexData
import com.unhuman.managertools.util.CommandLineHelper

class SprintReportIndividualAnalysis extends SprintReportTeamAnalysis {
    List<String> teamUsers

    @Override
    protected void setupRun() {
        super.setupRun()
        // Load the command line helper here for ability to manage users / team
        CommandLineHelper commandLineHelper = new CommandLineHelper(".managerTools.cfg")
        teamUsers = commandLineHelper.getBoardTeamUsers(commandLineOptions.'boardId')
        if (teamUsers.get(0) == "*") {
            teamUsers = Collections.emptyList()
        }
    }

    @Override
    protected void generateOutput() {
        // Determine the list of columns to report
        List<String> columnOrder = generateColumnsOrder()

        List<String> sprints = database.findUniqueValues(DBIndexData.SPRINT.name())
        LinkedHashSet<String> users = new LinkedHashSet<>(database.findUniqueValues(DBIndexData.USER.name()))

        // Try to get values out of the database for user and case-insensitively up-convert the matches,
        // otherwise preserve unknown values (they won't matter)
        List<String> specifiedUsers = (teamUsers.size() > 0) ? teamUsers : null
        if (specifiedUsers != null) {
            users = specifiedUsers.stream().map { specifiedUser -> {
                List<String> matched = users.stream().findAll {it::equalsIgnoreCase(specifiedUser) }
                specifiedUser = (matched.size() > 0) ? matched.get(0) : specifiedUser
            }}
        }

        // 1. Iterate through all users
        // 1.    Iterate through sprints
        // 2.       Report all rows for that user
        // 3.       Sum up those rows
        // 4.       Report totals
        //
        // 5. Report total results of all totals

        StringBuilder sb = new StringBuilder(4096)
        users.each {user -> {
            sb.setLength(0)
            sb.append(FlexiDBRow.headingsToCSV(columnOrder))
            sb.append('\n')

            FlexiDBRow overallTotalsRow = new FlexiDBRow(columnOrder.size())
            sprints.each { sprint -> {
                List<FlexiDBQueryColumn> userSprintFinder = new ArrayList<>()
                userSprintFinder.add(new FlexiDBQueryColumn(DBIndexData.SPRINT.name(), sprint))
                userSprintFinder.add(new FlexiDBQueryColumn(DBIndexData.USER.name(), user))

                findRowsAndAppendCSVData(userSprintFinder, sb, overallTotalsRow)
            }}

            // Summary
            appendSummary(sb, overallTotalsRow)

            // Write to file
            String filename = getCommandLineOptions().'outputCSV'.replace(".csv", "-${commandLineOptions.'boardId'}-${user}.csv")
            writeResultsFile(filename, sb)
        }}
    }
}