package com.unhuman.managertools

import com.unhuman.flexidb.FlexiDBQueryColumn
import com.unhuman.flexidb.data.FlexiDBRow
import com.unhuman.managertools.data.DBData
import com.unhuman.managertools.data.DBIndexData
import com.unhuman.managertools.data.UserActivity
import com.unhuman.managertools.util.CommandLineHelper

class SprintReportIndividualAnalysis extends SprintReportTeamAnalysis {
    List<String> teamUsers

    @Override
    protected void setupRun() {
        super.setupRun()
        // Load the command line helper here for ability to manage users / team
        CommandLineHelper commandLineHelper = new CommandLineHelper(".managerTools.cfg")
        if (commandLineOptions.q) {
            commandLineHelper.setQuietModeNoPrompts()
        }

        teamUsers = commandLineHelper.getTeamBoardUsers(teamName, boardId)
    }

    @Override
    protected List<String> generateColumnsOrder() {
        List<String> columnOrder = super.generateColumnsOrder()

        int commentedIndex = columnOrder.indexOf(UserActivity.COMMENTED.name())
        columnOrder.add(commentedIndex + 1, UserActivity.OTHERS_COMMENTED.name())

        return columnOrder
    }

    @Override
    protected void generateOutput() {
        // Determine the list of columns to report
        List<String> columnOrder = generateColumnsOrder()

        List<String> sprints = database.findUniqueValues(DBIndexData.SPRINT.name())

        Set<String> authors = new LinkedHashSet<>(database.findUniqueValues(DBData.AUTHOR.name()))
        Set<String> users = new LinkedHashSet<>(database.findUniqueValues(DBIndexData.USER.name()))

        // Adjust the teamUsers list to either (*) authors or (**) all users
        if (teamUsers.size() == 1) {
            if (teamUsers.get(0) == "*") { // authors
                teamUsers = authors.toList()
            } else if (teamUsers.get(0) == "**") { // all users
                teamUsers = users.toList()
            }
        }

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
        // 2.    Iterate through sprints
        // 3.       Report all rows for that user
        // 4.       Sum up those rows
        // 5.       Report totals
        //
        // 6. Report total results of all totals

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
            String dataIndicator = (teamName != null) ? teamName : boardId
            String filename = getCommandLineOptions().'outputCSV'.replace(".csv", "-${dataIndicator}-${user}.csv")
            writeResultsFile(filename, sb)
        }}
    }
}