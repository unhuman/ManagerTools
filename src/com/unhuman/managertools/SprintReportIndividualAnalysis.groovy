package com.unhuman.managertools

import com.unhuman.flexidb.FlexiDBQueryColumn
import com.unhuman.flexidb.data.FlexiDBRow
import com.unhuman.managertools.data.DBIndexData
import com.unhuman.managertools.util.CommandLineHelper
import groovy.cli.commons.CliBuilder

import java.util.stream.Collectors

class SprintReportIndividualAnalysis extends SprintReportTeamAnalysis {
    List<String> teamUsers

    @Override
    protected void setupRun() {
        super.setupRun()
        // Load the command line helper here for ability to manage users / team
        CommandLineHelper commandLineHelper = new CommandLineHelper(".managerTools.cfg")
        teamUsers = commandLineHelper.getBoardTeamUsers(commandLineOptions.'boardId')
    }

    @Override
    protected void generateOutput(ArrayList<String> columnOrder) {
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

        // TODO: Add empty column first to be able to display totals?

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

                List<FlexiDBRow> rows = database.findRows(userSprintFinder, true)

                // Render rows
                FlexiDBRow sprintTotalsRow = new FlexiDBRow(columnOrder.size())
                rows.each {row -> {
                    sb.append(row.toCSV(columnOrder))
                    sb.append('\n')

                    // Build up totals
                    columnOrder.each { column -> {
                        Object value = row.get(column)
                        if ((value instanceof Integer) || (value instanceof Long)) {
                            Long longValue = (Long) value
                            // add to sprint totals
                            sprintTotalsRow.put(column, sprintTotalsRow.containsKey(column)
                                    ? sprintTotalsRow.get(column) + longValue : longValue)
                            // add to overall totals
                            overallTotalsRow.put(column, overallTotalsRow.containsKey(column)
                                    ? overallTotalsRow.get(column) + longValue : longValue)
                        }
                    }}
                }}

                // Totals for Sprint
                if (!sprintTotalsRow.containsKey(columnOrder.get(0))) {
                    sprintTotalsRow.put(columnOrder.get(0), "Sprint Totals")
                }
                sb.append(sprintTotalsRow.toCSV(columnOrder))
                sb.append('\n')

                // space between sprints
                sb.append('\n')
            }}

            // Overall totals
            if (!overallTotalsRow.containsKey(columnOrder.get(0))) {
                overallTotalsRow.put(columnOrder.get(0), "Overall Totals")
            }
            sb.append(overallTotalsRow.toCSV(columnOrder))
            sb.append('\n')

            String filename = getCommandLineOptions().'outputCSV'.replace(".csv", "-${commandLineOptions.'boardId'}-${user}.csv")
            System.out.println("Writing file: ${filename}")
            try (PrintStream out = new PrintStream(new FileOutputStream(filename))) {
                out.print(sb.toString());
            }
        }}
    }
}