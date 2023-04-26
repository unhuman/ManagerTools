package com.unhuman.managertools

import com.unhuman.flexidb.FlexiDBQueryColumn
import com.unhuman.flexidb.data.FlexiDBRow
import com.unhuman.managertools.data.DBIndexData
import groovy.cli.commons.CliBuilder

import java.util.stream.Collectors

class SprintReportIndividualAnalysis extends SprintReportTeamAnalysis {
    @Override
    def addCustomCommandLineOptions(CliBuilder cli) {
        super.addCustomCommandLineOptions(cli)
        cli.u(longOpt: 'users', args: 1, required: true, argName: 'users', 'Users to limit processing to (comma separated)')
    }

    @Override
    protected void generateOutput(ArrayList<String> columnOrder) {
        List<String> sprints = database.findUniqueValues(DBIndexData.SPRINT.name())
        LinkedHashSet<String> users = new LinkedHashSet<>(database.findUniqueValues(DBIndexData.USER.name()))

        // Try to get values out of the database for user and case-insensitively up-convert the matches,
        // otherwise preserve unknown values (they won't matter)
        List<String> specifiedUsers = (getCommandLineOptions().'users' != null)
                ? Arrays.asList(getCommandLineOptions().'users'.split(',')) : null
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

            sprints.each { sprint -> {
                List<FlexiDBQueryColumn> userSprintFinder = new ArrayList<>()
                userSprintFinder.add(new FlexiDBQueryColumn(DBIndexData.SPRINT.name(), sprint))
                userSprintFinder.add(new FlexiDBQueryColumn(DBIndexData.USER.name(), user))

                List<FlexiDBRow> rows = database.findRows(userSprintFinder, true)

                // Render rows
                rows.each {row -> {
                    sb.append(row.toCSV(columnOrder))
                    sb.append('\n')
                }}

                // TODO: Totals for sprint

                // space between sprints
                sb.append('\n')
            }}

            // TODO: Totals Overall

            String filename = getCommandLineOptions().'outputCSV'.replace(".csv", "-${user}.csv")
            System.out.println("Writing file: ${filename}")
            try (PrintStream out = new PrintStream(new FileOutputStream(filename))) {
                out.print(sb.toString());
            }
        }}
    }
}