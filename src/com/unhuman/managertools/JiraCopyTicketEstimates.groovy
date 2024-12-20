package com.unhuman.managertools

import com.unhuman.managertools.util.CommandLineHelper
import com.unhuman.managertools.rest.JiraREST

import java.util.regex.Matcher
import java.util.regex.Pattern

class JiraCopyTicketEstimates extends Script {
    private final Pattern QUARTER_PATTERN = Pattern.compile("q[1234]", Pattern.CASE_INSENSITIVE)

    @Override
    Object run() {
        CommandLineHelper commandLineHelper = new CommandLineHelper(".managerTools.cfg")
        String jiraServer = commandLineHelper.getJiraServer()
        String jiraCookies = commandLineHelper.getJiraAuth()

        JiraREST jiraREST = new JiraREST(jiraServer, jiraCookies)

        // TODO: Make this data parameters?
        // TODO: Make input easier (prompt for only team and quarter, figure out the date)

        String sprintTeam = commandLineHelper.getSprintTeam()
        String boardId = commandLineHelper.getSprintTeamBoardId()

                // prompt for values - but insert expected dashes
        String sourceQuarter = commandLineHelper.prompt("Enter Quarter for source tickets (ex: q3)", QUARTER_PATTERN)
        String destinationQuarter = commandLineHelper.prompt("Enter Quarter for destination tickets (ex: q4)", QUARTER_PATTERN)
        String dateForData = commandLineHelper.getDateCheck("after which all tickets were created", "copyTicketDateCreation")

        Boolean doIt = commandLineHelper.prompt("Type in \"DoIt\" to perform the migration (anything else is an audit)").toUpperCase().equals("DOIT")

        String sourceJql = generateJQL(sprintTeam, sourceQuarter, dateForData)
        String destinationJql = generateJQL(sprintTeam, destinationQuarter, dateForData)

        // We allow a prefix (to reflect adopted / carryover /etc on source only)
        Pattern sourcePattern = convertToPattern(true, sourceQuarter)
        Pattern destinationPattern = convertToPattern(false, destinationQuarter)

        System.out.println("Requesting source data...")
        Object sourceResults = jiraREST.jqlSummaryQuery(sourceJql)
        System.out.println("Requesting destination data...")
        Object destinationResults = jiraREST.jqlSummaryQuery(destinationJql)

        // sourceResults.issues[].key == ticket
        // sourceResults.issues[].fields.summary = summary
        // sourceResults.issues[].fields.aggregatetimeoriginalestimate = time in seconds

        int counter = 0
        for (Object sourceIssue: sourceResults.issues) {
            Long sourceOriginalEstimate = sourceIssue.fields.aggregatetimeoriginalestimate
            if (sourceOriginalEstimate && sourceOriginalEstimate > 0) {
                String sourceSummary = sourceIssue.fields.summary
                Matcher sourceMatcher = sourcePattern.matcher(sourceSummary)
                if (sourceMatcher.matches()) {
                    String sourceIssueSummaryType = sourceMatcher.group(1)
                    String sourceTicket = sourceIssue.key
                    // System.out.println("Source: ${sourceTicket} ${sourceIssueSummaryType} = ${sourceOriginalEstimate}")

                    // Find a matching destination issue
                    List<Object> destinationIssues = new ArrayList<>()
                    for (Object destinationIssue : destinationResults.issues) {
                        Matcher destinationMatcher = destinationPattern.matcher(destinationIssue.fields.summary)
                        Long destinationOriginalEstimate = destinationIssue.fields.aggregatetimeoriginalestimate

                        // Sometimes descriptions get annotated, so source must start with the destination
                        if (destinationMatcher.matches() && sourceIssueSummaryType.startsWith(destinationMatcher.group(1)) &&
                                (destinationOriginalEstimate == null || destinationOriginalEstimate == 0)) {
                            destinationIssues.add(destinationIssue)
                        }
                    }

                    switch (destinationIssues.size()) {
                        case 0:
                            // Nothing to do
                            break
                        case 1:
                            // process
                            ++counter
                            String updateTicket = destinationIssues.get(0).key
                            System.out.println("   ${!doIt ? "AUDIT " : ""}" +
                                    "Copy estimate ${sourceOriginalEstimate}" +
                                    " from ticket ${sourceTicket}: ${sourceSummary}" +
                                    " to ticket ${updateTicket}: ${destinationIssues.get(0).fields.summary}")
                            if (doIt) {
                                jiraREST.updateOriginalEstimate(updateTicket, boardId, sourceOriginalEstimate)
                            }
                            break
                        default:
                            System.out.println("   Too many matches: ")
                            destinationIssues.each(errorDestinationIssue ->
                                System.out.println("      ${errorDestinationIssue.key} - ${errorDestinationIssue.fields.summary}")
                            )
                    }
                }
            }
        }
        System.out.println("${!doIt ? "AUDIT " : ""}Processed ${counter} tickets")
    }

    private String generateJQL(String sprintTeam, String quarter, String ticketAfterDate) {
        String jql = "created>${ticketAfterDate}"
        jql += " AND summary~\"${quarter}\""
        jql += " AND \"Sprint Team\"=\"${sprintTeam}\""
        return jql
    }

    private Pattern convertToPattern(boolean allowPrefixedData, String quarterPart) {
        String regex = "${allowPrefixedData ? ".*" : ""}${regexEscape(quarterPart)}(.*)"
        return Pattern.compile(regex, Pattern.CASE_INSENSITIVE)
    }

    private String regexEscape(String regex) {
        return regex.replaceAll("[\\W]", "\\\\\$0")
    }
}
