import java.util.regex.Matcher
import java.util.regex.Pattern

class JiraCopyTicketEstimates extends Script {
    @Override
    Object run() {
        CommandLineHelper commandLineHelper = new CommandLineHelper(".managerTools.cfg")
        String jiraServer = commandLineHelper.getJiraServer()
        String jiraCookies = commandLineHelper.getJiraCookies()

        JiraREST jiraREST = new JiraREST(jiraServer, jiraCookies)

        // TODO: Make this data parameters?
        // TODO: Make input easier (prompt for only team and quarter, figure out the date)

        String sprintTeam = commandLineHelper.getSprintTeam()
        String boardId = commandLineHelper.getSprintTeamBoardId()
        sprintTeam = " - " + sprintTeam

                // prompt for values - but insert expected dashes
        String sourceQuarter = commandLineHelper.prompt("Enter Quarter for source tickets (ex: q3)") + " - "
        String destinationQuarter = commandLineHelper.prompt("Enter Quarter for destination tickets (ex: q4)") + " - "
        String dateForData = commandLineHelper.getDateCheck("after which all tickets were created", "copyTicketDateCreation")

        String sourceTicketQuery = "${sourceQuarter} - * - ${sprintTeam}"
        String destinationTicketQuery = "${destinationQuarter} - * - ${sprintTeam}"

        String sourceJql = generateJQL(sourceTicketQuery, dateForData)
        String destinationJql = generateJQL(destinationTicketQuery, dateForData)

        // We allow a prefix (to reflect adopted / carryover /etc on source only)
        Pattern sourcePattern = convertToPattern(true, sourceQuarter, sprintTeam)
        Pattern destinationPattern = convertToPattern(false, destinationQuarter, sprintTeam)

        System.out.println("Requesting source data...")
        Object sourceResults = jiraREST.jqlSummaryQuery(sourceJql)
        System.out.println("Requesting destination data...")
        Object destinationResults = jiraREST.jqlSummaryQuery(destinationJql)

        // sourceResults.issues[].key == ticket
        // sourceResults.issues[].fields.summary = summary
        // sourceResults.issues[].fields.aggregatetimeoriginalestimate = time in seconds

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

                        if (destinationMatcher.matches() && sourceIssueSummaryType.equals(destinationMatcher.group(1)) &&
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
                            String updateTicket = destinationIssues.get(0).key
                            System.out.println("   Copying estimate ${sourceOriginalEstimate}" +
                                    " from ticket ${sourceTicket}: ${sourceSummary}" +
                                    " to ticket ${updateTicket}: ${destinationIssues.get(0).fields.summary}")
                            jiraREST.updateOriginalEstimate(updateTicket, boardId, sourceOriginalEstimate)
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



    }

    private String generateJQL(String ticketQuery, String ticketAfterDate) {
        String[] components = ticketQuery.split("\\*");
        String jql = "created>${ticketAfterDate}"
        for (String component: components) {
            jql += " AND summary~\"${component}\""
        }
        return jql
    }

    private Pattern convertToPattern(boolean allowPrefixedData, String quarterPart, String teamPart) {
        String regex = "${allowPrefixedData ? ".*" : ""}${regexEscape(quarterPart)}(.*)${regexEscape(teamPart)}(.*)"
        return Pattern.compile(regex, Pattern.CASE_INSENSITIVE)
    }

    private String regexEscape(String regex) {
        return regex.replaceAll("[\\W]", "\\\\\$0")
    }
}
