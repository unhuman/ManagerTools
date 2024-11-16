package com.unhuman.managertools

@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.codehaus.gpars', module='gpars', version='1.2.1'),
])

import com.unhuman.flexidb.FlexiDB
import com.unhuman.flexidb.FlexiDBQueryColumn
import com.unhuman.flexidb.data.FlexiDBRow
import com.unhuman.flexidb.init.AbstractFlexiDBInitColumn
import com.unhuman.flexidb.init.FlexiDBInitDataColumn
import com.unhuman.flexidb.init.FlexiDBInitIndexColumn
import com.unhuman.flexidb.output.ConvertZerosToEmptyOutputFilter
import com.unhuman.flexidb.output.OutputFilter
import com.unhuman.managertools.data.DBData
import com.unhuman.managertools.data.DBIndexData
import com.unhuman.managertools.data.UserActivity
import com.unhuman.managertools.output.ConvertSelfMetricsEmptyToZeroOutputFilter
import com.unhuman.managertools.rest.SourceControlREST
import com.unhuman.managertools.rest.exceptions.RESTException
import groovy.cli.commons.CliBuilder
import groovyx.gpars.GParsPool
import org.apache.hc.core5.http.HttpStatus


import java.text.SimpleDateFormat
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

class SprintReportTeamAnalysis extends AbstractSprintReport {
    static final List<String> IGNORE_USERS = ["codeowners".toLowerCase(),
                                              "DeployMan".toLowerCase(),
                                              "sa-sre-jencim".toLowerCase()]
    static final List<String> IGNORE_COMMENTS = ["Tasks to Complete Before Merging Pull Request"]

    static final String SELF_PREFIX = "SELF_"
    static final String TOTAL_PREFIX = "TOTAL_"

    static final String PR_PREFIX = "PR_"
    static final String COMMIT_PREFIX = "COMMIT_"

    static final List<OutputFilter> STANDARD_OUTPUT_RULES =
            Collections.singletonList(new ConvertZerosToEmptyOutputFilter())
    static final List<OutputFilter> SAME_USER_OUTPUT_RULES =
            Arrays.asList(new ConvertSelfMetricsEmptyToZeroOutputFilter())

    static final SimpleDateFormat DATE_PARSER = new SimpleDateFormat("dd/MMM/yy", Locale.US)
    static final SimpleDateFormat DATE_TIME_PARSER = new SimpleDateFormat("dd/MMM/yy h:mm a", Locale.US)
    static final SimpleDateFormat DATE_OUTPUT = new SimpleDateFormat("yyyy/MM/dd", Locale.US);


    FlexiDB database
    // We need to track items we have processed to prevent them from appearing twice
    // This can occur when a PR winds up linked to multiple tickets
    Set<Object> processedItems

    @Override
    def addCustomCommandLineOptions(CliBuilder cli) {
        cli.o(longOpt: 'outputCSV', required: true, args: 1, argName: 'outputCSV', 'Output filename (.csv)')
    }

    @Override
    def validateCustomCommandLineOptions() {
        super.validateCustomCommandLineOptions()
        if (!getCommandLineOptions().'outputCSV'.endsWith(".csv")) {
            throw new RuntimeException("Output filename must end in .csv")
        }
    }

    @Override
    def process(String teamName, String boardId, List<String> sprintIds) {
        long time = System.currentTimeMillis()
        database = new FlexiDB(generateDBSignature(), true)
        processedItems = new ConcurrentHashMap<>().newKeySet()

        // populate the database
        System.out.println("Processing ${sprintIds.size()} sprints...")
        for (int i = 0; i < sprintIds.size(); i++) {
            String sprintId = sprintIds.get(i)

            Object data = jiraREST.getSprintReport(boardId, sprintId)

            def allIssues = new ArrayList()
            allIssues.addAll(data.contents.completedIssues)
            allIssues.addAll(data.contents.issuesNotCompletedInCurrentSprint)

            System.out.println("${i+1} / ${sprintIds.size()}: ${data.sprint.name} (id: ${sprintId}" +
                    ", issues: ${allIssues.size()})")

            // Gather ticket data for all issues (completed and incomplete work)
            getIssueCategoryInformation(data.sprint, allIssues)
        }

        // Generate the CSV file - we'll do some column adjustments
        generateOutput()

        time = (long) ((System.currentTimeMillis() - time) / 1000)
        Calendar.instance.with {
            clear()
            set(SECOND, (Integer) time)
            System.out.println("Time to process: ${format('HH:mm:ss')}")
        }
    }

    protected List<String> generateColumnsOrder() {
        List<String> columnOrder = new ArrayList<>(database.getOriginalColumnOrder())
        // start date right after sprint, which is first
        columnOrder.remove(DBData.START_DATE.name())
        columnOrder.add(1, DBData.START_DATE.name())
        columnOrder.remove(DBData.END_DATE.name())
        columnOrder.add(2, DBData.END_DATE.name())

        // Remove SELF_COMMENTED and OTHERS_COMMENTED since they don't apply to Sprint Reports
        columnOrder.remove(UserActivity.SELF_COMMENTED.name())
        columnOrder.remove(UserActivity.OTHERS_COMMENTED.name())

        // comments & commit messages are currently generated last - if things changed, might need to manage that here
        return columnOrder
    }

    protected void generateOutput() {
        // Determine the list of columns to report
        List<String> columnOrder = generateColumnsOrder()

        List<String> sprints = database.findUniqueValues(DBIndexData.SPRINT.name())
        LinkedHashSet<String> users = new LinkedHashSet<>(database.findUniqueValues(DBIndexData.USER.name()))

        // 1. Iterate through sprints
        // 2.    Report all rows for that sprint
        // 3.    Sum up those rows
        // 4.    Report totals
        //
        // 5. Report total results of all totals

        StringBuilder sb = new StringBuilder(4096)
        sb.setLength(0)
        sb.append(FlexiDBRow.headingsToCSV(columnOrder))
        sb.append('\n')

        FlexiDBRow overallTotalsRow = new FlexiDBRow(columnOrder.size())
        sprints.each {
            sprint -> {
                List<FlexiDBQueryColumn> sprintFinder = new ArrayList<>()
                sprintFinder.add(new FlexiDBQueryColumn(DBIndexData.SPRINT.name(), sprint))

                findRowsAndAppendCSVData(sprintFinder, sb, overallTotalsRow)
            }
        }

        // Summary
        appendSummary(sb, overallTotalsRow)

        // Write file
        String dataIndicator = (teamName != null) ? teamName : boardId
        String filename = getCommandLineOptions().'outputCSV'.replace(".csv", "-${dataIndicator}.csv")
        writeResultsFile(filename, sb)
    }

    /**
     * Update Find rows matching the sprintFinder, CSV stringbuilder, track totals in provided overallTotalsRow
     * @param rowsFilter
     * @param sb (will be updated)
     * @param overallTotalsRow (will be updated)
     * @return
     */
    StringBuilder findRowsAndAppendCSVData(ArrayList<FlexiDBQueryColumn> rowsFilter, StringBuilder sb, overallTotalsRow) {
        List<FlexiDBRow> rows = database.findRows(rowsFilter, true)
        if (rows.size() == 0) {
            return sb
        }

        // Determine the list of columns to report
        List<String> columnOrder = generateColumnsOrder()

        // Render rows
        FlexiDBRow sprintTotalsRow = new FlexiDBRow(columnOrder.size())
        rows.each { row ->
            {
                // We have some special output rules for SELF_COMMENTS and OTHERS_COMMENTS
                // when the user is the author
                sb.append(row.toCSV(columnOrder, STANDARD_OUTPUT_RULES))
                sb.append('\n')

                // Build up totals
                columnOrder.each { column ->
                    {
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
                    }
                }
            }
        }

        // Totals for Sprint
        appendTotalsInfo(sb, "Sprint Totals", sprintTotalsRow)
        // space between sprints
        return sb.append('\n')
    }

    protected void appendSummary(StringBuilder sb, FlexiDBRow overallTotalsRow) {
        // Overall totals
        appendTotalsInfo(sb, "Overall Totals", overallTotalsRow)
    }

    protected void writeResultsFile(String filename, StringBuilder sb) {
        System.out.println("Writing file: ${filename}")
        try (PrintStream out = new PrintStream(new FileOutputStream(filename))) {
            out.print(sb.toString());
        }
    }

    def getIssueCategoryInformation(final Object sprint, List<Object> issueList) {
        String sprintName = sprint.name
        String startDate = cleanDate(sprint.startDate)
        String endDate = cleanDate(sprint.endDate)

        // Track exact time for ensuring operations occur within sprint
        Date sprintStartTime = DATE_TIME_PARSER.parse(sprint.startDate)
        Date sprintEndTime = DATE_TIME_PARSER.parse(sprint.endDate)

        AtomicInteger counter = new AtomicInteger()
        issueList = Collections.synchronizedList(issueList)
        GParsPool.withPool() {
            issueList.eachParallel(issue -> {
                def ticket = issue.key
                def issueId = issue.id

                def pullRequests = []
                try {
                    pullRequests = jiraREST.getTicketPullRequestInfo(issueId.toString())
                } catch (RESTException re) {
                    if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND) {
                        return
                    }
                }
                System.out.println("   ${counter.incrementAndGet()}/${issueList.size()}: ${ticket} / Issue ${issueId} has ${pullRequests.size()} PRs")
                pullRequests.each(pullRequest -> {
                    // based on the PR - determine where the source is - choosing github or (default) bitbucket
                    String prUrl = pullRequest.url
                    boolean isGithub = prUrl.toLowerCase().contains("github.com/")
                    SourceControlREST sourceControlREST = (isGithub) ? githubREST : bitbucketREST

                    // can get approvers out of ^^^
                    // check comment count before polling for comments
                    def prId = (pullRequest.id.startsWith("#") ? pullRequest.id.substring(1) : pullRequest.id)
                    def prAuthor = sourceControlREST.mapNameToJiraName(pullRequest.author.name) // Below, we try to update this to match the username

                    prUrl = sourceControlREST.apiConvert(prUrl)

                    // Get and process activities (comments, etc)
                    def prActivities = sourceControlREST.getActivities(prUrl)

                    def commentBlockers = new ArrayList<CommentBlocker>()

                    System.out.println("      PR ${ticket} / ${prId} has ${prActivities.values.size()} activities")
                    // process from oldest to newest (reverse)
                    for (int i = prActivities.values.size() - 1; i >= 0; i--) {
                        def prActivity = (prActivities instanceof List) ? prActivities[0] : prActivities.values.get(i)
                        // map the user name from source control
                        String userName = prActivity.user.name // already mapped from getActivities()
                        // try to match up the names better
                        prAuthor = (prAuthor.equals(prActivity.user.displayName)) ? userName : prAuthor

                        // Skip this if not desired
                        if (IGNORE_USERS.contains(userName.toLowerCase())) {
                            continue
                        }

                        // Get / ensure we have a known action
                        UserActivity prActivityAction = UserActivity.getResolvedValue((String) prActivity.action)
                        if (prActivityAction == null) {
                            // this was logged
                            continue
                        }

                        // If we have already processed this activity or the activity didn't occur in this sprint, don't include it
                        if (processedItems.contains(prActivity.id)
                                || sprintStartTime.getTime() > prActivity.createdDate
                                || prActivity.createdDate >= sprintEndTime.getTime()) {
                            continue
                        }

                        // Generate index to look for data
                        List<FlexiDBQueryColumn> indexLookup = createIndexLookup(sprintName, ticket, prId, userName)
                        populateBaselineDBInfo(indexLookup, startDate, endDate, prAuthor)

                        // Track we have processed this item
                        processedItems.add(prActivity.id)

                        // TODO: Github actions

                        switch (prActivityAction) {
                            case UserActivity.APPROVED.name():
                                break
                            case UserActivity.COMMENTED.name():
                                processComment(commentBlockers, indexLookup, prAuthor, prActivity)
                                // processComment updates counters due to nested data
                                continue
                            case UserActivity.DECLINED.name():
                                break
                            case UserActivity.MERGED.name():
                                break
                            case UserActivity.OPENED.name():
                                break
                            case UserActivity.RESCOPED.name():
                                break
                            case UserActivity.UNAPPROVED.name():
                                break
                            case UserActivity.UPDATED.name():
                                break
                        }

                        // increment counter
                        incrementCounter(indexLookup, prActivityAction)
                    }

                    // Get and process commits
                    def prCommits = sourceControlREST.getCommits(prUrl)
                    if (prCommits == null) {
                        return
                    }
                    for (int i = prCommits.values.size() - 1; i >= 0; i--) {
                        def commit = (prCommits instanceof List) ? prCommits.get(i) : prCommits.values.get(i)
                        String commitSHA = commit.id
                        Long commitTimestamp = commit.committerTimestamp

                        // If we have already processed this activity or the activity didn't occur in this sprint, don't include it
                        // TODO: Duplicate of operations for activities
                        if (processedItems.contains(commitSHA)
                                || sprintStartTime.getTime() > commitTimestamp
                                || commitTimestamp >= sprintEndTime.getTime()) {
                            continue
                        }
                        // Track we have processed this item
                        processedItems.add(commitSHA)

                        // Github can put the name in multiple places, which is painful
                        String userName = commit.committer.name

                        // Skip this if not desired (unlikely in this case)
                        // TODO: Duplicate of operations for activities
                        if (IGNORE_USERS.contains(userName.toLowerCase())) {
                            continue
                        }

                        // Generate index to look for data
                        // TODO: this could cause issues with a data disconnect between username + prAuthor
                        List<FlexiDBQueryColumn> indexLookup = createIndexLookup(sprintName, ticket, prId, userName)
                        populateBaselineDBInfo(indexLookup, startDate, endDate, prAuthor)

                        // use the commit url if there is one, else use that from the PR
                        String commitUrl = (commit.url != null) ? commit.url : prUrl

                        def diffsResponse = sourceControlREST.getCommitDiffs(commitUrl, commitSHA)
                        if (diffsResponse != null) {
                            processDiffs(COMMIT_PREFIX, diffsResponse, indexLookup)
                        }

                        // Add pr commit messages to database
                        def commitMessage = commit.message.replaceAll("(\\r|\\n)?\\n", "  ").trim()
                        database.append(indexLookup, DBData.COMMIT_MESSAGES.name(), commitMessage, true)
                        // TODO: Add counter for commits
                        // incrementCounter(currentUserIndexLookup, JiraDBActions.COMMIT)
                    }

                    // only populate pull request data if there was commit activity
                    // NOTICE: In this mode, all attributions go to the PR author
                    List<FlexiDBQueryColumn> indexLookup = createIndexLookup(sprintName, ticket, prId, prAuthor)
                    if (database.findRows(indexLookup, false)
                            && (database.getValue(indexLookup, UserActivity.COMMIT_ADDED.toString()) > 0
                            || database.getValue(indexLookup, UserActivity.COMMIT_REMOVED.toString()) > 0)) {
                        // Process Pull Request data
                        def diffsResponse = sourceControlREST.getDiffs(prUrl)
                        if (diffsResponse != null) {
                            // Generate index to look for data
                            populateBaselineDBInfo(indexLookup, startDate, endDate, prAuthor)
                            processDiffs(PR_PREFIX, diffsResponse, indexLookup)
                        }
                    }
                })

                // Find comments in Jira - we do this after we process PR's so we can allocate to an appropriate
                // PR.
                // TODO: This is currently broken
//        try {
//            // TODO: Cache this request since we may need to refer back to the same data
//            def jiraComments = jiraREST.getTicket(ticket).fields.comment.comments
//            jiraComments.each(comment -> {
//                def commentText = comment.body
//                def commentDate = (comment.updated != null) ? comment.updated : comment.created
//                def commentAuthor = (comment.updated != null) ? comment.updateAuthor.name : comment.author.name
//
//                processComment(indexLookup, prAuthor, prActivity)
//            })
//        } catch (NullPointerException npe) {
//            System.err.println("Ticket: ${ticket} could not find comments")
//        } catch (RESTException re) {
//            if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND) {
//                return
//            }
//        }
            })
        }
    }

    protected List<FlexiDBQueryColumn> createIndexLookup(String sprintName, ticket, prId, String userName) {
        List<FlexiDBQueryColumn> indexLookup = new ArrayList<>()
        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.SPRINT.name(), sprintName))
        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.TICKET.name(), ticket))
        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.PR_ID.name(), prId))
        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.USER.name(), userName))
        return indexLookup
    }

    protected void populateBaselineDBInfo(List<FlexiDBQueryColumn> indexLookup, String startDate, String endDate, String prAuthor) {
        database.setValue(indexLookup, DBData.START_DATE.name(), startDate)
        database.setValue(indexLookup, DBData.END_DATE.name(), endDate)
        database.setValue(indexLookup, DBData.AUTHOR.name(), prAuthor)
    }

    protected void processDiffs(String prefix, def diffsResponse, List<FlexiDBQueryColumn> indexLookup) {
        // If we have stats - just use them (Github cases)
        Integer additions = diffsResponse.additions
        Integer deletions = diffsResponse.deletions
        if (diffsResponse.stats) {
            additions = diffsResponse.stats.additions
            deletions = diffsResponse.stats.deletions
        }
        if (additions != null || deletions != null) {
            incrementCounter(indexLookup, UserActivity.valueOf(prefix + "ADDED"), additions)
            incrementCounter(indexLookup, UserActivity.valueOf(prefix + "REMOVED"), deletions)
            return
        }

        // copilot suggested this as a way to find all the content
        int addedCalculated = 0
        int removedCalculated = 0
        diffsResponse.diffs.each { diff ->
            diff.hunks.each { hunk ->
                hunk.segments.each { segment ->
                    addedCalculated += (segment.type == "ADDED") ? segment.lines.size() : 0
                    removedCalculated += (segment.type == "REMOVED") ? segment.lines.size() : 0

//                    segment.lines.each { line ->
//                        println("${segment.type}: ${line}")
//                    }
                }
            }
        }

        int addedCalculated2 = 0
        int removedCalculated2 = 0
        // aggregate the total responses to get all the diffs
        diffsResponse.diffs.forEach { diff ->
            {
                // sometimes these can be null - file comments is an example
                if (diff.hunks == null) {
                    return
                }
                diff.hunks.forEach(hunk -> {
                    int added = 0
                    int removed = 0

                    hunk.segments.forEach(segment -> {
                        // TODO: within a hunk, multiple segments (likely) indicates a MODIFIED
                        //       these segments seem to have REMOVED and ADDED both
                        switch (segment.type) {

                            // TODO:
                            // for every line, track source + destination to see if a value is added, deleted, or modified

                            case "ADDED":
                                added += segment.lines.size()
                                break
                            case "REMOVED":
                                // Note this is silly for now, since it's the same as ADDED, for now
                                removed += segment.lines.size()
                                break
                        }
                    })

                    addedCalculated2 = incrementCounter(indexLookup, UserActivity.valueOf(prefix + "ADDED"), added)
                    removedCalculated2 = incrementCounter(indexLookup, UserActivity.valueOf(prefix + "REMOVED"), removed)
                })
            }
        }

        if (prefix.startsWith(getPR_PREFIX()) && ((addedCalculated != addedCalculated2) || (removedCalculated != removedCalculated2))) {
            System.out.println("Add1: ${addedCalculated} Add2: ${addedCalculated2} Remove1: ${removedCalculated} Remove2: ${removedCalculated2}")
        }
    }

    /**
     * Increment counters for data provided
     * @param indexLookup
     * @param prActivityAction
     * @param dbActivityAction
     */
    protected int incrementCounter(ArrayList<FlexiDBQueryColumn> indexLookup, UserActivity prActivityAction) {
        return incrementCounter(indexLookup, prActivityAction, 1)
    }

    /**
     * Increment counters for data provided
     * @param indexLookup
     * @param prActivityAction
     * @param dbActivityAction
     */
    protected int incrementCounter(ArrayList<FlexiDBQueryColumn> indexLookup, UserActivity prActivityAction, int increment) {
        UserActivity dbActivityAction = prActivityAction
        return database.incrementField(indexLookup, dbActivityAction.name(), increment)
    }

    /**
     * process comments - this will update the counters since the data can be recursive
     * @param indexLookup
     * @param prAuthor
     * @param prActivity
     * @return
     */
    def processComment(List commentBlockers, List<FlexiDBQueryColumn> indexLookup, String prAuthor, Object prActivity) {
        processComment(commentBlockers, indexLookup, prAuthor, prActivity.user.name, prActivity.action, prActivity.commentAction, prActivity.comment, 3)
    }

    /**
     * process comments - this will update the counters since the data can be recursive
     * @param commentBlockers (data kept to prevent associated comments from being processed)
     * @param indexLookup
     * @param prAuthor
     * @param userName
     * @param action
     * @param commentAction
     * @param comment
     * @param indentation
     * @return
     */
    def processComment(List<CommentBlocker> commentBlockers, List<FlexiDBQueryColumn> originalIndexLookup, String prAuthor,
                       String userName, String action, String commentAction, Object comment, int indentation) {

        // recreate the indexLookup with the actual user (and a version for the prAuthor)
        List<FlexiDBQueryColumn> sprintTicketPRIndexBase = originalIndexLookup.stream().filter { it.getName() != DBIndexData.USER.name() }.toList()
        List<FlexiDBQueryColumn> prAuthorUserIndexLookup = new ArrayList<>(sprintTicketPRIndexBase)
        prAuthorUserIndexLookup.add(new FlexiDBQueryColumn(DBIndexData.USER.name(), prAuthor))
        List<FlexiDBQueryColumn> currentUserIndexLookup = new ArrayList<>(sprintTicketPRIndexBase)
        currentUserIndexLookup.add(new FlexiDBQueryColumn(DBIndexData.USER.name(), userName))

        // TODO: distinguish comments on own PR versus others
        String commentText = comment.text

        // Skip comments from the same time (=/- 1 second) as the comment created that's blocked (with same author)
        for (CommentBlocker commentBlocker : commentBlockers) {
            if ((commentBlocker.name == comment.author.name)
                && (Math.abs(commentBlocker.date - comment.createdDate)) <= 1000)  {
                return
            }
        }

        // Skip this if not desired
        if (IGNORE_COMMENTS.contains(commentText)) {
            def commentBlocker = new CommentBlocker()
            commentBlocker.name = comment.author.name
            commentBlocker.date = comment.createdDate
            commentBlockers.add(commentBlocker)
            return
        }

        // Update comments
        commentText = commentText.replaceAll("(\\r|\\n)?\\n", "  ").trim()

        // Ensure we have baseline info for this user (copy times from original comment)
        // TODO: Since comments are nested, we are attributing them to the time of the first / parent
        // TODO: This may lead to mis-attribution to sprint based on the parent.
        populateBaselineDBInfo(currentUserIndexLookup,
                database.getValue(originalIndexLookup, DBData.START_DATE.name()),
                database.getValue(originalIndexLookup, DBData.END_DATE.name()),
                prAuthor)

        database.append(currentUserIndexLookup, DBData.COMMENTS.name(), commentText, true)
        incrementCounter(currentUserIndexLookup, UserActivity.COMMENTED)
        // Count pr author's own versus others' comment counts on the PR
        incrementCounter(prAuthorUserIndexLookup,
                (prAuthor == userName) ? UserActivity.SELF_COMMENTED : UserActivity.OTHERS_COMMENTED)

        // Recursively process responses
        if (comment.comments != null) {
            comment.comments.forEach(replyComment -> {
                // Use the original index lookup so we can determine if self
                processComment(commentBlockers, originalIndexLookup, prAuthor, replyComment.author.name,
                        UserActivity.COMMENTED.name(), "REPLY", replyComment, indentation + 3)
            })
        }
    }

    List<AbstractFlexiDBInitColumn> generateDBSignature() {
        List<AbstractFlexiDBInitColumn> columns = new ArrayList<>()

        // Searchable / index columns
        DBIndexData.values().each { index -> {
            columns.add(new FlexiDBInitIndexColumn(index.name()))
        }}

        // JiraDB Actions to the data columns
        for (int i = 0; i < UserActivity.values().length; i++) {
            UserActivity action = UserActivity.values()[i]
            columns.add(new FlexiDBInitDataColumn(action.name(), action.getDefaultValue()))
        }

        // Relevant Jira / Bitbucket Data
        DBData.values().each { data -> {
            columns.add(new FlexiDBInitDataColumn(data.name(), data.getDefaultValue()))
        }}

        return columns
    }

    /** append a row of totals information (with header)
     *
     * @param sb
     * @param totalsDescription (will try to use if first column is not calculated)
     * @param totalsRow
     */
    StringBuilder appendTotalsInfo(StringBuilder sb, String totalsDescription, FlexiDBRow totalsRow) {
        List<String> columnOrder = generateColumnsOrder()

        // if we can replace the first column (non-calculated value) we can clear out the first column heading
        if (!totalsRow.containsKey(columnOrder.get(0))) {
            columnOrder.set(0, "")
            totalsRow.put(columnOrder.get(0), totalsDescription)
        }

        for (int i = 1; i < columnOrder.size(); i++) {
            String columnName = columnOrder.get(i)
            try {
                if (totalsRow.containsKey(columnName) || UserActivity.valueOf(columnName) != null) { // always report
                    Object value = (totalsRow.get(columnOrder.get(i)) == UserActivity.valueOf(columnName).getDefaultValue())
                            ? 0 : totalsRow.get(columnOrder.get(i))
                    totalsRow.put(columnOrder.get(i), columnOrder.get(i) + ": " + value)
                }
            } catch (IllegalArgumentException e) {
                // ignore
            }
        }
        sb.append(totalsRow.toCSV(columnOrder))
        return sb.append('\n')
    }

    def cleanDate(String date) {
        return DATE_OUTPUT.format(DATE_PARSER.parse(date))
    }

    protected List<String> getSprintIds() {
        return sprintIds
    }

    class CommentBlocker {
        def name
        def date
    }
}