package com.unhuman.managertools

import com.unhuman.flexidb.FlexiDB
import com.unhuman.flexidb.FlexiDBQueryColumn
import com.unhuman.flexidb.data.FlexiDBRow
import com.unhuman.flexidb.init.AbstractFlexiDBInitColumn
import com.unhuman.flexidb.init.FlexiDBInitDataColumn
import com.unhuman.flexidb.init.FlexiDBInitIndexColumn
import com.unhuman.managertools.data.DBData
import com.unhuman.managertools.data.DBIndexData
import com.unhuman.managertools.data.JiraDBActions
import groovy.cli.commons.CliBuilder

import java.text.SimpleDateFormat

class SprintReportTeamAnalysis extends AbstractSprintReport {
    static final List<String> IGNORE_USERS = ["codeowners", "DeployMan"]
    static final List<String> IGNORE_COMMENTS = ["Tasks to Complete Before Merging Pull Request"]

    static final String SELF_PREFIX = "SELF_"
    static final String TOTAL_PREFIX = "TOTAL_"

    static final String PR_PREFIX = "PR_"
    static final String COMMIT_PREFIX = "COMMIT_"

    static final SimpleDateFormat DATE_PARSER = new SimpleDateFormat("dd/MMM/yy")
    static final SimpleDateFormat DATE_TIME_PARSER = new SimpleDateFormat("dd/MMM/yy K:mm a")
    static final SimpleDateFormat DATE_OUTPUT = new SimpleDateFormat("yyyy/MM/dd");

    FlexiDB database
    // We need to track items we have processed to prevent them from appearing twice
    // This can occur when a PR winds up linked to multiple tickets
    Set<Object> processedItems

    @Override
    def addCustomCommandLineOptions(CliBuilder cli) {
        cli.o(longOpt: 'outputCSV', required: true, args: 1, argName: 'outputCSV', 'Output filename (.csv)')
        cli.d(longOpt: 'detailed', args: 1, argName: 'detailed', 'Detailed breakdown of participation counts')
    }

    @Override
    def validateCustomCommandLineOptions() {
        super.validateCustomCommandLineOptions()
        if (!getCommandLineOptions().'outputCSV'.endsWith(".csv")) {
            throw new RuntimeException("Output filename must end in .csv")
        }
    }

    @Override
    def process(String boardId, List<String> sprintIds) {
        database = new FlexiDB(generateDBSignature())
        processedItems = new HashSet<>()

        // populate the database
        System.out.println("Processing ${sprintIds.size()} sprints...")
        for (int i = 0; i < sprintIds.size(); i++) {
            String sprintId = sprintIds.get(i)

            Object data = jiraREST.getSprintReport(boardId, sprintId)
            System.out.println("${i+1} / ${sprintIds.size()}: ${data.sprint.name}")

            // Gather ticket data for completed and incomplete work
            getIssueCategoryInformation(data.sprint, data.contents.completedIssues)
            getIssueCategoryInformation(data.sprint, data.contents.issuesNotCompletedInCurrentSprint)
        }

        // Generate the CSV file - we'll do some column adjustments
        generateOutput()
    }

    protected List<String> generateColumnsOrder() {
        List<String> columnOrder = new ArrayList<>(database.getOriginalColumnOrder())
        // start date right after sprint, which is first
        columnOrder.remove(DBData.START_DATE.name())
        columnOrder.add(1, DBData.START_DATE.name())
        columnOrder.remove(DBData.END_DATE.name())
        columnOrder.add(2, DBData.END_DATE.name())
        // comments are currently generated last - if things changed, might need to manage that here
        columnOrder
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
        String filename = getCommandLineOptions().'outputCSV'.replace(".csv", "-${commandLineOptions.'boardId'}.csv")
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
        // Determine the list of columns to report
        List<String> columnOrder = generateColumnsOrder()

        List<FlexiDBRow> rows = database.findRows(rowsFilter, true)

        // Render rows
        FlexiDBRow sprintTotalsRow = new FlexiDBRow(columnOrder.size())
        rows.each { row ->
            {
                sb.append(row.toCSV(columnOrder))
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

        issueList.each(issue -> {
            def ticket = issue.key
            def issueId = issue.id

            def prInfo = jiraREST.getTicketPullRequestInfo(issueId.toString())
            prInfo.detail.each(prDetail -> {
                System.out.println("   ${ticket} / Issue ${issueId} has ${prDetail.pullRequests.size()} PRs")
                prDetail.pullRequests.each(pullRequest -> {
                    // can get approvers out of ^^^
                    // check comment count before polling for comments
                    def prId = (pullRequest.id.startsWith("#") ? pullRequest.id.substring(1) : pullRequest.id)
                    def prAuthor = pullRequest.author.name // Below, we try to update this to match the username
                    def prUrl = pullRequest.url

                    // Get and process activities (comments, etc)
                    def prActivities = bitbucketREST.getActivities(prUrl)

                    System.out.println("      PR ${ticket} / ${prId} has ${prActivities.values.size()} activities")
                    // process from oldest to newest (reverse)
                    for (int i = prActivities.values.size() - 1; i >= 0; i--) {
                        prActivity = prActivities.values.get(i)
                        String userName = prActivity.user.name
                        // try to match up the names better
                        prAuthor = (prAuthor.equals(prActivity.user.displayName)) ? userName : prAuthor
                        boolean isSelf = (userName.equals(prAuthor))

                        // Skip this if not desired
                        if (IGNORE_USERS.contains(userName)) {
                            continue
                        }

                        // Generate index to look for data
                        List<FlexiDBQueryColumn> indexLookup = createIndexLookup(sprintName, ticket, prId, userName)

                        database.setValue(indexLookup, DBData.START_DATE.name(), startDate)
                        database.setValue(indexLookup, DBData.END_DATE.name(), endDate)
                        database.setValue(indexLookup, DBData.AUTHOR.name(), prAuthor)

                        // Get / ensure we have a known action
                        JiraDBActions prActivityAction = JiraDBActions.getResolvedValue((String) prActivity.action)
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
                        // Track we have processed this item
                        processedItems.add(prActivity.id)

                        switch (prActivityAction) {
                            case JiraDBActions.APPROVED.name():
                                break
                            case JiraDBActions.COMMENTED.name():
                                processComment(indexLookup, prActivity)
                                // processComment updates counters due to nested data
                                continue
                            case JiraDBActions.DECLINED.name():
                                break
                            case JiraDBActions.MERGED.name():
                                break
                            case JiraDBActions.OPENED.name():
                                break
                            case JiraDBActions.RESCOPED.name():
                                break
                            case JiraDBActions.UNAPPROVED.name():
                                break
                            case JiraDBActions.UPDATED.name():
                                break
                        }

                        // increment counters (total and then specific) based on detailed configuration
                        incrementCounter(indexLookup, prActivityAction, isSelf)
                    }

                    // Get and process commits
                    def prCommits = bitbucketREST.getCommits(prUrl)

                    for (int i = prCommits.values.size() - 1; i >= 0; i--) {
                        def commit = prCommits.values.get(i)
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

                        String userName = commit.committer.name
                        // Skip this if not desired (unlikely in this case)
                        // TODO: Duplicate of operations for activities
                        if (IGNORE_USERS.contains(userName)) {
                            continue
                        }

                        // Generate index to look for data
                        List<FlexiDBQueryColumn> indexLookup = createIndexLookup(sprintName, ticket, prId, userName)
                        boolean isSelf = (userName.equals(prAuthor))

                        def diffsResponse = bitbucketREST.getCommitDiffs(prUrl, commitSHA)
                        processDiffs(COMMIT_PREFIX, diffsResponse, indexLookup, isSelf)
                    }

                    // Process Pull Request data
                    def diffsResponse = bitbucketREST.getDiffs(prUrl)

                    // Generate index to look for data
                    // NOTICE: In this mode, all attributions go to the PR author
                    List<FlexiDBQueryColumn> indexLookup = createIndexLookup(sprintName, ticket, prId, prAuthor)
                    processDiffs(PR_PREFIX, diffsResponse, indexLookup, true)
                })
            })
        })
    }

    protected List<FlexiDBQueryColumn> createIndexLookup(String sprintName, ticket, prId, String userName) {
        List<FlexiDBQueryColumn> indexLookup = new ArrayList<>()
        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.SPRINT.name(), sprintName))
        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.TICKET.name(), ticket))
        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.PR_ID.name(), prId))
        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.USER.name(), userName))
        return indexLookup
    }

    protected void processDiffs(String prefix, def diffsResponse, List<FlexiDBQueryColumn> indexLookup, boolean isSelf) {
        diffsResponse.diffs.forEach { diff ->
            {
                // sometimes these can be null - file comments is an example
                if (diff.hunks == null) {
                    return
                }
                diff.hunks.forEach(hunk -> {
                    int added = 0
                    int removed = 0

                    int sourceStart = hunk.sourceLine
                    int sourceEnd = sourceStart + hunk.sourceSpan - 1
                    int destinationStart = hunk.destinationLine
                    int destinationEnd = destinationStart + hunk.destinationSpan - 1

                    // Calculate the overlap as modified count
                    int modified = (sourceEnd > 0 && destinationEnd > 0) && (sourceStart <= destinationEnd) && (destinationStart <= sourceEnd)
                            ? Math.min(sourceEnd, destinationEnd) - Math.max(sourceStart, destinationStart) + 1 // always at least one line
                            : 0

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

                    // TODO: Make sure this is correct logic
                    // the modified lines are the smaller of the overlap (I think this may be over-simplified)
                    int modified2 = Math.min(added, removed)
                    
                    added -= modified
                    removed -= modified

                    incrementCounter(indexLookup, JiraDBActions.valueOf(prefix + "ADDED"), isSelf, added)
                    incrementCounter(indexLookup, JiraDBActions.valueOf(prefix + "REMOVED"), isSelf, removed)
                    incrementCounter(indexLookup, JiraDBActions.valueOf(prefix + "MODIFIED"), isSelf, modified)
                })
            }
        }
    }

    /**
     * Increment counters for data provided
     * @param indexLookup
     * @param prActivityAction
     * @param isSelf
     * @param dbActivityAction
     */
    protected void incrementCounter(ArrayList<FlexiDBQueryColumn> indexLookup, JiraDBActions prActivityAction, boolean isSelf) {
        incrementCounter(indexLookup, prActivityAction, isSelf, 1)
    }

    /**
     * Increment counters for data provided
     * @param indexLookup
     * @param prActivityAction
     * @param isSelf
     * @param dbActivityAction
     */
    protected void incrementCounter(ArrayList<FlexiDBQueryColumn> indexLookup, JiraDBActions prActivityAction, boolean isSelf, int increment) {
        JiraDBActions dbActivityAction = prActivityAction
        if (commandLineOptions.'detailed') {
            database.incrementField(indexLookup, TOTAL_PREFIX + prActivityAction.name())
            dbActivityAction = (isSelf)
                    ? JiraDBActions.valueOf(SELF_PREFIX + prActivityAction.name()) : prActivityAction
        }
        database.incrementField(indexLookup, dbActivityAction.name(), increment)
    }

    /**
     * process comments - this will update the counters since the data can be recursive
     * @param indexLookup
     * @param prActivity
     * @return
     */
    def processComment(List<FlexiDBQueryColumn> indexLookup, Object prActivity) {
        processComment(indexLookup, prActivity.user.name, prActivity.action, prActivity.commentAction, prActivity.comment, 3)
    }

    /**
     * process comments - this will update the counters since the data can be recursive
     * @param indexLookup
     * @param userName
     * @param action
     * @param commentAction
     * @param comment
     * @param indentation
     * @return
     */
    def processComment(List<FlexiDBQueryColumn> originalIndexLookup, String userName, String action, String commentAction, Object comment, int indentation) {

        // TODO: this self tracking & lookup handling are a bit wonky
        boolean isSelf = userName.equals(originalIndexLookup.stream().filter { it.getName() == DBIndexData.USER.name() }.toList()[0].getMatchValue())

        // recreate the indexLookup with the actual user
        List<FlexiDBQueryColumn> currentUserIndexLookup = new ArrayList<>(originalIndexLookup.stream().filter { it.getName() != DBIndexData.USER.name() }.toList())
        currentUserIndexLookup.add(new FlexiDBQueryColumn(DBIndexData.USER.name(), userName))

        // TODO: distinguish comments on own PR versus others
        String commentText = comment.text
        if (IGNORE_COMMENTS.contains(commentText)) {
            return
        }

        // Update comments
        commentText = commentText.replaceAll("(\\r|\\n)?\\n", "  ").trim()
        database.append(currentUserIndexLookup, DBData.COMMENTS.name(), commentText, true)
        incrementCounter(currentUserIndexLookup, JiraDBActions.COMMENTED, isSelf)

        // Recursively process responses
        comment.comments.forEach(replyComment -> {
            // Use the original index lookup so we can determine if self
            processComment(originalIndexLookup, replyComment.author.name, JiraDBActions.COMMENTED.name(), "REPLY", replyComment, indentation + 3)
        })
    }

    List<AbstractFlexiDBInitColumn> generateDBSignature() {
        List<AbstractFlexiDBInitColumn> columns = new ArrayList<>()

        // Searchable / index columns
        DBIndexData.values().each { index -> {
            columns.add(new FlexiDBInitIndexColumn(index.name()))
        }}

        // JiraDB Actions to the data columns
        for (int i = 0; i < JiraDBActions.values().length; i++) {
            JiraDBActions action = JiraDBActions.values()[i]
            columns.add(new FlexiDBInitDataColumn(action.name(), action.getDefaultValue()))
            i += (commandLineOptions.'detailed') ? 0 : JiraDBActions.DETAIL_DATA_SKIP_COUNT
        }

        // Relevant Jira Data
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
            if (totalsRow.containsKey(columnOrder.get(i))) {
                totalsRow.put(columnOrder.get(i), columnOrder.get(i) + ": " + totalsRow.get(columnOrder.get(i)))
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
}