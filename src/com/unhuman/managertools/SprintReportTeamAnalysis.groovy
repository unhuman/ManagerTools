package com.unhuman.managertools

import com.unhuman.flexidb.FlexiDB
import com.unhuman.flexidb.FlexiDBQueryColumn
import com.unhuman.flexidb.init.AbstractFlexiDBInitColumn
import com.unhuman.flexidb.init.FlexiDBInitDataColumn
import com.unhuman.flexidb.init.FlexiDBInitIndexColumn
import groovy.cli.commons.OptionAccessor

class SprintReportTeamAnalysis extends AbstractSprintReport {
    final List<String> IGNORE_USERS = [ "codeowners", "DeployMan" ]
    final List<String> IGNORE_COMMENTS = [ "Tasks to Complete Before Merging Pull Request" ]

    static final String SELF_PREFIX = "SELF_"
    static final String TOTAL_PREFIX = "SELF_"

    static final String DB_COLUMN_COMMENTS = "COMMENTS"

    enum DBIndexData {
        SPRINT("sprint"),
        TICKET("ticket"),
        PR_ID("prId"),
        USER("user")

        private String jiraField

        private DBIndexData(String jiraField) {
            this.jiraField = jiraField
        }

        String getJiraField() {
            return jiraField
        }
    }

    // Note these values could all get DB_SELF_PREFIX pre-pended based on user
    enum JiraDBActions {
        APPROVED(0), SELF_APPROVED(0), TOTAL_APPROVED(0),
        COMMENTED(0), SELF_COMMENTED(0), TOTAL_COMMENTED(0),
        DECLINED(0), SELF_DECLINED(0), TOTAL_DECLINED(0),
        MERGED(0), SELF_MERGED(0), TOTAL_MERGED(0),
        OPENED(0), SELF_OPENED(0), TOTAL_OPENED(0),
        RESCOPED(0), SELF_RESCOPED(0), TOTAL_RESCOPED(0),
        UNAPPROVED(0), SELF_UNAPPROVED(0), TOTAL_UNAPPROVED(0),
        UPDATED(0), SELF_UPDATED(0), TOTAL_UPDATED(0),

        private Object defaultValue

        private JiraDBActions(Object defaultValue) {
            this.defaultValue = defaultValue
        }

        Object getDefaultValue() {
            return defaultValue
        }

        static JiraDBActions getResolvedValue(String desiredAction) {
            try {
                return JiraDBActions.valueOf(desiredAction)
            } catch (Exception e) {
                System.out.println("   Unknown Jira action: ${desiredAction}")
                return null
            }
        }
    }

    // other data
    enum DBData {
        START_DATE("startDate", null),
        END_DATE("startDate", null),
        AUTHOR("author", null)

        private String jiraField
        private Object defaultValue

        private DBData(String jiraField, Object defaultValue) {
            this.jiraField = jiraField
            this.defaultValue = defaultValue
        }

        String getJiraField() {
            return jiraField
        }

        Object getDefaultValue() {
            return defaultValue
        }
    }

    static FlexiDB database = new FlexiDB(generateDBSignature())

    @Override
    def process(OptionAccessor commandLineOptions, String boardId, List<String> sprintIds) {
        sprintIds.each(sprintId -> {
            Object data = jiraREST.getSprintReport(boardId, sprintId)
            System.out.println(data.sprint.name)

            // Gather ticket data for completed and incomplete work
            getIssueCategoryInformation(data.sprint, data.contents.completedIssues)
            getIssueCategoryInformation(data.sprint, data.contents.issuesNotCompletedInCurrentSprint)
        })

        // Generate the CSV file - we'll do some column adjustments
        List<String> columnOrder = new ArrayList<>(database.getOriginalColumnOrder())
        columnOrder.remove(DBData.START_DATE.name())
        columnOrder.add(1, DBData.START_DATE.name())
        columnOrder.remove(DBData.END_DATE.name())
        columnOrder.add(1, DBData.END_DATE.name())

        System.out.println(database.toCSV())
    }

    def getIssueCategoryInformation(Object sprint, List<Object> issueList) {
        String sprintName = sprint.name
        String startDate = sprint.startDate
        String endDate = sprint.endDate

        issueList.each(issue -> {
            def ticket = issue.key
            def issueId = issue.id

            def prInfo = jiraREST.getTicketPullRequestInfo(issueId.toString())
            prInfo.detail.each(prDetail -> {
                System.out.println("${ticket} / Issue ${issueId} has ${prDetail.pullRequests.size()} PRs")
                prDetail.pullRequests.each(pullRequest -> {
                    // can get approvers out of ^^^
                    // check comment count before polling for comments
                    def prId = (pullRequest.id.startsWith("#") ? pullRequest.id.substring(1) : pullRequest.id)
                    def prAuthor = pullRequest.author.name // Below, we try to update this to match the username
                    def prUrl = pullRequest.url

                    def prActivities = bitbucketREST.getActivities(prUrl)

                    System.out.println("   PR ${ticket} / ${prId} has ${prActivities.values.size()} activities")
                    // process from oldest to newest (reverse)
                    for (int i = prActivities.size -1; i >= 0; i--) {
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
                        List<FlexiDBQueryColumn> indexLookup = new ArrayList<>()
                        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.SPRINT.name(), sprintName))
                        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.TICKET.name(), ticket))
                        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.PR_ID.name(), prId))
                        indexLookup.add(new FlexiDBQueryColumn(DBIndexData.USER.name(), userName))

                        database.setValue(indexLookup, DBData.START_DATE.name(), startDate)
                        database.setValue(indexLookup, DBData.END_DATE.name(), endDate)
                        database.setValue(indexLookup, DBData.AUTHOR.name(), prAuthor)

                        // Get / ensure we have a known action
                        JiraDBActions prActivityAction = JiraDBActions.getResolvedValue((String) prActivity.action)
                        if (prActivityAction == null) {
                            // this was logged
                            continue
                        }

                        switch (prActivityAction) {
                            case JiraDBActions.APPROVED.name():
                                break
                            case JiraDBActions.COMMENTED.name():
                                processComment(indexLookup, prActivity)
                                break
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

                        // increment counters (total and then specific)
                        database.incrementField(indexLookup, TOTAL_PREFIX + prActivityAction.name())
                        JiraDBActions dbActivityAction = (isSelf)
                                ? JiraDBActions.valueOf(SELF_PREFIX + prActivityAction.name()) : prActivityAction
                        database.incrementField(indexLookup, dbActivityAction.name())
                    }
                })
            })
        })
    }

    def processComment(List<FlexiDBQueryColumn> indexLookup, Object prActivity) {
        processComment(indexLookup, prActivity.user.name, prActivity.action, prActivity.commentAction, prActivity.comment, 3)
    }

    def processComment(List<FlexiDBQueryColumn> indexLookup, String userName, String action, String commentAction, Object comment, int indentation) {
        // TODO: distinguish comments on own PR versus others
        String commentText = comment.text
        if (IGNORE_COMMENTS.contains(commentText)) {
            return
        }
        commentText = commentText.replaceAll("(\\r|\\n)?\\n", "  ").trim()
        comment.comments.forEach(replyComment -> {
            processComment(indexLookup, replyComment.author.name, "COMMENTED", "REPLY", replyComment, indentation + 3)
        })
    }

    static List<AbstractFlexiDBInitColumn> generateDBSignature() {
        List<AbstractFlexiDBInitColumn> columns = new ArrayList<>()

        // Searchable / index columns
        DBIndexData.values().each { index -> {
            columns.add(new FlexiDBInitIndexColumn(index.name()))
        }}

        // JiraDB Actions to the data columns
        JiraDBActions.values().each { action -> {
            columns.add(new FlexiDBInitDataColumn(action.name(), action.getDefaultValue()))
        }}

        // Relevant Jira Data
        DBData.values().each { data -> {
            columns.add(new FlexiDBInitDataColumn(data.name(), data.getDefaultValue()))
        }}

        // other data columns
        columns.add(new FlexiDBInitDataColumn(DB_COLUMN_COMMENTS, Collections.emptyList()))

        return columns
    }
}