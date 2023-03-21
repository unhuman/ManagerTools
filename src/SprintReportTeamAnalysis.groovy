import groovy.cli.commons.OptionAccessor

class SprintReportTeamAnalysis extends AbstractSprintReport {
    @Override
    def process(OptionAccessor commandLineOptions, String boardId, List<String> sprintIds) {
        sprintIds.each(sprintId -> {
            Object data = jiraREST.getSprintReport(boardId, sprintId)
            System.out.println(data.sprint.name)

            // Gather ticket data for completed and incomplete work
            getIssueCategoryInformation(data.contents.completedIssues)
            getIssueCategoryInformation(data.contents.issuesNotCompletedInCurrentSprint)
        })
    }

    def getIssueCategoryInformation(issueList) {
        issueList.each(issue -> {
            def ticket = issue.key
            def issueId = issue.id

            def prInfo = jiraREST.getTicketPullRequestInfo(issueId.toString())
            prInfo.detail.each(prDetail -> {
                System.out.println("${ticket} / ${issueId}  has ${prDetail.pullRequests.size()} PRs")
                prDetail.pullRequests.each(pullRequest -> {
                    // can get approvers out of ^^^
                    // check comment count before polling for comments
                    def prId = (pullRequest.id.startsWith("#") ? pullRequest.id.substring(1) : pullRequest.id)
                    def prAuthor = pullRequest.author
                    def prUrl = pullRequest.url

                    def prActivities = bitbucketREST.getActivities(prUrl)
                    System.out.println("   PR ${prId} has ${prActivities.values.size()} activities")
                })
            })
        })
    }
}