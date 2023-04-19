import groovy.cli.commons.OptionAccessor
import org.apache.commons.codec.binary.StringUtils

class SprintReportTeamAnalysis extends AbstractSprintReport {
    final List<String> IGNORE_USERS = [ "codeowners" ]
    final List<String> IGNORE_COMMENTS = [ "Tasks to Complete Before Merging Pull Request" ]
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
                System.out.println("${ticket} / Issue ${issueId} has ${prDetail.pullRequests.size()} PRs")
                prDetail.pullRequests.each(pullRequest -> {
                    // can get approvers out of ^^^
                    // check comment count before polling for comments
                    def prId = (pullRequest.id.startsWith("#") ? pullRequest.id.substring(1) : pullRequest.id)
                    def prAuthor = pullRequest.author
                    def prUrl = pullRequest.url

                    def prActivities = bitbucketREST.getActivities(prUrl)
                    System.out.println("   PR ${ticket} / ${prId} has ${prActivities.values.size()} activities")
                    for (Object prActivity: prActivities.values) {
                        String userName = prActivity.user.name
                        if (IGNORE_USERS.contains(userName)) {
                            continue
                        }
                        String prActivityAction = prActivity.action
                        switch (prActivityAction) {
                            case "APPROVED":
                                System.out.println("   ${userName} ${prActivityAction}")
                                continue
                            case "COMMENTED":
                                processComment(userName, prActivity)
                                continue
                            case "DECLINED":
                                // TODO Track this
                                continue
                            case "OPENED":
                                System.out.println("   ${userName} ${prActivityAction}")
                                continue
                            case "MERGED":
                                System.out.println("   ${userName} ${prActivityAction}")
                                continue
                            case "RESCOPED":
                                // do nothing
                                continue
                            case "UNAPPROVED":
                                // TODO Revisit - do nothing?
                                continue
                            case "UPDATED":
                                // do nothing
                                continue
                            // TODO: Needs Work would be great to capture
                            default:
                                System.out.println("   ${userName} Unhandled action: ${prActivityAction}")
                        }
                    }
                })
            })
        })
    }

    def processComment(String userName, Object prActivity) {
        processComment(userName, prActivity.action, prActivity.commentAction, prActivity.comment, 3)
    }

    def processComment(String userName, String action, String commentAction, Object comment, int indentation) {
        // TODO: distinguish comments on own PR versus others
        String commentText = comment.text
        if (IGNORE_COMMENTS.contains(commentText)) {
            return
        }
        commentText = commentText.replaceAll("(\\r|\\n)?\\n", "  ").trim()
        System.out.println("${org.apache.ivy.util.StringUtils.repeat(' ', indentation)}${userName} ${action} ${commentAction}: ${commentText}")
        comment.comments.forEach(replyComment -> {
            processComment(replyComment.author.name, "COMMENTED", "REPLY", replyComment, indentation + 3)
        })
    }
}