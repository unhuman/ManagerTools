package com.unhuman.managertools

@Grapes([
        @Grab(group='org.spockframework', module='spock-core', version='2.3-groovy-4.0'),
        @Grab(group='net.bytebuddy', module='byte-buddy', version='1.12.20'),
        @Grab(group='net.bytebuddy', module='byte-buddy-agent', version='1.12.20'),
        @Grab(group='org.objenesis', module='objenesis', version='3.2')
])

import spock.lang.Specification
import spock.lang.Unroll

// Import the necessary classes
import com.unhuman.managertools.rest.JiraREST
import com.unhuman.managertools.rest.SourceControlREST
import com.unhuman.flexidb.FlexiDB

import java.text.SimpleDateFormat
import java.text.ParseException

class SprintReportTeamAnalysisSpec extends Specification {
    def sprintReportTeamAnalysis = new SprintReportTeamAnalysis()

    def setup() {
        // Initialize necessary mocks and dependencies
        sprintReportTeamAnalysis.jiraREST = Mock(JiraREST)
        sprintReportTeamAnalysis.githubREST = Mock(SourceControlREST)
        sprintReportTeamAnalysis.bitbucketREST = Mock(SourceControlREST)
        sprintReportTeamAnalysis.database = Mock(FlexiDB)
    }

    @Unroll
    def "test getIssueCategoryInformation with sprint: #sprint and issues: #issueList"() {
        given:
        sprintReportTeamAnalysis.processedItems = new HashSet<>()
        sprintReportTeamAnalysis.jiraREST.getTicketPullRequestInfo(_) >> { issueId ->
            return pullRequests
        }
        sprintReportTeamAnalysis.githubREST.getActivities(_) >> { prUrl ->
            return prActivities
        }
        sprintReportTeamAnalysis.githubREST.getCommits(_) >> { prUrl ->
            return prCommits
        }
        sprintReportTeamAnalysis.githubREST.getCommitDiffs(_, _) >> { prUrl, commitSHA ->
            return diffsResponse
        }
        sprintReportTeamAnalysis.githubREST.getDiffs(_) >> { prUrl ->
            return diffsResponse
        }

        when:
        sprintReportTeamAnalysis.getIssueCategoryInformation(sprint, issueList)

        then:
        noExceptionThrown()

        where:
        sprint = [name: "Sprint 1", startDate: "01/Jan/2023 12:00 AM", endDate: "31/Jan/2023 11:59 PM"]
        issueList = [[key: "ISSUE-1", id: 1], [key: "ISSUE-2", id: 2]]
        pullRequests = [[url: "https://github.com/repo/pr/1", id: "#1", author: [name: "author1"]]]
        prActivities = [values: [[user: [name: "user1"], action: "COMMENTED", createdDate: new Date().time]]]
        prCommits = [values: [[id: "commit1", committerTimestamp: new Date().time, committer: [name: "committer1"], message: "commit message"]]]
        diffsResponse = [additions: 10, deletions: 5, diffs: []]
    }

    private String formatDate(Date date) {
        return new SimpleDateFormat("dd/MMM/yy h:mm a", Locale.US).format(date)
    }

    private Date parseDate(String dateStr) throws ParseException {
        return new SimpleDateFormat("dd/MMM/yy h:mm a", Locale.US).parse(dateStr)
    }
}