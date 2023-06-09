package com.unhuman.managertools.rest

@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicNameValuePair

class JiraREST {
    String jiraServer
    AuthInfo authInfo

    private final int JQL_LIMIT = 250

    JiraREST(String jiraServer, String username, String password) {
        this.jiraServer = jiraServer

        // convert user / password to cookies
        this.authInfo = new AuthInfo(username, password)
    }

    JiraREST(String jiraServer, String cookies) {
        this.jiraServer = jiraServer
        this.authInfo = new AuthInfo(cookies)
    }

    // Get Sprints v2
    // https://jira.x.com/rest/agile/1.0/board/679/sprint
    List<Object> getSprints(String boardId) {
        int startAt = 0
        List<Object> values = new ArrayList<>()
        def response
        do {
            String uri = "https://${jiraServer}/rest/agile/1.0/board/${boardId}/sprint"
            NameValuePair rapidViewIdPair = new BasicNameValuePair("state", "active,closed")
            NameValuePair sprintIdPair = new BasicNameValuePair("startAt", startAt.toString())
            NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
            response = RestService.GetRequest(uri, authInfo, rapidViewIdPair, sprintIdPair, timeIdPair)
            values.addAll(response.values)
            startAt += response.maxResults
        } while (!response.isLast)

        // Sort this list by endDate
        Collections.sort(values, new Comparator<Object>() {
            @Override
            int compare(Object o1, Object o2) {
                return o1.endDate.compareTo(o2.endDate)
            }
        })

        return values
    }

    // Get Sprint Report
    // https://jira.x.com/rest/greenhopper/1.0/rapid/charts/sprintreport?rapidViewId=679&sprintId=26636&_=1679081186799
    // an alternative would be: https://jira.X.com/rest/agile/1.0/sprint/26636/issue but it is much slower
    Object getSprintReport(String boardId, String sprintId) {
        String uri = "https://${jiraServer}/rest/greenhopper/1.0/rapid/charts/sprintreport"
        NameValuePair rapidViewIdPair = new BasicNameValuePair("rapidViewId", boardId)
        NameValuePair sprintIdPair = new BasicNameValuePair("sprintId", sprintId)
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(uri, authInfo, rapidViewIdPair, sprintIdPair, timeIdPair)
    }

    // get ticket info
    // https://jira.x.com/rest/api/latest/issue/ISSUE-ID
    Object getTicket(String ticketId) {
        String uri = "https://${jiraServer}/rest/api/latest/issue/${ticketId}"
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(uri, authInfo, timeIdPair)
    }

    // Get pull request data
    // https://jira.x.com/rest/dev-status/1.0/issue/detail?issueId=3699559&applicationType=stash&dataType=pullrequest&_=1679440827558
    Object getTicketPullRequestInfo(String issueId) {
        String uri = "https://${jiraServer}/rest/dev-status/1.0/issue/detail"
        NameValuePair issueIdPair = new BasicNameValuePair("issueId", issueId)
        NameValuePair applicationTypePair = new BasicNameValuePair("applicationType", "stash")
        NameValuePair dataTypePair = new BasicNameValuePair("dataType", "pullrequest")
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(uri, authInfo, issueIdPair, applicationTypePair, dataTypePair, timeIdPair)
    }

    // Simple JQL Query (Summary)
    // GET https://jira.x.com/rest/api/2/search?jql=summary~q1%20and%20summary~yellow
    Object jqlSummaryQuery(String jql) {
        String uri = "https://${jiraServer}/rest/api/2/search?startAt=0&maxResults=${JQL_LIMIT}&jql=${URLEncoder.encode(jql)}"
        return RestService.GetRequest(uri, authInfo)
    }

    // https://jira.x.com/rest/agile/1.0/issue/ISSUE-ID/estimation?boardId=BOARD-ID
    // https://developer.atlassian.com/cloud/jira/software/rest/api-group-issue/#api-rest-agile-1-0-issue-issueidorkey-estimation-put
    Object updateOriginalEstimate(String ticketId, String boardId, Long estimateInSeconds) {
        String uri = "https://${jiraServer}/rest/agile/1.0/issue/${ticketId}/estimation"
        NameValuePair boardIdPair = new BasicNameValuePair("boardId", boardId)
        String content = "{ \"value\": \"${estimateInSeconds / 60}m\"}" // API requires converted to minutes
        return RestService.PutRequest(uri, authInfo, content, boardIdPair)
    }
}
