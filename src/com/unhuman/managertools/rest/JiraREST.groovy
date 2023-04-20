package com.unhuman.managertools.rest

import org.apache.http.NameValuePair
import org.apache.http.message.BasicNameValuePair

class JiraREST {
    String jiraServer
    String cookies

    private final int JQL_LIMIT = 250

    JiraREST(String jiraServer, String cookies) {
        this.jiraServer = jiraServer
        this.cookies = cookies
    }

    // Get Sprints
    // https://jira.x.com/rest/greenhopper/1.0/sprintquery/679?includeHistoricSprints=false&includeFutureSprints=false&_=1679081186798
    Object getSprints(String boardId) {
        String uri = "https://${jiraServer}/rest/greenhopper/1.0/sprintquery/${boardId}"
        NameValuePair rapidViewIdPair = new BasicNameValuePair("includeHistoricSprints", "false")
        NameValuePair sprintIdPair = new BasicNameValuePair("includeFutureSprints", "false")
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(uri, cookies, rapidViewIdPair, sprintIdPair, timeIdPair)
    }

    // Get Sprint Report
    // https://jira.x.com/rest/greenhopper/1.0/rapid/charts/sprintreport?rapidViewId=679&sprintId=26636&_=1679081186799
    Object getSprintReport(String boardId, String sprintId) {
        String uri = "https://${jiraServer}/rest/greenhopper/1.0/rapid/charts/sprintreport"
        NameValuePair rapidViewIdPair = new BasicNameValuePair("rapidViewId", boardId)
        NameValuePair sprintIdPair = new BasicNameValuePair("sprintId", sprintId)
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(uri, cookies, rapidViewIdPair, sprintIdPair, timeIdPair)
    }

    //static Object getSprintReport(String user, String password, String boardId, String sprintId) {
    //    String uri = "https://${jiraServer}/rest/greenhopper/1.0/rapid/charts/sprintreport"
    //    NameValuePair rapidViewIdPair = new BasicNameValuePair("rapidViewId", boardId)
    //    NameValuePair sprintIdPair = new BasicNameValuePair("sprintId", sprintId)
    //    NameValuePair timeIdPair = new BasicNameValuePair("_", "1679335101555")
    //    return RestService.GetRequest(uri, user, password, rapidViewIdPair, sprintIdPair, timeIdPair)
    //}

    // get ticket info
    // https://jira.x.com/rest/api/latest/issue/ISSUE-ID
    Object getTicket(String ticketId) {
        String uri = "https://${jiraServer}/rest/api/latest/issue/${ticketId}"
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(uri, cookies, timeIdPair)
    }

    // Get pull request data
    // https://jira.x.com/rest/dev-status/1.0/issue/detail?issueId=3699559&applicationType=stash&dataType=pullrequest&_=1679440827558
    Object getTicketPullRequestInfo(String issueId) {
        String uri = "https://${jiraServer}/rest/dev-status/1.0/issue/detail"
        NameValuePair issueIdPair = new BasicNameValuePair("issueId", issueId)
        NameValuePair applicationTypePair = new BasicNameValuePair("applicationType", "stash")
        NameValuePair dataTypePair = new BasicNameValuePair("dataType", "pullrequest")
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return RestService.GetRequest(uri, cookies, issueIdPair, applicationTypePair, dataTypePair, timeIdPair)
    }

    // Simple JQL Query (Summary)
    // GET https://jira.x.com/rest/api/2/search?jql=summary~q1%20and%20summary~yellow
    Object jqlSummaryQuery(String jql) {
        String uri = "https://${jiraServer}/rest/api/2/search?startAt=0&maxResults=${JQL_LIMIT}&jql=${URLEncoder.encode(jql)}"
        return RestService.GetRequest(uri, cookies)
    }

    // https://jira.x.com/rest/agile/1.0/issue/ISSUE-ID/estimation?boardId=BOARD-ID
    // https://developer.atlassian.com/cloud/jira/software/rest/api-group-issue/#api-rest-agile-1-0-issue-issueidorkey-estimation-put
    Object updateOriginalEstimate(String ticketId, String boardId, Long estimateInSeconds) {
        String uri = "https://${jiraServer}/rest/agile/1.0/issue/${ticketId}/estimation"
        NameValuePair boardIdPair = new BasicNameValuePair("boardId", boardId)
        String content = "{ \"value\": \"${estimateInSeconds / 60}m\"}" // API requires converted to minutes
        return RestService.PutRequest(uri, cookies, content, boardIdPair)
    }
}
