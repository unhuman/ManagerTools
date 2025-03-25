package com.unhuman.managertools.rest

@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import com.unhuman.managertools.rest.exceptions.RESTException
import org.apache.groovy.json.internal.LazyMap
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicNameValuePair

class JiraREST extends RestService {
    String jiraServer

    private final int JQL_LIMIT = 250

    JiraREST(String jiraServer, String username, String password) {
        super(new AuthInfo(username, password))
        this.jiraServer = jiraServer
    }

    JiraREST(String jiraServer, String auth) {
        super(new AuthInfo((auth.contains('=')) ? AuthInfo.AuthType.Cookies : AuthInfo.AuthType.Bearer, auth))
        this.jiraServer = jiraServer
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
            response = getRequest(uri, rapidViewIdPair, sprintIdPair, timeIdPair)
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
        return getRequest(uri, rapidViewIdPair, sprintIdPair, timeIdPair)
    }

    // get ticket info
    // https://jira.x.com/rest/api/latest/issue/ISSUE-ID
    Object getTicket(String ticketId) {
        String uri = "https://${jiraServer}/rest/api/latest/issue/${ticketId}"
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())
        return getRequest(uri, timeIdPair)
    }

    // Get pull request data
    // https://jira.x.com/rest/dev-status/1.0/issue/detail?issueId=3699559&applicationType=stash&dataType=pullrequest&_=1679440827558
    Object getTicketPullRequestInfo(String issueId) {
        String uri = "https://${jiraServer}/rest/dev-status/1.0/issue/detail"
        NameValuePair issueIdPair = new BasicNameValuePair("issueId", issueId)

        NameValuePair dataTypePair = new BasicNameValuePair("dataType", "pullrequest")
        NameValuePair timeIdPair = new BasicNameValuePair("_", System.currentTimeMillis().toString())

        // TODO: Make these async

        // combine all PR data
        List<Object> pullRequests = new ArrayList<>();

        // Make request for Stash data
        NameValuePair applicationTypePair = new BasicNameValuePair("applicationType", "stash")
        try {
            LazyMap stashData = (LazyMap) getRequest(uri, issueIdPair, dataTypePair, timeIdPair, applicationTypePair)

            // Temporary
            if (stashData.detail.pullRequests == null) {
                System.err.print("No pull requests 3 found for ${issueId}")
            }
            if (stashData.detail.pullRequests[0] == null) {
                System.err.print("No pull requests 4 found for ${issueId}")
            }
            System.err.flush()

            if (stashData.errors.size() > 0) {
                System.err.println("Error in response: ${stashData.errors}")
            }

            pullRequests.addAll(stashData.detail.pullRequests[0])
        } catch(RESTException re) {
            if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND) {
                throw re
            }
            System.err.println("Unable to retrieve requested url ${re.toString()}")
        }

        // Make request for GitHub data
        applicationTypePair = new BasicNameValuePair("applicationType", "githube")
        try {
            LazyMap githubData = (LazyMap) getRequest(uri, issueIdPair, dataTypePair, timeIdPair, applicationTypePair)
            if (githubData.detail.pullRequests != null && githubData.detail.pullRequests[0] != null) {
                pullRequests.addAll(githubData.detail.pullRequests[0])
            }
        } catch(RESTException re) {
            if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND) {
                throw re
            }
            System.err.println("Unable to retrieve requested url ${re.toString()}")
        }
        return pullRequests
    }

    // Simple JQL Query (Summary)
    // GET https://jira.x.com/rest/api/2/search?jql=summary~q1%20and%20summary~yellow
    Object jqlSummaryQuery(String jql) {
        String uri = "https://${jiraServer}/rest/api/2/search?startAt=0&maxResults=${JQL_LIMIT}&jql=${URLEncoder.encode(jql)}"
        return getRequest(uri)
    }

    // https://jira.x.com/rest/agile/1.0/issue/ISSUE-ID/estimation?boardId=BOARD-ID
    // https://developer.atlassian.com/cloud/jira/software/rest/api-group-issue/#api-rest-agile-1-0-issue-issueidorkey-estimation-put
    Object updateOriginalEstimate(String ticketId, String boardId, Long estimateInSeconds) {
        String uri = "https://${jiraServer}/rest/agile/1.0/issue/${ticketId}/estimation"
        NameValuePair boardIdPair = new BasicNameValuePair("boardId", boardId)
        String content = "{ \"value\": \"${estimateInSeconds / 60}m\"}" // API requires converted to minutes
        return putRequest(uri, content, boardIdPair)
    }
}
