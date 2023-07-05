package com.unhuman.managertools.rest

import com.unhuman.managertools.rest.exceptions.RESTException
import org.apache.hc.core5.http.HttpStatus
@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicNameValuePair

import java.util.regex.Matcher
import java.util.regex.Pattern

class GithubREST extends SourceControlREST {
    private static final String STARTING_PAGE = "0"
    private static final String PAGE_SIZE_LIMIT = "100"

    AuthInfo authInfo

    GithubREST(String bearerToken) {
        this.authInfo = new AuthInfo(AuthInfo.AuthType.Bearer, bearerToken)
    }

    @Override
    String apiConvert(String prUrl) {
        return prUrl
                .replace("://github.com/", "://api.github.com/repos/")
                .replace("/pull/", "/pulls/")
    }

    // Get activities (approvals, comments, etc)
    Object getActivities(String prUrl) {
        String uri = "${prUrl}/comments"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        NameValuePair markupPair = new BasicNameValuePair("markup", "true")
        return RestService.GetRequest(uri, authInfo, startPair, limitPair, markupPair)
    }

    // Get Commits
    Object getCommits(String prUrl) {
        String uri = "${prUrl}/commits"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        try {
            return RestService.GetRequest(uri, authInfo, startPair, limitPair)
        } catch (RESTException re) {
            if (re.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
                throw re
            }
            System.err.println("Unable to retrieve commits ${re.toString()}")
            return null
        }
    }

    // Get commit information

    // Get commit changes

    // Get Pull Request (PR) diffs
    Object getDiffs(String prUrl) {
        String uri = "${prUrl}/diff"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        NameValuePair contextPair = new BasicNameValuePair("contextLines", "0")

        try {
            return RestService.GetRequest(uri, authInfo, startPair, limitPair, contextPair)
        } catch (RESTException re) {
            if (re.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
                throw re
            }
            System.err.println("Unable to retrieve diffs ${re.toString()}")
            return null
        }
    }

    // Get commit diffs
    Object getCommitDiffs(String commitUrl, String commitSHA) {
        // Do some validation
        String commitEnding = "/commits/${commitSHA}"
        if (!commitUrl.endsWith(commitEnding)) {
            throw new RuntimeException("Invalid commitUrl ${commitUrl} not matching SHA: ${commitSHA}")
        }

        Object commitData = RestService.GetRequest(commitUrl, authInfo)
        return commitData
    }
}
