package com.unhuman.managertools.rest

@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicNameValuePair

import java.util.regex.Matcher
import java.util.regex.Pattern

class BitbucketREST {
    private static final String STARTING_PAGE = "0"
    private static final String PAGE_SIZE_LIMIT = "100"
    private static final Pattern FIND_PROJECT_URL = Pattern.compile("(.*)/pull-requests/[\\d]+")


    String bitbucketServer
    AuthInfo authInfo

    BitbucketREST(String bitbucketServer, String username, String password) {
        this.bitbucketServer = bitbucketServer

        // convert user / password to cookies
        this.authInfo = new AuthInfo(username, password)

//        String url = "${bitbucketServer}/site/oauth2/access_token"
//        // -d grant_type=authorization_code
    }

    BitbucketREST(String bitbucketServer, String cookies) {
        this.bitbucketServer = bitbucketServer
        this.authInfo = new AuthInfo(cookies)
    }

    // Get activities (approvals, comments, etc)
    // https://{bitbucket}/rest/api/latest/projects/{project}/repos/{repo}/pull-requests/78/activities?avatarSize=48&start=0&limit=25&markup=true
    Object getActivities(String prUrl) {
        String uri = "${prUrl}/activities"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        NameValuePair markupPair = new BasicNameValuePair("markup", "true")
        return RestService.GetRequest(uri, authInfo, startPair, limitPair, markupPair)
    }

    // Get Commits
    // https://{bitbucket}/rest/api/latest/projects/{project}/repos/{repo}/pull-requests/115/commits
    Object getCommits(String prUrl) {
        String uri = "${prUrl}/commits"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        return RestService.GetRequest(uri, authInfo, startPair, limitPair)
    }

    // Get commit information https://{bitbucket}/rest/api/latest/projects/{project}/repos/{repo}/commits/{commitSHA}

    // Get commit changes https://{bitbucket}/rest/api/latest/projects/{project}/repos/{repo}/commits/{commitSHA}/changes

    // Get commit diff: https://{bitbucket}}/rest/api/latest/projects/{project}/repos/{repo}}/commits/{commitSHA}/diff?contextLines=0
    Object getDiffs(String prUrl, String commitSHA) {
        Matcher projectUrlMatcher = FIND_PROJECT_URL.matcher(prUrl)
        // Trim off PR info to get project/repo path
        if (!projectUrlMatcher.matches()) {
            throw new RuntimeException("Couldn't extract Project URL from: ${prUrl}")
        }
        String repoUri = "${projectUrlMatcher.group(1)}/commits/${commitSHA}/diff"

        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        NameValuePair contextPair = new BasicNameValuePair("contextLines", "0")
        return RestService.GetRequest(repoUri, authInfo, startPair, limitPair, contextPair)
    }
}
