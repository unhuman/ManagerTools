package com.unhuman.managertools.rest

@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import com.unhuman.managertools.rest.exceptions.RESTException
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicNameValuePair

import java.util.regex.Matcher
import java.util.regex.Pattern

// The data format by bitbucket is the reference implementation
// other implementations of Source Control must convert their data to look like BitBucket

class BitbucketREST extends SourceControlREST {
    private static final String STARTING_PAGE = "0"
    private static final String PAGE_SIZE_LIMIT = "100"
    private static final Pattern FIND_PROJECT_URL = Pattern.compile("(.*)/pull-requests/[\\d]+")

    String bitbucketServer

    BitbucketREST(String bitbucketServer, String username, String password) {
        super(new AuthInfo(username, password))
        this.bitbucketServer = bitbucketServer

//        String url = "${bitbucketServer}/site/oauth2/access_token"
//        // -d grant_type=authorization_code
    }

    BitbucketREST(String bitbucketServer, String cookies) {
        super(new AuthInfo(AuthInfo.AuthType.Cookies, cookies))
        this.bitbucketServer = bitbucketServer
    }

    // Get activities (approvals, comments, etc)
    // https://{bitbucket}/rest/api/latest/projects/{project}/repos/{repo}/pull-requests/{prId}/activities?avatarSize=48&start=0&limit=25&markup=true
    Object getActivities(String prUrl) {
        String uri = "${prUrl}/activities"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        NameValuePair markupPair = new BasicNameValuePair("markup", "true")
        return getRequest(uri, startPair, limitPair, markupPair)
    }

    // Get Commits
    // https://{bitbucket}/rest/api/latest/projects/{project}/repos/{repo}/pull-requests/115/commits
    Object getCommits(String prUrl) {
        String uri = "${prUrl}/commits"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        try {
            return getRequest(uri, startPair, limitPair)
        } catch (RESTException re) {
            if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND) {
                throw re
            }
            System.err.println("Unable to retrieve commits ${re.toString()}")
            return null
        }
    }

    // Get commit information https://{bitbucket}/rest/api/latest/projects/{project}/repos/{repo}/commits/{commitSHA}

    // Get commit changes https://{bitbucket}/rest/api/latest/projects/{project}/repos/{repo}/commits/{commitSHA}/changes

    // Get Pull Request (PR) diff: https://{bitbucket}}/rest/api/latest/projects/{project}}/repos/{repo}}/pull-requests/{prID}/diff
    Object getDiffs(String prUrl) {
        String uri = "${prUrl}/diff"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        NameValuePair contextPair = new BasicNameValuePair("contextLines", "0")
        NameValuePair ignoreWhitespacePair = new BasicNameValuePair("whitespace", "ignore-all")

        try {
            return getRequest(uri, startPair, limitPair, contextPair, ignoreWhitespacePair)
        } catch (RESTException re) {
            if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND) {
                throw re
            }
            System.err.println("Unable to retrieve diffs ${re.toString()}")
            return null
        }
    }

    // Get commit diff: https://{bitbucket}}/rest/api/latest/projects/{project}/repos/{repo}/commits/{commitSHA}/diff?contextLines=0
    Object getCommitDiffs(String prUrl, String commitSHA) {
        Matcher projectUrlMatcher = FIND_PROJECT_URL.matcher(prUrl)
        // Trim off PR info to get project/repo path
        if (!projectUrlMatcher.matches()) {
            throw new RuntimeException("Couldn't extract Project URL from: ${prUrl}")
        }
        String uri = "${projectUrlMatcher.group(1)}/commits/${commitSHA}/diff"

        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        NameValuePair contextPair = new BasicNameValuePair("contextLines", "0")
        NameValuePair ignoreWhitespacePair = new BasicNameValuePair("whitespace", "ignore-all")

        return getRequest(uri, startPair, limitPair, contextPair, ignoreWhitespacePair)
    }

    @Override
    String mapNameToJiraName(String name) {
        // Bitbucket is the same as Jira, so do nothing
        return name
    }
}
