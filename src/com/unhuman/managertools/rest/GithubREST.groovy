package com.unhuman.managertools.rest

import com.unhuman.managertools.data.UserActivity
@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import com.unhuman.managertools.rest.exceptions.RESTException
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicNameValuePair

import java.lang.reflect.Array

class GithubREST extends SourceControlREST {
    private static final String STARTING_PAGE = "0"
    private static final String PAGE_SIZE_LIMIT = "100"

    GithubREST(String bearerToken) {
        super(new AuthInfo(AuthInfo.AuthType.Bearer, bearerToken))
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

        Object activities = getRequest(uri, startPair, limitPair, markupPair)

        // make this data look the same as bitbucket
        for (int i = activities.values.size() - 1; i >= 0; i--) {
            def activity = (activities instanceof List) ? activities[0] : activities.values.get(i)
            activity.user.name = activity.user.login

            activity.createdDate = java.time.Instant.parse(activity.created_at).getEpochSecond() * 1000 // ms

            // Determine the activity type
            if (activity.author_association in ["CONTRIBUTOR", "COLLABORATOR"] && activity.body != null) {
                activity.action = UserActivity.COMMENTED.name()
                activity.comment = new HashMap<>()
                activity.comment.text = activity.body
            } else {
                activity.action = null
            }
        }

        return activities
    }

    // Get Commits
    Object getCommits(String prUrl) {
        String uri = "${prUrl}/commits"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        try {
            Object commits = getRequest(uri, startPair, limitPair)

            // make the data look like bitbucket
            for (int i = commits.values.size() - 1; i >= 0; i--) {
                def commit = (commits instanceof List) ? commits.get(i) : commits.values.get(i)
                commit.id = commit.sha
                commit.committerTimestamp = java.time.Instant.parse(commit.commit.committer.date).getEpochSecond() * 1000 // ms
                commit.message = commit.commit.message

                // Github does funny things - so we have to determine userName
                // TODO - this thing might not exist
                String userName = (commit.committer != null) ? commit.committer.name : null
                if (userName == null) {
                    // nested commit.commit - madness
                    if (commit.commit != null) {
                        userName = commit.commit.author.name
                    }
                }
                if (userName == null) {
                    if (commit.author != null) {
                        userName = commit.author.login
                    }
                }

                commit.committer.name = userName
            }

            return commits
        } catch (RESTException re) {
            if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND) {
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
        String uri = "${prUrl}"

        try {
            return getRequest(uri)
        } catch (RESTException re) {
            if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND) {
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

        Object commitData = getRequest(commitUrl)
        return commitData
    }
}
