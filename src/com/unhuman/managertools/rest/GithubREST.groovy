package com.unhuman.managertools.rest

import com.unhuman.managertools.data.UserActivity
@Grapes([
        @Grab(group='org.apache.httpcomponents.core5', module='httpcore5', version='5.2.1'),
        @Grab(group='org.apache.httpcomponents.client5', module='httpclient5', version='5.2.1')
])

import com.unhuman.managertools.rest.exceptions.RESTException
import com.unhuman.managertools.util.CommandLineHelper
import org.apache.hc.core5.http.HttpStatus
import org.apache.hc.core5.http.NameValuePair
import org.apache.hc.core5.http.message.BasicNameValuePair

import java.time.Instant
import java.util.regex.Pattern

class GithubREST extends SourceControlREST {
    private static final String STARTING_PAGE = "0"
    private static final String PAGE_SIZE_LIMIT = "100"
    private static final Pattern JIRA_NAME_PATTERN = Pattern.compile(/\w.*/)

    private CommandLineHelper commandLineHelper

    GithubREST(CommandLineHelper commandLineHelper, String bearerToken) {
        super(new AuthInfo(AuthInfo.AuthType.Bearer, bearerToken))
        this.commandLineHelper = commandLineHelper
    }

    @Override
    String apiConvert(String prUrl) {
        return prUrl
                .replace("://github.com/", "://api.github.com/repos/")
                .replace("/pull/", "/pulls/")
    }

    // Get activities (approvals, comments, etc)
    Object getActivities(String prUrl) {
        List<Object> activitiesList = new ArrayList<>()
        activitiesList.addAll(getComments(prUrl))
        activitiesList.addAll(getReviews(prUrl))
        return activitiesList
    }

    protected ArrayList<Object> getComments(String prUrl) {
        String uri = "${prUrl}/comments"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        NameValuePair markupPair = new BasicNameValuePair("markup", "true")

        Object activities = getRequest(uri, startPair, limitPair, markupPair)

        // make this data look the same as bitbucket
        List<Object> comments = new ArrayList<>()
        for (int i = activities.values.size() - 1; i >= 0; i--) {
            def activity = (activities instanceof List) ? activities[i] : activities.values.get(i)
            activity.user.name = mapUserToJiraName(activity.user)

            activity.createdDate = Instant.parse(activity.created_at).getEpochSecond() * 1000 // ms

            // Determine the activity type
            // see: https://docs.github.com/en/graphql/reference/enums#commentauthorassociation
            if (activity.author_association in ["CONTRIBUTOR", "COLLABORATOR",
                                                "FIRST_TIMER", "FIRST_TIME_CONTRIBUTOR",
                                                "MEMBER", "OWNER"] && activity.body != null) {
                activity.action = UserActivity.COMMENTED.name()
                activity.comment = new HashMap<>()
                activity.comment.text = activity.body
                comments.add(activity)
            }
        }

        return comments
    }

    protected ArrayList<Object> getReviews(String prUrl) {
        String uri = "${prUrl}/reviews"
        NameValuePair startPair = new BasicNameValuePair("start", STARTING_PAGE)
        NameValuePair limitPair = new BasicNameValuePair("limit", PAGE_SIZE_LIMIT)
        NameValuePair markupPair = new BasicNameValuePair("markup", "true")

        Object activities = getRequest(uri, startPair, limitPair, markupPair)

        // make this data look the same as bitbucket
        List<Object> reviews = new ArrayList<>()
        for (int i = activities.values.size() - 1; i >= 0; i--) {
            def activity = (activities instanceof List) ? activities[i] : activities.values.get(i)

            activity.user.name = mapUserToJiraName(activity.user)

            activity.createdDate = Instant.parse(activity.submitted_at).getEpochSecond() * 1000 // ms

            // Determine the activity type
            // see: https://docs.github.com/en/graphql/reference/enums#commentauthorassociation
            if (activity.author_association in ["CONTRIBUTOR", "COLLABORATOR",
                                                "FIRST_TIMER", "FIRST_TIME_CONTRIBUTOR",
                                                "MEMBER", "OWNER"] && activity.body != null) {
                if (activity.state in ["APPROVED", "DISMISSED"]) {
                    activity.action = activity.state
                    reviews.add(activity)
                }
            }
        }

        return reviews
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
                String userName = mapUserToJiraName(commit?.author)
                if (userName == null) {
                    userName = (commit.committer != null) ? commit.committer.name : null
                }
                if (userName == null) {
                    // nested commit.commit - madness
                    if (commit.commit != null) {
                        userName = commit.commit.author.name
                    }
                }

                try {
                    if (commit.committer == null) {
                        commit.committer = new HashMap<>()
                    }
                    commit.committer.name = mapUserToJiraName(userName)
                } catch (Exception e) {
                    System.err.println(e)
                }
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
            if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND
                    && re.statusCode != HttpStatus.SC_INTERNAL_SERVER_ERROR) {
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

    @Override
    String mapUserToJiraName(Object userData) {
        // Preserve null
        if (userData == null) {
            return null
        }

        // If this entity is already a String, just use it.
        if (userData instanceof String) {
            return userData
        }

        // all references to name will be lowercase
        String jiraName = (userData.login != null)
                ? userData.login
                : userData.url.toLowerCase().substring(userData.url.lastIndexOf("/") + 1)

        if (!jiraName.contains("_")) {
            // seeing some randomized IDs... so we need to strip them out
            if (jiraName.length() >= 39) {
                System.err.println("Could not identify user - expected '_' separator missing from ${jiraName} (type: ${userData.type})")
                return null
            }
        } else {
            jiraName = jiraName.substring(0, jiraName.lastIndexOf("_")) // cuts after last _ which should include enterprise name
        }

        return jiraName
    }
}
