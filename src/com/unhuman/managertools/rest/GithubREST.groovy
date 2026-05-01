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
        List<String> errors = new ArrayList<>(2)
        List<Object> activitiesList = new ArrayList<>()
        try {
            activitiesList.addAll(getComments(prUrl))
        } catch (RESTException re) {
            if (re.statusCode != HttpStatus.SC_NOT_FOUND && re.statusCode != HttpStatus.SC_FORBIDDEN) {
                throw re
            }
            errors.add("Comments (${re.statusCode})")
        }
        try {
            activitiesList.addAll(getReviews(prUrl))
        } catch (RESTException re) {
            if (re.statusCode != HttpStatus.SC_NOT_FOUND && re.statusCode != HttpStatus.SC_FORBIDDEN) {
                throw re
            }
            errors.add("Reviews (${re.statusCode})")
        }

        if (errors.size() > 0) {
            String errorMessage = "Unable to retrieve activities: ${errors.join(", ")}"
//            commandLineHelper.printError(errorMessage)
        }

        return activitiesList
    }

    protected ArrayList<Object> getComments(String prUrl) {
        String uri = "${prUrl}/comments"

        List<Object> allActivities = new ArrayList<>()
        int page = 1
        while (true) {
            NameValuePair pagePair = new BasicNameValuePair("page", String.valueOf(page))
            NameValuePair perPagePair = new BasicNameValuePair("per_page", PAGE_SIZE_LIMIT)
            Object response = getRequest(uri, pagePair, perPagePair)
            List<Object> pageItems = (response instanceof List) ? response : response.values
            if (!pageItems) break
            allActivities.addAll(pageItems)
            if (pageItems.size() < Integer.parseInt(PAGE_SIZE_LIMIT)) break
            page++
        }

        // make this data look the same as bitbucket
        List<Object> comments = new ArrayList<>()
        for (int i = allActivities.size() - 1; i >= 0; i--) {
            def activity = allActivities[i]
            if (activity.user == null) {
                activity.user = [name: "unknown", displayName: "unknown"]
            } else {
                activity.user.name = mapUserToJiraName(activity.user)
            }

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

        List<Object> allActivities = new ArrayList<>()
        int page = 1
        while (true) {
            NameValuePair pagePair = new BasicNameValuePair("page", String.valueOf(page))
            NameValuePair perPagePair = new BasicNameValuePair("per_page", PAGE_SIZE_LIMIT)
            Object response = getRequest(uri, pagePair, perPagePair)
            List<Object> pageItems = (response instanceof List) ? response : response.values
            if (!pageItems) break
            allActivities.addAll(pageItems)
            if (pageItems.size() < Integer.parseInt(PAGE_SIZE_LIMIT)) break
            page++
        }

        // make this data look the same as bitbucket
        List<Object> reviews = new ArrayList<>()
        for (int i = allActivities.size() - 1; i >= 0; i--) {
            def activity = allActivities[i]

            if (activity.user == null) {
                activity.user = [name: "unknown", displayName: "unknown"]
            } else {
                activity.user.name = mapUserToJiraName(activity.user)
            }

            // skip activities that do not have a time
            if (activity.submitted_at == null) {
                continue
            }

            activity.createdDate = Instant.parse(activity.submitted_at).getEpochSecond() * 1000 // ms

            // Determine the activity type
            // see: https://docs.github.com/en/graphql/reference/enums#commentauthorassociation
            if (activity.author_association in ["CONTRIBUTOR", "COLLABORATOR",
                                                "FIRST_TIMER", "FIRST_TIME_CONTRIBUTOR",
                                                "MEMBER", "OWNER"] && activity.body != null) {
                if (activity.state in ["APPROVED", "DISMISSED"]) {
                    // Remap DISMISSED to DECLINED here; the switch in SprintReportTeamAnalysis
                    // is never reached because getResolvedValue("DISMISSED") returns null first
                    activity.action = (activity.state == "DISMISSED") ? UserActivity.DECLINED.name() : activity.state
                    reviews.add(activity)
                }
            }
        }

        return reviews
    }

    // Get Commits
    Object getCommits(String prUrl) {
        String uri = "${prUrl}/commits"
        try {
            List<Object> allCommits = new ArrayList<>()
            int page = 1
            while (true) {
                NameValuePair pagePair = new BasicNameValuePair("page", String.valueOf(page))
                NameValuePair perPagePair = new BasicNameValuePair("per_page", PAGE_SIZE_LIMIT)
                Object response = getRequest(uri, pagePair, perPagePair)
                List<Object> pageItems = (response instanceof List) ? response : response.values
                if (!pageItems) break
                allCommits.addAll(pageItems)
                if (pageItems.size() < Integer.parseInt(PAGE_SIZE_LIMIT)) break
                page++
            }

            // make the data look like bitbucket
            for (int i = allCommits.size() - 1; i >= 0; i--) {
                def commit = allCommits[i]
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

            // Return Bitbucket-compatible structure so SprintReportTeamAnalysis iterates via .values
            return [values: allCommits]
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

        try {
            return getRequest(commitUrl)
        } catch (RESTException re) {
            if (re.statusCode != HttpStatus.SC_FORBIDDEN && re.statusCode != HttpStatus.SC_NOT_FOUND) {
                throw re
            }
            System.err.println("Unable to retrieve commit diffs ${re.toString()}")
            return null
        }
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

        String jiraName = userData.login
        if (jiraName == null && userData.url != null) {
            jiraName = userData.url.toLowerCase().substring(userData.url.lastIndexOf("/") + 1)
        }

        if (jiraName == null) {
            return null
        }

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
