package com.unhuman.managertools.rest

abstract class SourceControlREST {
    // Get activities (approvals, comments, etc)
    abstract Object getActivities(String prUrl)

    // Get Commits
    abstract Object getCommits(String prUrl)

    // Get Pull Request (PR) diff
    abstract Object getDiffs(String prUrl)

    // Get commit diff: https://{bitbucket}}/rest/api/latest/projects/{project}/repos/{repo}/commits/{commitSHA}/diff?contextLines=0
    abstract Object getCommitDiffs(String prUrl, String commitSHA)

    // Convert an URL provided (presumably by jira) to an api call
    String apiConvert(String prUrl) {
        return prUrl
    }
}