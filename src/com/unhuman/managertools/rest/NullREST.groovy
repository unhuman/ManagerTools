package com.unhuman.managertools.rest;

class NullREST extends SourceControlREST{
    NullREST(String sourceControlType) {
        super(null)
        System.err.println("No " + sourceControlType + " unavailable - using empty responses.")
    }

    @Override
    Object getActivities(String prUrl) {
        return Collections.emptyList()
    }

    @Override
    Object getCommits(String prUrl) {
        return Collections.emptyList()
    }

    @Override
    Object getDiffs(String prUrl) {
        return Collections.emptyList()
    }

    @Override
    Object getCommitDiffs(String prUrl, String commitSHA) {
        return Collections.emptyList()
    }

    @Override
    String mapNameToJiraName(String name) {
        return null
    }
}
