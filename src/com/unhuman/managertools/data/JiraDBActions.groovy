package com.unhuman.managertools.data

enum JiraDBActions {
    // Code Changes
    PR_ADDED(0),
    PR_REMOVED(0),

    COMMIT_ADDED(0),
    COMMIT_REMOVED(0),

    // Activities on PRs
    APPROVED(0),
    COMMENTED(0),
    DECLINED(0),
    MERGED(0),
    OPENED(0),
    RESCOPED(0),
    UNAPPROVED(0),
    UPDATED(0)

    private Object defaultValue

    private JiraDBActions(Object defaultValue) {
        this.defaultValue = defaultValue
    }

    Object getDefaultValue() {
        return defaultValue
    }

    static JiraDBActions getResolvedValue(String desiredAction) {
        try {
            return JiraDBActions.valueOf(desiredAction)
        } catch (Exception e) {
            System.out.println("   Unknown Jira action: ${desiredAction}")
            return null
        }
    }
}
