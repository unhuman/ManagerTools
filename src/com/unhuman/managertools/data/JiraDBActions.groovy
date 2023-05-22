package com.unhuman.managertools.data

enum JiraDBActions {
    // Code Changes
    PR_ADDED(0), PR_SELF_ADDED(0), PR_TOTAL_ADDED(0),
    PR_REMOVED(0), PR_SELF_REMOVED(0), PR_TOTAL_REMOVED(0),
    PR_MODIFIED(0), PR_SELF_MODIFIED(0), PR_TOTAL_MODIFIED(0),

    COMMIT_ADDED(0), COMMIT_SELF_ADDED(0), COMMIT_TOTAL_ADDED(0),
    COMMIT_REMOVED(0), COMMIT_SELF_REMOVED(0), COMMIT_TOTAL_REMOVED(0),
    COMMIT_MODIFIED(0), COMMIT_SELF_MODIFIED(0), COMMIT_TOTAL_MODIFIED(0),

    // Activities on PRs
    APPROVED(0), SELF_APPROVED(0), TOTAL_APPROVED(0),
    COMMENTED(0), SELF_COMMENTED(0), TOTAL_COMMENTED(0),
    DECLINED(0), SELF_DECLINED(0), TOTAL_DECLINED(0),
    MERGED(0), SELF_MERGED(0), TOTAL_MERGED(0),
    OPENED(0), SELF_OPENED(0), TOTAL_OPENED(0),
    RESCOPED(0), SELF_RESCOPED(0), TOTAL_RESCOPED(0),
    UNAPPROVED(0), SELF_UNAPPROVED(0), TOTAL_UNAPPROVED(0),
    UPDATED(0), SELF_UPDATED(0), TOTAL_UPDATED(0)

    private Object defaultValue

    static final int DETAIL_DATA_SKIP_COUNT = 2

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
