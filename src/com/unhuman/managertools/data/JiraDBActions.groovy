package com.unhuman.managertools.data

enum JiraDBActions {
    APPROVED(0), SELF_APPROVED(0), TOTAL_APPROVED(0),
    COMMENTED(0), SELF_COMMENTED(0), TOTAL_COMMENTED(0),
    DECLINED(0), SELF_DECLINED(0), TOTAL_DECLINED(0),
    MERGED(0), SELF_MERGED(0), TOTAL_MERGED(0),
    OPENED(0), SELF_OPENED(0), TOTAL_OPENED(0),
    RESCOPED(0), SELF_RESCOPED(0), TOTAL_RESCOPED(0),
    UNAPPROVED(0), SELF_UNAPPROVED(0), TOTAL_UNAPPROVED(0),
    UPDATED(0), SELF_UPDATED(0), TOTAL_UPDATED(0),

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
