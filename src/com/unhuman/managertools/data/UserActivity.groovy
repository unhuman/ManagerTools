package com.unhuman.managertools.data

import com.unhuman.flexidb.FlexiDB

enum UserActivity {
    // Code Changes
    PR_ADDED(0),
    PR_REMOVED(0),

    COMMITS(0),
    COMMIT_ADDED(0),
    COMMIT_REMOVED(0),

    // Activities on PRs
    APPROVED(0),
    COMMENTED(0), // this gets removed b/c data winds up in COMMENTS_OWN & COMMENTS_ON_OTHERS
    COMMENTED_ON_SELF(0),
    COMMENTED_ON_OTHERS(0),
    OTHERS_COMMENTED(FlexiDB.EMPTY_INCREMENTOR),
    DECLINED(0),
    MERGED(0),
    OPENED(0),
    RESCOPED(0),
    UNAPPROVED(0),
    UPDATED(0),

    private Object defaultValue

    private UserActivity(Object defaultValue) {
        this.defaultValue = defaultValue
    }

    Object getDefaultValue() {
        return defaultValue
    }

    static UserActivity getResolvedValue(String desiredAction) {
        try {
            return UserActivity.valueOf(desiredAction)
        } catch (Exception e) {
            System.err.println("Unknown action: ${desiredAction}")
            return null
        }
    }
}
