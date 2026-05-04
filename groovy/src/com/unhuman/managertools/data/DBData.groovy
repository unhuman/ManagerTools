package com.unhuman.managertools.data

enum DBData {
    START_DATE("startDate", null),
    END_DATE("endDate", null),
    AUTHOR("author", null),
    COMMENTS("comments", Collections.emptyList()),
    OTHERS_COMMENTS(null, Collections.emptyList()),
    COMMIT_MESSAGES(null, Collections.emptyList())

    private String jiraField
    private Object defaultValue

    private DBData(String jiraField, Object defaultValue) {
        this.jiraField = jiraField
        this.defaultValue = defaultValue
    }

    String getJiraField() {
        return jiraField
    }

    Object getDefaultValue() {
        return defaultValue
    }
}
