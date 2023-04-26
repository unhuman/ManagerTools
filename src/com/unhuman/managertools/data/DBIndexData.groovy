package com.unhuman.managertools.data

enum DBIndexData {
    SPRINT("sprint"),
    TICKET("ticket"),
    PR_ID("prId"),
    USER("user")

    private String jiraField

    private DBIndexData(String jiraField) {
        this.jiraField = jiraField
    }

    String getJiraField() {
        return jiraField
    }
}
