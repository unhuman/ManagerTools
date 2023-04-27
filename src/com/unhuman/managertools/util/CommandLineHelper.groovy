package com.unhuman.managertools.util

import org.codehaus.groovy.util.StringUtil

import java.util.regex.Pattern

class CommandLineHelper {
    private static final Pattern ANY_MATCH_PATTERN = Pattern.compile(".*")
    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}")
    private static final Pattern NUMBERS_REQUIRED_PATTERN = Pattern.compile("\\d+")
    private static final Pattern FQDN_PATTERN = Pattern.compile("(\\w+\\.){2,}\\w+")

    ConfigFileManager configFileManager

    CommandLineHelper(String configFilename) {
        if (configFilename) {
            configFileManager = new ConfigFileManager(configFilename)
        }
    }

    String getUsername() {
        String username = promptAndStore("Username (or press enter for ${System.getProperty("user.name")})", false, ANY_MATCH_PATTERN, null)
        if (username.isEmpty()) {
            username = System.getProperty("user.name")
        }
        return username
    }

    String getPassword() {
        return promptAndStore("Password", true, ANY_MATCH_PATTERN, null);
    }

    String getSprintTeam() {
        return promptAndStore("Sprint Team name", false, ANY_MATCH_PATTERN, "sprintTeam", true)
    }

    String getSprintTeamBoardId() {
        if (!configFileManager.containsKey("sprintTeam")) {
            throw new RuntimeException("Need sprintTeam already found")
        }

        String sprintTeam = configFileManager.getValue("sprintTeam")
        return promptAndStore("Board Id for ${sprintTeam}", false, NUMBERS_REQUIRED_PATTERN, "${sprintTeam}-boardId", true)
    }

    String getJiraServer() {
        return promptAndStore("Jira Server (jira.x.com)", false, FQDN_PATTERN, "jiraServer", false)
    }

    String getBitbucketServer() {
        return promptAndStore("Bitbucket Server (bitbucket.x.com)", false, FQDN_PATTERN, "bitbucketServer", false)
    }

    String getJiraCookies() {
        return promptAndStore("Jira Cookies (DevTools/Request/Cookie)", true, ANY_MATCH_PATTERN, "jiraCookies", true)
    }

    String getBitbucketCookies() {
        return promptAndStore("Bitbucket Cookies (DevTools/Request/Cookie)", true, ANY_MATCH_PATTERN, "bitbucketCookies", true)
    }

    List<String> getBoardTeamUsers(String boardId) {
        List<String> users = promptAndStore("Team users (optional, comma separated) for board: ${boardId}", false, ANY_MATCH_PATTERN, "${boardId}-users", true).split(",").toList()
        users = users.stream().map { it.trim() }.filter { it != null && !it.isEmpty() }.toList()
        return users
    }

    String getDateCheck(String promptDescription, String configKey) {
        return promptAndStore("Enter date ${promptDescription} (yyyy-mm-dd)", false, DATE_PATTERN, configKey, true)
    }

    String prompt(String text) {
        return prompt(text, false)
    }

    String prompt(String text, Pattern patternValidation) {
        return prompt(text, false, patternValidation)
    }

    String prompt(String text, boolean isPassword) {
        return prompt(text, isPassword, ANY_MATCH_PATTERN)
    }

    String prompt(String text, boolean isPassword, Pattern validationPattern) {
        while (true) {
            String input
            if (System.console() != null) {
                System.console().print("${text}: ")
                input = (isPassword) ? System.console().readPassword().toString().trim() : System.console().readLine().trim()
            } else {
                if (isPassword) {
                    System.out.println("Warning: value will not be masked (****)")
                }
                System.out.print("${text}: ")
                input = new Scanner(System.in).nextLine().trim()
            }

            if (validationPattern.matcher(input).matches()) {
                return input;
            }

            System.out.println("Input must match regular expression: ${validationPattern}")
        }
    }

    private String promptAndStore(String text, boolean isPassword, Pattern validationPattern,
                                  String defaultValueConfigKey, boolean promptForExistingValue) {
        def defaultValue
        if (defaultValueConfigKey && configFileManager.containsKey(defaultValueConfigKey)) {
            defaultValue = configFileManager.getValue(defaultValueConfigKey)
            if (!promptForExistingValue) {
                System.out.println("Found ${defaultValueConfigKey} configuration value - ${defaultValue}")
                return defaultValue
            }
        }

        text = (defaultValue) ? "${text} (press return to use existing value: ${isPassword ? "****" : defaultValue})" : text

        while (true) {
            // We match anything here, but then later do our own check
            String input = prompt(text, isPassword, ANY_MATCH_PATTERN)
            if (input.isEmpty() && defaultValue) {
                return defaultValue
            } else if (defaultValueConfigKey &&
                    validationPattern.matcher(input).matches()) {
                configFileManager.updateValue(defaultValueConfigKey, input)
                return input
            }
            System.out.println("Input must match regular expression: ${validationPattern}")
        }
    }
}
