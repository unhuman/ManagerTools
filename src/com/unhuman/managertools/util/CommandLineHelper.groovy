package com.unhuman.managertools.util


import java.util.regex.Pattern

class CommandLineHelper {
    private static final Pattern ANY_MATCH_PATTERN = Pattern.compile(".*")

    enum TextSecurity {
        NONE,
        PASSWORD,
        MASK
    }

    private static final Pattern AUTH_PATTERN = Pattern.compile("(p|password|pw|c|cookie)")
    // Users Match * (authors), ** (users), or comma separated list
    private static final Pattern USERS_MATCH_PATTERN = Pattern.compile("^\\*{1,2}\$|^[a-zA-Z0-9\\.]+([,\\s]+[a-zA-Z0-9\\.]*)*\$")
    private static final Pattern DATE_PATTERN = Pattern.compile("\\d{4}-\\d{2}-\\d{2}")
    private static final Pattern NUMBERS_REQUIRED_PATTERN = Pattern.compile("\\d+")
    private static final Pattern FQDN_PATTERN = Pattern.compile("(\\w+\\.){2,}\\w+")

    ConfigFileManager configFileManager
    private boolean quietMode = false

    CommandLineHelper(String configFilename) {
        if (configFilename) {
            configFileManager = new ConfigFileManager(configFilename)
        }
    }

    ConfigFileManager getConfigFileManager() {
        return configFileManager
    }

    void setQuietModeNoPrompts() {
        this.quietMode = true
    }

    /**
     * will return p for password or c for cookie
     * @return
     */
    String getAuthMethod() {
        return promptAndStore("Auth Method ${AUTH_PATTERN.toString()}", TextSecurity.NONE, AUTH_PATTERN, "authMethod", true, "password").substring(0, 1)
    }

    String getUsername() {
        String username = promptAndStore("Username (or press enter for ${System.getProperty("user.name")})", TextSecurity.NONE, ANY_MATCH_PATTERN, null, false)
        if (username.isEmpty()) {
            username = System.getProperty("user.name")
        }
        return username
    }

    String getPassword() {
        return promptAndStore("Password", TextSecurity.PASSWORD, ANY_MATCH_PATTERN, null, false);
    }

    String getSprintTeam() {
        return promptAndStore("Sprint Team name", TextSecurity.NONE, ANY_MATCH_PATTERN, "sprintTeam", true)
    }

    String getSprintTeamBoardId() {
        if (!configFileManager.containsKey("sprintTeam")) {
            throw new RuntimeException("Need sprintTeam already found")
        }

        String sprintTeam = configFileManager.getValue("sprintTeam")
        return promptAndStore("Board Id for ${sprintTeam}", TextSecurity.NONE, NUMBERS_REQUIRED_PATTERN, "${sprintTeam}-boardId", true)
    }

    String getJiraServer() {
        return promptAndStore("Jira Server (jira.x.com)", TextSecurity.NONE, FQDN_PATTERN, "jiraServer", false)
    }

    String getBitbucketServer() {
        return promptAndStore("Bitbucket Server (bitbucket.x.com)", TextSecurity.NONE, FQDN_PATTERN, "bitbucketServer", false)
    }

    String getJiraAuth() {
        return promptAndStore("Jira Access Token or Cookies (DevTools/Request/Cookie)", TextSecurity.MASK, ANY_MATCH_PATTERN, "jiraAuth", true)
    }

    String getBitbucketAuth() {
        return promptAndStore("Bitbucket Access Token or Cookies (DevTools/Request/Cookie)", TextSecurity.MASK, ANY_MATCH_PATTERN, "bitbucketAuth", true)
    }

    String getGithubToken() {
        return promptAndStore("Github Token", TextSecurity.MASK, ANY_MATCH_PATTERN, "githubToken", true)
    }

    List<String> getTeamBoardUsers(String teamName, String boardId) {
        String configKey = (teamName != null) ? "teamUsers.${teamName}" : "${boardId}-users"
        String typeIndicator = (teamName != null) ? "team" : "board"
        String item = (teamName != null) ? teamName : boardId
        List<String> users = promptAndStore("Team users (optional, comma/space separated, * for all authors, ** for all users) for ${typeIndicator}: ${item}",
                TextSecurity.NONE, USERS_MATCH_PATTERN, configKey, true, "*").split(",").toList()
        users = users.stream().map { it.trim() }.filter { it != null && !it.isEmpty() }.toList()
        return users
    }

    String getDateCheck(String promptDescription, String configKey) {
        return promptAndStore("Enter date ${promptDescription} (yyyy-mm-dd)", TextSecurity.NONE, DATE_PATTERN, configKey, true)
    }

    String prompt(String text) {
        return prompt(text, TextSecurity.NONE)
    }

    String prompt(String text, Pattern patternValidation) {
        return prompt(text, TextSecurity.NONE, patternValidation)
    }

    String prompt(String text, TextSecurity textSecurity) {
        return prompt(text, textSecurity, ANY_MATCH_PATTERN)
    }

    String prompt(String text, TextSecurity textSecurity, Pattern validationPattern) {
        if (quietMode) {
            return ""
        }

        performPrompt(text, textSecurity, validationPattern)
    }

    String performPrompt(String text, TextSecurity textSecurity, Pattern validationPattern) {
        while (true) {
            String input
            if (System.console() != null) {
                if (TextSecurity.PASSWORD == textSecurity) {
                    // We display the prompt when we are asking for a real password
                    // otherwise, getStringValue manages that
                    System.console().print("${text}: ")
                    input = System.console().readPassword().toString().trim()
                } else {
                    input = getStringValue(text).trim()
                }
            } else {
                if (TextSecurity.PASSWORD == textSecurity) {
                    System.out.println("Warning: value will not be masked (****)")
                }
                input = getStringValue(text).trim()
            }

            if (validationPattern.matcher(input).matches()) {
                return input
            }

            System.out.println("Input must match regular expression: ${validationPattern}")
        }
    }

    private String promptAndStore(String text, TextSecurity textSecurity, Pattern validationPattern,
                                  String defaultValueConfigKey, boolean promptForExistingValue) {
        return promptAndStore(text, textSecurity, validationPattern, defaultValueConfigKey, promptForExistingValue, null)
    }


    /**
     * Note: will not store data without defaultValueConfigKey
     *
     * @param text
     * @param textSecurity
     * @param validationPattern
     * @param defaultValueConfigKey
     * @param promptForExistingValue
     * @param defaultValue
     * @return
     */
    private String promptAndStore(String text, TextSecurity textSecurity, Pattern validationPattern,
                                  String defaultValueConfigKey, boolean promptForExistingValue, String defaultValue) {

        def useDefaultValue = defaultValue
        if (defaultValueConfigKey && configFileManager.containsKey(defaultValueConfigKey)) {
            useDefaultValue = (configFileManager.getValue(defaultValueConfigKey)) ? configFileManager.getValue(defaultValueConfigKey) : useDefaultValue
            if (!promptForExistingValue) {
                System.out.println("Found ${defaultValueConfigKey} configuration value - ${useDefaultValue}")
                return useDefaultValue
            }
        }

        String promptText = (useDefaultValue) ? "${text} (press return to use default value: ${TextSecurity.NONE != textSecurity  ? "****" : useDefaultValue})" : text

        while (true) {
            // We match anything here, but then later do our own check
            String input = prompt(promptText, textSecurity, ANY_MATCH_PATTERN)
            if (input.isEmpty() && useDefaultValue) {
                return useDefaultValue
            } else if (validationPattern.matcher(input).matches()) {
                if (defaultValueConfigKey) {
                    configFileManager.updateValue(defaultValueConfigKey, input)
                }
                return input
            }

            if (quietMode) {
                throw new RuntimeException("Couldn't quietly get text: ${text}")
            }
            
            System.out.println("Input must match regular expression: ${validationPattern}")
        }
    }

    private String getStringValue(promptText) {
        // Use console or zsh to get value b/c OSX limits values to 1024 chars
        // We do have to display the promptText differently b/c it doesn't show until newline otherwise

        boolean isWindows = System.properties['os.name'].toLowerCase().contains('windows')
        // https://stackoverflow.com/questions/15339148/check-if-java-code-is-running-from-intellij-eclipse-etc-or-command-line
        boolean isIDE = System.getProperty("java.class.path").contains("idea_rt.jar")
                || System.getenv("XPC_SERVICE_NAME").contains("intellij") // detect "run"

        // Use console if Windows or running from an IDE
        if (isWindows || isIDE) {
            System.out.print("${promptText}: ")
            if (System.console() != null) {
                return System.console().readLine()
            } else {
                return new Scanner(System.in).nextLine().trim()
            }
        } else {
            //
            Process process = ["zsh", "-c", 'unset tmp; vared -p "' + promptText + ': " -c tmp; echo $tmp; unset tmp'].execute()
            synchronized (process) {
                try {
                    process.waitFor()
                } catch (InterruptedException ie) {
                    throw new RuntimeException("Problem accepting input from zsh / vared", ie)
                }
            }
            if (process.exitValue() != 0) {
                throw new RuntimeException("Problem running zsh to get value: ${process.exitValue()}")
            }
            String value = process.getInputStream().readLines()[0]
            return value
        }
    }

    Object getValue(String key) {
        return configFileManager.getValue(key)
    }

    void storeValue(String key, Object value) {
        configFileManager.updateValue(key, value)
    }

}
