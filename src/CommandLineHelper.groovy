class CommandLineHelper {
    ConfigFileManager configFileManager

    CommandLineHelper(String configFilename) {
        if (configFilename) {
            configFileManager = new ConfigFileManager(configFilename)
        }
    }

    String getUsername() {
        String username = promptAndStore("Username (or press enter for ${System.getProperty("user.name")})", false, null)
        if (username.isEmpty()) {
            username = System.getProperty("user.name")
        }
        return username
    }

    String getPassword() {
        return promptAndStore("Password", true, null);
    }

    String getSprintTeam() {
        return promptAndStore("Sprint Team name", false, "sprintTeam", true)
    }

    String getSprintTeamBoardId() {
        if (!configFileManager.containsKey("sprintTeam")) {
            throw new RuntimeException("Need sprintTeam already found")
        }

        String sprintTeam = configFileManager.getValue("sprintTeam")
        return promptAndStore("Board Id for ${sprintTeam}", false, "${sprintTeam}-boardId", true)
    }

    String getJiraServer() {
        return promptAndStore("Jira Server (jira.x.com)", false, "jiraServer", false)
    }

    String getBitbucketServer() {
        return promptAndStore("Bitbucket Server (bitbucket.x.com)", false, "bitbucketServer", false)
    }

    String getJiraCookies() {
        return promptAndStore("Jira Cookies (DevTools/Request/Cookie)", true, "jiraCookies", true)
    }

    String getBitbucketCookies() {
        return promptAndStore("Bitbucket Cookies (DevTools/Request/Cookie)", false, "bitbucketCookies", true)
    }

    String getDateCheck(String promptDescription, String configKey) {
        return promptAndStore("Enter date ${promptDescription} (yyyy-mm-dd)", false, configKey, true)
    }


    String prompt(String text) {
        return prompt(text, false)
    }

    String prompt(String text, boolean isPassword) {
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
        return input
    }

    private String promptAndStore(String text, boolean isPassword, String defaultValueConfigKey, boolean promptForExistingValue) {
        def defaultValue
        if (defaultValueConfigKey && configFileManager.containsKey(defaultValueConfigKey)) {
            defaultValue = configFileManager.getValue(defaultValueConfigKey)

            if (!promptForExistingValue) {
                System.out.println("Found ${defaultValueConfigKey} configuration value - ${defaultValue}")
                return defaultValue
            }

            text = "${text} (press return to use existing value: ${isPassword ? "****" : defaultValue})"
        }

        String input = prompt(text, isPassword)
        if (input.isEmpty() && defaultValue) {
            input = defaultValue
        } else if (defaultValueConfigKey && !input.isEmpty()) {
            configFileManager.updateValue(defaultValueConfigKey, input)
        }
        return input
    }
}
