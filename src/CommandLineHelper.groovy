class CommandLineHelper {
    ConfigFileManager configFileManager

    CommandLineHelper(String configFilename) {
        if (configFilename) {
            configFileManager = new ConfigFileManager(configFilename)
        }
    }

    String getUsername() {
        String username = prompt("Username (or press enter for ${System.getProperty("user.name")})", false, null)
        if (username.isEmpty()) {
            username = System.getProperty("user.name")
        }
        return username
    }

    String getPassword() {
        return prompt("Password", true, null);
    }

    String getJiraServer() {
        return prompt("Jira Server (jira.x.com)", false, "jiraServer", false)
    }

    String getBitbucketServer() {
        return prompt("Bitbucket Server (bitbucket.x.com)", false, "bitbucketServer", false)
    }

    String getJiraCookies() {
        return prompt("Jira Cookies (DevTools/Request/Cookie)", false, "jiraCookies", true)
    }

    String getBitbucketCookies() {
        return prompt("Bitbucket Cookies (DevTools/Request/Cookie)", false, "bitbucketCookies", true)
    }

    String prompt(String text, boolean isPassword, String defaultValueConfigKey, boolean promptForExistingValue) {
        def defaultValue
        if (defaultValueConfigKey && configFileManager.containsKey(defaultValueConfigKey)) {
            defaultValue = configFileManager.getValue(defaultValueConfigKey)

            if (!promptForExistingValue) {
                System.out.println("Found ${defaultValueConfigKey} configuration value - ${defaultValue}")
                return defaultValue
            }

            text = "${text} (press return to use existing value)"
        }

        String input
        if (System.console() != null) {
            System.out.print("${text}: ")
            input = (isPassword) ? System.console().readPassword().toString().trim() : System.console().readLine().trim()
        } else {
            if (isPassword) {
                System.out.println("Warning: value will not be masked (****)")
            }
            System.out.print("${text}: ")
            input = new Scanner(System.in).nextLine().trim()
        }

        if (input.isEmpty() && defaultValue) {
            input = defaultValue
        } else if (defaultValueConfigKey && !input.isEmpty()) {
            configFileManager.updateValue(defaultValueConfigKey, input)
        }
        return input
    }
}
