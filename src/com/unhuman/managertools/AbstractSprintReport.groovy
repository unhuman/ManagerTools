package com.unhuman.managertools

import com.unhuman.managertools.rest.exceptions.RESTException
@Grapes([
        @Grab(group='commons-cli', module='commons-cli', version='1.5.0')
])

import com.unhuman.managertools.util.CommandLineHelper
import com.unhuman.managertools.rest.BitbucketREST
import com.unhuman.managertools.rest.GithubREST
import com.unhuman.managertools.rest.JiraREST

import groovy.cli.commons.CliBuilder
import groovy.cli.commons.OptionAccessor
import org.apache.commons.cli.OptionGroup
import org.apache.hc.core5.http.HttpStatus

import java.util.stream.Collectors

abstract class AbstractSprintReport extends Script {
    private static final CONFIG_FILENAME = ".managerTools.cfg"
    protected JiraREST jiraREST
    protected BitbucketREST bitbucketREST
    protected GithubREST githubREST
    private OptionAccessor commandLineOptions
    private List<String> sprintIds

    /**
     * Implementations should override this functionality.
     * @param commandLineOptions
     * @param boardId
     * @param sprintIds
     * @return
     */
    abstract def process(String boardId, List<String> sprintIds)

    // this is an example of a very simple thing done
    //    def process(String boardId, List<String> sprintIds) {
    //        sprintIds.each { sprintId -> {
    //            Object data = jiraREST.getSprintReport(boardId, sprintId)
    //            System.out.println(data.sprint.name)
    //        }}
    //    }

    /**
     * Implementations can override this to support custom options
     * @param cliBuilder
     */
    def addCustomCommandLineOptions(CliBuilder cliBuilder) { }

    def validateCustomCommandLineOptions() { }

    def run() {
        setupRun()
        process(commandLineOptions.'boardId', sprintIds)
    }

    protected void setupRun() {
        CliBuilder cli = new CliBuilder(usage: 'SprintReportTeamAnalysis [options]', header: 'Options:');
        cli.width = 120
        cli.h(longOpt: 'help', 'Shows useful information')
        cli.b(longOpt: 'boardId', required: true, args: 1, argName: 'boardId', 'Sprint Board Id Number')
        cli.q(longOpt: 'quietMode', 'Quiet mode (use default/stored values without prompt)')

        def optionGroup = new OptionGroup(required: true)
        optionGroup.with {
            addOption(cli.option('l', [longOpt: 'limit', args: 1, argName: 'limitSprints'], 'Number of recent sprints to process'))
            addOption(cli.option('s', [longOpt: 'sprintIds', args: 1, argName: 'sprintIds'], 'Sprint Id Numbers (comma separated)'))
        }
        cli.options.addOptionGroup(optionGroup)

        // Any custom options need to be added
        addCustomCommandLineOptions(cli)

        commandLineOptions = cli.parse(this.args)

        if (!commandLineOptions) {
            System.exit(-1)
        }

        try {
            validateCustomCommandLineOptions()
        } catch (Exception e) {
            System.out.println(e.getMessage())
            cli.usage()
            return
        }

        if (commandLineOptions.h) {
            cli.usage()
            return
        }

        setupServices()

        if (commandLineOptions.'limit') {
            try {
                GetTeamSprints getTeamSprints = new GetTeamSprints(jiraREST)
                def sprintData = getTeamSprints.getClosedRecentSprints(commandLineOptions.'boardId', Integer.parseInt(commandLineOptions.'limit'))
                sprintIds = sprintData.stream().map(sprint -> sprint.id.toString()).collect(Collectors.toUnmodifiableList())
            } catch (RESTException re) {
                if (re.statusCode == HttpStatus.SC_BAD_REQUEST) {
                    // No sprintIds = this could be Kanban board
                    sprintIds = null
                } else {
                    throw re
                }
            }
        } else {
            // limit and sprintIds are required / mutually exclusive, so just use what we get
            sprintIds = commandLineOptions.'sprintIds'.split(',')
        }
    }

    protected OptionAccessor getCommandLineOptions() {
        return commandLineOptions
    }

    protected void setupServices() {
        CommandLineHelper commandLineHelper = new CommandLineHelper(CONFIG_FILENAME)
        if (commandLineOptions.q) {
            commandLineHelper.setQuietModeNoPrompts()
        }

        // Get server information
        String jiraServer = commandLineHelper.getJiraServer()
        String bitbucketServer = commandLineHelper.getBitbucketServer()

        // Get auth method
        String authMethod = "c" // commandLineHelper.getAuthMethod()

        switch (authMethod) {
            case "p":
                String username = commandLineHelper.getUsername()
                String password = commandLineHelper.getPassword()

                jiraREST = new JiraREST(jiraServer, username, password)
                bitbucketREST = new BitbucketREST(bitbucketServer, username, password)

                break
            case "c":
                // Get authentication information
                String jiraCookies = commandLineHelper.getJiraCookies()
                String bitbucketCookies = commandLineHelper.getBitbucketCookies()

                jiraREST = new JiraREST(jiraServer, jiraCookies)
                bitbucketREST = new BitbucketREST(bitbucketServer, bitbucketCookies)

                break
            default:
                throw new RuntimeException("Invalid auth method: ${authMethod}")
        }

        // Github always uses token-based auth
        String githubToken = commandLineHelper.getGithubToken()
        githubREST = new GithubREST(commandLineHelper, githubToken)
    }
}