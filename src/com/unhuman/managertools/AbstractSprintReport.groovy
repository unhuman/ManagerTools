package com.unhuman.managertools

@Grapes([
        @Grab(group='commons-cli', module='commons-cli', version='1.5.0')
])

import com.unhuman.managertools.rest.NullREST
import com.unhuman.managertools.rest.SourceControlREST
import com.unhuman.managertools.rest.exceptions.RESTException
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
    enum Mode { SCRUM, KANBAN }
    private static final CONFIG_FILENAME = ".managerTools.cfg"
    protected JiraREST jiraREST
    protected SourceControlREST bitbucketREST
    protected SourceControlREST githubREST
    private OptionAccessor commandLineOptions
    private Mode mode = Mode.SCRUM
    protected String boardId
    protected String teamName
    private List<String> sprintIds
    private Long weeks

    /**
     * Implementations should override this functionality.
     * @param commandLineOptions
     * @param teamName
     * @param boardId
     * @param sprintIds
     * @return
     */
    protected abstract def aggregateData(String teamName, String boardId, Mode mode, List<String> sprintIds, Long weeks)

    // this is an example of a very simple thing done
    //    protected def aggregateData(String teamName, String boardId, Mode mode, List<String> sprintIds) {
    //        sprintIds.each { sprintId -> {
    //            Object data = jiraREST.getSprintReport(boardId, sprintId)
    //            System.out.println(data.sprint.name)
    //        }}
    //    }

    /**
     * Implementations should override this functionality
     * @return
     */
    protected abstract void generateOutput()

    /**
     * Implementations can override this to support custom options
     * @param cliBuilder
     */
    def addCustomCommandLineOptions(CliBuilder cliBuilder) { }

    def validateCustomCommandLineOptions() { }

    def run() {
        setupRun()

        long time = System.currentTimeMillis()

        aggregateData(teamName, boardId, mode, sprintIds, weeks)
        generateOutput()

        time = (long) ((System.currentTimeMillis() - time) / 1000)
        Calendar.instance.with {
            clear()
            set(SECOND, (Integer) time)
            System.out.println("Time to process: ${format('HH:mm:ss')}")
        }
    }

    protected void setupRun() {
        CliBuilder cli = new CliBuilder(usage: 'SprintReportTeamAnalysis [options]', header: 'Options:');
        cli.width = 120
        cli.h(longOpt: 'help', 'Shows useful information')
        def boardOrTeamGroup = new OptionGroup(required: false)

        // create a single option for prompt (used twice, below)
        def promptOption = cli.option('p', [longOpt: 'prompt'], 'Prompt for team / timeframe info')

        boardOrTeamGroup.with {
            // Note these params must be kept up to date with the commandLineOptions validation below
            addOption(cli.option('b', [longOpt: 'boardId', args: 1, argName: 'boardId'], 'Sprint Board Id Number'))
            addOption(cli.option('t', [longOpt: 'teamName', args: 1, argName: 'team'], 'Sprint Team Name'))
            addOption(promptOption)
        }

        cli.options.addOptionGroup(boardOrTeamGroup)

        cli.q(longOpt: 'quietMode', 'Quiet mode (use default/stored values without prompt)')

        def scrumOrKanbanOptions = new OptionGroup(required: false)
        scrumOrKanbanOptions.with {
            // Note these params must be kept up to date with the commandLineOptions validation below
            addOption(cli.option('l', [longOpt: 'limit', args: 1, argName: 'limitSprints'], 'Number of recent sprints to process'))
            addOption(cli.option('s', [longOpt: 'sprintIds', args: 1, argName: 'sprintIds'], 'Sprint Id Numbers (comma separated)'))
            addOption(cli.option('w', [longOpt: 'weeks', args: 1, argName: 'weeks'], 'Kanban weeks to process'))
            addOption(promptOption)
        }

        cli.options.addOptionGroup(scrumOrKanbanOptions)

        cli.ia(longOpt: 'includeActive', 'Include current active sprint in results')

        // Any custom options need to be added
        addCustomCommandLineOptions(cli)

        commandLineOptions = cli.parse(this.args)

        if (!commandLineOptions) {
            System.exit(-1)
        }

        // ensure things are set up correctly (re-use of prompt in 2 groups is handled inconsistently)
        if (commandLineOptions.p &&
                (commandLineOptions.b || commandLineOptions.t || commandLineOptions.l || commandLineOptions.s || commandLineOptions.w)) {
            System.out.println("Cannot mix prompt with board/team/limit/sprintIds/weeks")
            cli.usage()
            System.exit(-1)
        }

        try {
            validateCustomCommandLineOptions()
        } catch (Exception e) {
            System.out.println(e.getMessage())
            cli.usage()
            System.exit(-1)
        }

        if (commandLineOptions.h) {
            cli.usage()
            System.exit(0)
        }

        setupServices()

        def limit
        if (commandLineOptions.'prompt') {
            limit = Integer.parseInt(CommandLineHelper.promptNumber("Number of Sprints/Cycles"))
        } else if (commandLineOptions.'limit') {
            limit = Integer.parseInt(commandLineOptions.'limit')
        } else if (commandLineOptions.'weeks') {
            weeks = Integer.parseInt(commandLineOptions.'weeks')
        }

        if (limit) {
            try {
                GetTeamSprints getTeamSprints = new GetTeamSprints(jiraREST)
                def sprintData = getTeamSprints.getRecentSprints(commandLineOptions.'includeActive',
                        boardId, limit)
                sprintIds = sprintData.stream().map(sprint -> sprint.id.toString()).collect(Collectors.toUnmodifiableList())
            } catch (RESTException re) {
                if (re.statusCode == HttpStatus.SC_BAD_REQUEST) {
                    // No sprintIds = this could be Kanban board
                    if (commandLineOptions.'prompt') {
                        weeks = limit
                    } else {
                        throw new RuntimeException("Notice: This is a Kanban board - no sprints found")
                    }
                } else {
                    throw re
                }
            }
        } else if (sprintIds) {
            // limit and sprintIds are required / mutually exclusive, so just use what we get
            sprintIds = commandLineOptions.'sprintIds'.split(',')
        }

        // This is separate since it can be reset in prompt mode during limit check, above
        if (weeks) {
            mode = Mode.KANBAN
            // in prompt mode, we already have the weeks value from processing above
            if (!commandLineOptions.'prompt') {
                weeks = Long.parseLong(commandLineOptions.'weeks')
            }
        }
    }

    protected void setCommandLineOptions(OptionAccessor commandLineOptions) {
        this.commandLineOptions = commandLineOptions;
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
                bitbucketREST = (password != null && password.length() > 0)
                        ? new BitbucketREST(bitbucketServer, username, password)
                        : new NullREST("bitbucket")

                break
            case "c":
                // Get authentication information
                String jiraCookies = commandLineHelper.getJiraAuth()
                String bitbucketCookies = commandLineHelper.getBitbucketAuth()

                jiraREST = new JiraREST(jiraServer, jiraCookies)
                bitbucketREST = (bitbucketCookies != null && bitbucketCookies.length() > 0)
                        ? new BitbucketREST(bitbucketServer, bitbucketCookies)
                        : new NullREST("bitbucket")
                break
            default:
                throw new RuntimeException("Invalid auth method: ${authMethod}")
        }

        // Github always uses token-based auth
        String githubToken = commandLineHelper.getGithubToken()
        githubREST = (githubToken != null && githubToken.length() > 0)
                ? new GithubREST(commandLineHelper, githubToken)
                : new NullREST("github")

        // TODO: Extract team name and boardId
        teamName = (commandLineOptions.'teamName') ? commandLineOptions.'teamName' : null
        boardId = (commandLineOptions.'boardId') ? commandLineOptions.'boardId' : null
        if (boardId == null) {
            if (teamName == null) {
                teamName = commandLineHelper.promptValue("Team name")
            }
            // look up boardId from team name
            String lookupValue = "teamMappings.${teamName}"
            boardId = commandLineHelper.getConfigFileManager().getValue(lookupValue)
        }

        // TODO: find team name if only boardId is provided

        // if Board Id is null, then we have a problem
        if (boardId == null) {
            throw new RuntimeException("boardId is required")
        }
    }
}