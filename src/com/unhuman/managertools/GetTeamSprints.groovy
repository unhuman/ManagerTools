package com.unhuman.managertools

@Grapes([
        @Grab(group='commons-cli', module='commons-cli', version='1.5.0')
])

import com.unhuman.managertools.util.CommandLineHelper
import com.unhuman.managertools.rest.JiraREST
import groovy.cli.commons.CliBuilder

import java.time.LocalDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

class GetTeamSprints extends Script {
    JiraREST jiraREST

    def GetTeamSprints() {
        // do nothing - rely on run() to setup jiraREST
    }

    def GetTeamSprints(JiraREST jiraREST) {
        this.jiraREST = jiraREST
    }

    def run() {
        def cli = new CliBuilder(usage: 'GetTeamSprints [options]', header: 'Options:');
        cli.width = 120
        cli.h(longOpt: 'help', 'Shows useful information')
        // TODO: Make take either boardId or teamName - similar to AbstractSprintReport
        cli.b(longOpt: 'boardId', required: true, args: 1, argName: 'boardId', 'Sprint Board Id Number')
        cli.l(longOpt: 'limit', required: false, args: 1, argName: 'limitCount', 'Limit of count to get')
        cli.q(longOpt: 'quietMode', 'Quiet mode (use default/stored values without prompt)')
        cli.ia(longOpt: 'includeActive', 'Include current active sprint in results')

        def options = cli.parse(this.args)

        if (!options) {
            return
        }

        if (options.h) {
            cli.usage()
            return
        }

        CommandLineHelper commandLineHelper = new CommandLineHelper(".managerTools.cfg")
        if (options.q) {
            commandLineHelper.setQuietModeNoPrompts()
        }

        String jiraServer = commandLineHelper.getJiraServer()
        String jiraCookies = commandLineHelper.getJiraAuth()

        jiraREST = new JiraREST(jiraServer, jiraCookies)

        Object data = getRecentSprints(options.'includeActive', options.'boardId', (options.'limit') ? Integer.parseInt(options.'limit') : null)
        data.each { sprint ->
            System.out.println("${sprint.id}: ${sprint.name}")
        }
    }

    List<Object> getRecentSprints(Boolean includeActiveSprint, String boardId, Integer limitCount) {
        List<Object> data = jiraREST.getSprints(boardId)

        // filter out boards that show up here incorrectly
        data = new ArrayList<>(data.stream().filter { retro -> retro.originBoardId.toString() == boardId }.toList())

        // invert the order of sprints (most recent first)
        Collections.reverse(data)

        for (int i = data.size() - 1; i >= 0; --i) {
            def sprint = data.get(i);

            def sprintActive = LocalDateTime.parse(sprint.endDate, DateTimeFormatter.ISO_DATE_TIME).toInstant(ZoneOffset.UTC).toEpochMilli() > System.currentTimeMillis()

            if (!includeActiveSprint && !sprint.state.equalsIgnoreCase("CLOSED") && sprintActive) {
                data.removeAt(i)
            }
        }

        if (limitCount != null) {
            data = data.subList(0, Math.min(limitCount, data.size()))
        }

        // Flip what's left to order back the way we want it
        Collections.reverse(data)

        return data
    }
}