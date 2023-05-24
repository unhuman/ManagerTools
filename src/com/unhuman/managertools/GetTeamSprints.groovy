package com.unhuman.managertools

@Grapes([
        @Grab(group='commons-cli', module='commons-cli', version='1.5.0')
])

import com.unhuman.managertools.util.CommandLineHelper
import com.unhuman.managertools.rest.JiraREST
import groovy.cli.commons.CliBuilder

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
        cli.b(longOpt: 'boardId', required: true, args: 1, argName: 'boardId', 'Sprint Board Id Number')
        cli.l(longOpt: 'limit', required: false, args: 1, argName: 'limitCount', 'Limit of count to get')

        def options = cli.parse(this.args)

        if (!options) {
            return
        }

        if (options.h) {
            cli.usage()
            return
        }

        CommandLineHelper commandLineHelper = new CommandLineHelper(".managerTools.cfg")

        String jiraServer = commandLineHelper.getJiraServer()
        String jiraCookies = commandLineHelper.getJiraCookies()

        jiraREST = new JiraREST(jiraServer, jiraCookies)

        Object data = getClosedRecentSprints(options.'boardId', (options.'limit') ? Integer.parseInt(options.'limit') : null)
        data.each { sprint ->
            System.out.println("${sprint.id}: ${sprint.name}")
        }
    }

    List<Object> getClosedRecentSprints(String boardId, Integer limitCount) {
        Object data = jiraREST.getSprints(boardId)

        // invert the order of sprints (most recent first)
        Collections.reverse(data)

        for (int i = data.size() - 1; i >= 0; --i) {
            def sprint = data.get(i);
            if (!sprint.state.toUpperCase().equals("CLOSED")) {
                data.removeAt(i)
            }
        }

        if (limitCount != null) {
            data = data.subList(0, limitCount)
        }

        // Flip what's left to order back the way we want it
        Collections.reverse(data)

        return data
    }
}