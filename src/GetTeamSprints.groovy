@Grapes([
        @Grab(group='commons-cli', module='commons-cli', version='1.5.0')
])

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
        cli.b(longOpt: 'board-id', required: true, args: 1, argName: 'boardId', 'Sprint Board Id Number')
        cli.l(longOpt: 'limit', required: false, args: 1, argName: 'limitCount', 'Limit of count to get')

        def options = cli.parse(this.args)

        if (options.h) {
            cli.usage()
            return
        }

        CommandLineHelper commandLineHelper = new CommandLineHelper(".managerTools.cfg")

        String jiraServer = commandLineHelper.getJiraServer()
        String jiraCookies = commandLineHelper.getJiraCookies()

        jiraREST = new JiraREST(jiraServer, jiraCookies)

        Object data = getClosedRecentSprints(options.'board-id', (options.'limit') ? Integer.parseInt(options.'limit') : null)
        data.sprints.each { sprint ->
            System.out.println("${sprint.id}: ${sprint.name}")
        }
    }

    Object getClosedRecentSprints(String boardId, Integer limitCount) {
        Object data = jiraREST.getSprints(boardId)

        // invert the order of sprints (most recent first)
        Collections.reverse(data.sprints)

        for (int i = data.sprints.size() - 1; i >= 0; --i) {
            def sprint = data.sprints.get(i);
            if (!sprint.state.toUpperCase().equals("CLOSED")) {
                data.sprints.removeAt(i)
            }
        }

        if (limitCount != null) {
            data.sprints = data.sprints.subList(0, limitCount)
        }

        return data
    }
}