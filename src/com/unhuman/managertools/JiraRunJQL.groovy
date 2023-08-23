package com.unhuman.managertools

import com.unhuman.managertools.rest.JiraREST
import com.unhuman.managertools.util.CommandLineHelper
import groovy.cli.commons.CliBuilder

import java.util.regex.Matcher
import java.util.regex.Pattern

class JiraRunJQL extends Script {
    @Override
    Object run() {
        def cli = new CliBuilder(usage: 'JiraRunJQL [options]', header: 'Options:');
        cli.width = 120
        cli.h(longOpt: 'help', 'Shows useful information')
        cli.j(longOpt: 'jql', required: true, args: 1, argName: 'jql', 'JQL to execute')
        cli.q(longOpt: 'quietMode', 'Quiet mode (use default/stored values without prompt)')

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
        String jiraCookies = commandLineHelper.getJiraCookies()

        JiraREST jiraREST = new JiraREST(jiraServer, jiraCookies)

        System.out.println("Requesting source data...")
        Object sourceResults = jiraREST.jqlSummaryQuery(options.j)
        System.out.println(sourceResults)
    }
}
