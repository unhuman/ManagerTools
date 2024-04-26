package com.unhuman.managertools

class SprintReportTeamCarryoverHours extends AbstractSprintReport {
    @Override
    def process(String teamName, String boardId, List<String> sprintIds) {
        double totalCarryOverHoursAcrossSprints = 0
        sprintIds.each(sprintId -> {
            Object data = jiraREST.getSprintReport(boardId, sprintId)
            System.out.println(data.sprint.name)

            // Gather ticket data for incomplete work
            double sprintCarryOverTime = 0
            data.contents.issuesNotCompletedInCurrentSprint.each(ticket -> {
                double ticketCarryOverTime
                String ticketCarryOverText
                try {
                    ticketCarryOverTime = ticket.trackingStatistic.statFieldValue.value / 21600 // (time in hours)
                    ticketCarryOverText = ticketCarryOverTime.toString()
                    sprintCarryOverTime += ticketCarryOverTime
                } catch (Exception e) {
                    ticketCarryOverText = "n/a"
                }
                System.out.println("   ${ticket.key}: ${ticket.summary} carry over days: ${ticketCarryOverText}")
            })
            System.out.println("   Sprint total carryover days: ${sprintCarryOverTime}")
            totalCarryOverHoursAcrossSprints += sprintCarryOverTime
        })
        System.out.println("total carryover days: ${totalCarryOverHoursAcrossSprints}")
    }
}