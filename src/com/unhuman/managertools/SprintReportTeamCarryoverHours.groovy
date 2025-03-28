package com.unhuman.managertools

class SprintReportTeamCarryoverHours extends AbstractSprintReport {
    @Override
    protected def aggregateData(String teamName, String boardId, Mode mode, List<String> sprintIds, Long weeks) {
        if (mode != Mode.SCRUM) {
            throw new RuntimeException("Only SCRUM mode is supported for this report")
        }

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

    @Override
    protected void generateOutput() {
        // TODO: Implement this
        // No output to generate as that's still in aggregateData
    }
}