import sys
from typing import List

from managertools.sprint_report_team_analysis import SprintReportTeamAnalysis
from managertools.flexidb.flexidb_query_column import FlexiDBQueryColumn
from managertools.flexidb.data.flexidb_row import FlexiDBRow
from managertools.data import DBData, DBIndexData, UserActivity


class SprintReportIndividualAnalysis(SprintReportTeamAnalysis):
    def __init__(self, args: List[str]):
        super().__init__(args)
        self.team_users = []

    def setup_run(self):
        super().setup_run()
        self.team_users = self.command_line_helper.get_team_board_users(self.team_name, self.board_id)
        print()

    def generate_columns_order(self) -> List[str]:
        column_order = super().generate_columns_order()

        index = column_order.index(UserActivity.COMMENTED.name)
        column_order.pop(index)
        column_order.insert(index, UserActivity.COMMENTED_ON_SELF.name)
        column_order.insert(index + 1, UserActivity.COMMENTED_ON_OTHERS.name)
        column_order.insert(index + 2, UserActivity.OTHERS_COMMENTED.name)

        return column_order

    def generate_output(self):
        column_order = self.generate_columns_order()
        sprints = self.database.find_unique_values(DBIndexData.SPRINT.name)

        authors = list(dict.fromkeys(self.database.find_unique_values(DBData.AUTHOR.name)))
        users = list(dict.fromkeys(self.database.find_unique_values(DBIndexData.USER.name)))

        if len(self.team_users) == 1:
            if self.team_users[0] == "*":
                self.team_users = authors
            elif self.team_users[0] == "**":
                self.team_users = users

        if self.team_users:
            resolved = []
            for specified in self.team_users:
                matched = [u for u in users if u.casefold() == specified.casefold()]
                resolved.append(matched[0] if matched else specified)
            users = resolved

        data_indicator = self.team_name or self.board_id

        for user in users:
            sb = [FlexiDBRow.headings_to_csv(column_order)]
            overall_totals_row = FlexiDBRow({})

            for sprint in sprints:
                user_sprint_finder = [
                    FlexiDBQueryColumn(DBIndexData.SPRINT.name, sprint),
                    FlexiDBQueryColumn(DBIndexData.USER.name, self.sanitize_name_for_index(user)),
                ]
                self.find_rows_and_append_csv_data(user_sprint_finder, sb, overall_totals_row)

            self.append_summary(sb, overall_totals_row)

            filename = self.command_line_options.outputCSV.replace(
                '.csv', f'-{data_indicator}-{user}.csv'
            )
            self.write_results_file(filename, '\n'.join(sb))


if __name__ == '__main__':
    try:
        analysis = SprintReportIndividualAnalysis(sys.argv[1:])
        analysis.run()
    except KeyboardInterrupt:
        print("\nOperation cancelled by user.", file=sys.stderr)
        sys.exit(0)
    except RuntimeError as e:
        print(f"Caught: {type(e).__name__}: {e}", file=sys.stderr)
        sys.exit(1)
