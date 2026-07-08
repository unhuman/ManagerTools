"""Metrics aggregator for team and org-wide analysis."""

import os
import glob
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict


class MetricsAggregator:
    """Aggregates individual sprint report CSVs into queryable metrics."""

    def __init__(self, reports_dir: str = "."):
        """Initialize aggregator with path to individual report CSVs.

        Args:
            reports_dir: Directory containing individual-{Team}-{User}.csv files
        """
        self.reports_dir = reports_dir
        self.data: List[Dict] = []
        self.teams: Dict[str, List[str]] = defaultdict(list)
        self._load_reports()

    def _load_reports(self):
        """Load all individual report CSV files from reports directory."""
        pattern = os.path.join(self.reports_dir, "individual-*.csv")
        csv_files = sorted(glob.glob(pattern))

        for csv_file in csv_files:
            try:
                team, user = self._extract_team_user(csv_file)
                if team and user:
                    df = self._parse_csv(csv_file)
                    if not df.empty:
                        # Normalize and add to data
                        normalized = self._normalize_rows(df, team, user)
                        self.data.extend(normalized)
                        if user not in self.teams[team]:
                            self.teams[team].append(user)
            except Exception as e:
                print(f"Warning: Could not load {csv_file}: {e}")

    @staticmethod
    def _extract_team_user(filepath: str) -> Tuple[Optional[str], Optional[str]]:
        """Extract team and user from filename 'individual-{Team}-{User}.csv'."""
        basename = Path(filepath).stem
        if not basename.startswith("individual-"):
            return None, None
        rest = basename[len("individual-"):]
        parts = rest.rsplit("-", 1)
        if len(parts) == 2:
            return parts[0], parts[1]
        return None, None

    @staticmethod
    def _parse_csv(filepath: str) -> pd.DataFrame:
        """Load CSV and filter out totals rows."""
        df = pd.read_csv(filepath)
        df = df[~df['SPRINT'].isin(['Sprint Totals', 'Overall Totals'])]
        df = df[df['SPRINT'].notna()]
        df = df[df['SPRINT'].str.strip() != '']
        return df.reset_index(drop=True)

    @staticmethod
    def _normalize_rows(df: pd.DataFrame, team: str, user: str) -> List[Dict]:
        """Normalize raw CSV rows into metric records.

        Returns list of dicts with normalized metrics per sprint/user.
        """
        records = []

        # Group by sprint to aggregate metrics
        for sprint, group in df.groupby('SPRINT'):
            # Aggregate numeric columns (sum across all PRs/tickets in sprint)
            record = {
                'team': team,
                'user': user,
                'sprint': sprint,
                'start_date': group['START_DATE'].iloc[0] if 'START_DATE' in group.columns else None,
                'end_date': group['END_DATE'].iloc[0] if 'END_DATE' in group.columns else None,
            }

            # Core metrics (sum)
            metrics = {
                'pr_added': 0,
                'pr_removed': 0,
                'commits': 0,
                'approved': 0,
                'commented_on_others': 0,
                'others_commented': 0,
                'tickets_closed': 0,
                'prs_merged': 0,
            }

            for _, row in group.iterrows():
                metrics['pr_added'] += int(row['PR_ADDED']) if pd.notna(row['PR_ADDED']) and str(row['PR_ADDED']).replace('*', '').strip() else 0
                metrics['pr_removed'] += int(row['PR_REMOVED']) if pd.notna(row['PR_REMOVED']) and str(row['PR_REMOVED']).replace('*', '').strip() else 0
                metrics['commits'] += int(row['COMMITS']) if pd.notna(row['COMMITS']) else 0
                metrics['approved'] += int(row['APPROVED']) if pd.notna(row['APPROVED']) else 0
                metrics['commented_on_others'] += int(row['COMMENTED_ON_OTHERS']) if pd.notna(row['COMMENTED_ON_OTHERS']) else 0
                metrics['others_commented'] += int(row['OTHERS_COMMENTED']) if pd.notna(row['OTHERS_COMMENTED']) else 0
                metrics['tickets_closed'] += int(row['TICKETS_CLOSED']) if pd.notna(row['TICKETS_CLOSED']) else 0

                # Count unique merged PRs
                if pd.notna(row['PR_STATUS']) and row['PR_STATUS'].upper() == 'MERGED':
                    metrics['prs_merged'] += 1

            # Derived metrics
            metrics['code_volume'] = metrics['pr_added'] + metrics['pr_removed']
            metrics['reviews_given'] = metrics['approved'] + metrics['commented_on_others']

            record.update(metrics)
            records.append(record)

        return records

    def get_all_teams(self) -> List[str]:
        """Return sorted list of all team names."""
        return sorted(self.teams.keys())

    def get_team_members(self, team: str) -> List[str]:
        """Return sorted list of members in a team."""
        return sorted(self.teams.get(team, []))

    def get_team_metrics(self, team: str, start_date: Optional[str] = None,
                        end_date: Optional[str] = None) -> List[Dict]:
        """Get aggregated metrics for all members of a team.

        Args:
            team: Team name
            start_date: Optional filter (format: YYYY-MM-DD or sprint name)
            end_date: Optional filter

        Returns:
            List of metric dicts aggregated by person
        """
        team_data = [r for r in self.data if r['team'] == team]

        if start_date or end_date:
            team_data = self._filter_by_date(team_data, start_date, end_date)

        # Aggregate by user
        aggregated = defaultdict(lambda: {
            'team': team,
            'user': '',
            'pr_added': 0,
            'pr_removed': 0,
            'commits': 0,
            'approved': 0,
            'commented_on_others': 0,
            'others_commented': 0,
            'tickets_closed': 0,
            'prs_merged': 0,
            'reviews_given': 0,
            'code_volume': 0,
            'sprint_count': 0,
        })

        for record in team_data:
            user = record['user']
            aggregated[user]['user'] = user
            aggregated[user]['pr_added'] += record['pr_added']
            aggregated[user]['pr_removed'] += record['pr_removed']
            aggregated[user]['commits'] += record['commits']
            aggregated[user]['approved'] += record['approved']
            aggregated[user]['commented_on_others'] += record['commented_on_others']
            aggregated[user]['others_commented'] += record['others_commented']
            aggregated[user]['tickets_closed'] += record['tickets_closed']
            aggregated[user]['prs_merged'] += record['prs_merged']
            aggregated[user]['reviews_given'] += record['reviews_given']
            aggregated[user]['code_volume'] += record['code_volume']
            aggregated[user]['sprint_count'] += 1

        return list(aggregated.values())

    def get_org_metrics(self, start_date: Optional[str] = None,
                       end_date: Optional[str] = None) -> List[Dict]:
        """Get aggregated metrics across all teams."""
        org_data = self.data.copy()

        if start_date or end_date:
            org_data = self._filter_by_date(org_data, start_date, end_date)

        # Aggregate by user (across all teams)
        aggregated = defaultdict(lambda: {
            'user': '',
            'team': 'ORG',
            'pr_added': 0,
            'pr_removed': 0,
            'commits': 0,
            'approved': 0,
            'commented_on_others': 0,
            'others_commented': 0,
            'tickets_closed': 0,
            'prs_merged': 0,
            'reviews_given': 0,
            'code_volume': 0,
            'sprint_count': 0,
        })

        for record in org_data:
            user = record['user']
            aggregated[user]['user'] = user
            aggregated[user]['pr_added'] += record['pr_added']
            aggregated[user]['pr_removed'] += record['pr_removed']
            aggregated[user]['commits'] += record['commits']
            aggregated[user]['approved'] += record['approved']
            aggregated[user]['commented_on_others'] += record['commented_on_others']
            aggregated[user]['others_commented'] += record['others_commented']
            aggregated[user]['tickets_closed'] += record['tickets_closed']
            aggregated[user]['prs_merged'] += record['prs_merged']
            aggregated[user]['reviews_given'] += record['reviews_given']
            aggregated[user]['code_volume'] += record['code_volume']
            aggregated[user]['sprint_count'] += 1

        return list(aggregated.values())

    def get_individual_history(self, team: str, user: str) -> List[Dict]:
        """Get sprint-by-sprint history for a specific person."""
        return [r for r in self.data if r['team'] == team and r['user'] == user]

    def get_unique_sprints(self) -> List[str]:
        """Get all unique sprint names, sorted."""
        sprints = set(r['sprint'] for r in self.data if r.get('sprint'))
        return sorted(sprints)

    @staticmethod
    def _filter_by_date(data: List[Dict], start_date: Optional[str],
                       end_date: Optional[str]) -> List[Dict]:
        """Filter records by date range. Supports sprint name or date format."""
        filtered = data

        if start_date:
            # If it looks like a sprint name (contains 'S' or 'Q'), filter by sprint name
            if any(c.isalpha() for c in start_date.upper()):
                filtered = [r for r in filtered if r.get('sprint', '').lower() >= start_date.lower()]
            else:
                # Try to parse as date
                try:
                    start_dt = datetime.strptime(start_date, "%Y-%m-%d")
                    filtered = [r for r in filtered if r.get('start_date') and
                              datetime.strptime(r['start_date'], "%m/%d/%y") >= start_dt]
                except ValueError:
                    pass

        if end_date:
            if any(c.isalpha() for c in end_date.upper()):
                filtered = [r for r in filtered if r.get('sprint', '').lower() <= end_date.lower()]
            else:
                try:
                    end_dt = datetime.strptime(end_date, "%Y-%m-%d")
                    filtered = [r for r in filtered if r.get('end_date') and
                              datetime.strptime(r['end_date'], "%m/%d/%y") <= end_dt]
                except ValueError:
                    pass

        return filtered
