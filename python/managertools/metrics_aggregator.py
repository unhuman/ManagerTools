"""Metrics aggregator for team and org-wide analysis."""

import os
import glob
import pandas as pd
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
from collections import defaultdict
import sys


class MetricsAggregator:
    """Aggregates individual sprint report CSVs into queryable metrics."""

    def __init__(self, reports_dir: str = ".", backstage_rest=None):
        """Initialize aggregator with path to individual report CSVs.

        Args:
            reports_dir: Directory containing individual-{Team}-{User}.csv files
            backstage_rest: Optional BackstageREST client for loading role data
        """
        self.reports_dir = reports_dir
        self.data: List[Dict] = []
        self.teams: Dict[str, List[str]] = defaultdict(list)
        self.backstage_rest = backstage_rest
        self.role_map: Dict[Tuple[str, str], str] = {}  # (team, user) -> role
        self._load_reports()
        self._load_roles()

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
                # Helper to safely parse integers, handling spaces and empty values
                def safe_int(val, default=0):
                    if pd.notna(val):
                        cleaned = str(val).replace('*', '').strip()
                        if cleaned:
                            try:
                                return int(cleaned)
                            except ValueError:
                                return default
                    return default

                metrics['pr_added'] += safe_int(row['PR_ADDED'])
                metrics['pr_removed'] += safe_int(row['PR_REMOVED'])
                metrics['commits'] += safe_int(row['COMMITS'])
                metrics['approved'] += safe_int(row['APPROVED'])
                metrics['commented_on_others'] += safe_int(row['COMMENTED_ON_OTHERS'])
                metrics['others_commented'] += safe_int(row['OTHERS_COMMENTED'])
                metrics['tickets_closed'] += safe_int(row['TICKETS_CLOSED'])

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

    def _load_roles(self) -> None:
        """Load team member roles from Backstage if available."""
        if not self.backstage_rest:
            return

        for team in self.teams.keys():
            try:
                roster = self.backstage_rest.get_team_roster(team)
                for member in roster:
                    user_ref = member.get('user_ref', '').casefold()
                    raw_entity = member.get('raw_entity', {})
                    role = raw_entity.get('spec', {}).get('profile', {}).get('role')
                    if user_ref and role:
                        # Store by case-insensitive user name
                        self.role_map[(team, user_ref)] = role
            except Exception as e:
                print(f"[WARN] Failed to load roles for team {team}: {e}", file=sys.stderr)

    def get_role(self, team: str, user: str) -> Optional[str]:
        """Get a user's role/title by team and username.

        Args:
            team: Team name
            user: Username (case-insensitive)

        Returns:
            Role string, or None if not found
        """
        return self.role_map.get((team, user.casefold()))

    def get_users_by_title(self, title: str, start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> List[Dict]:
        """Get aggregated metrics for all users with a specific title/role.

        Args:
            title: Role title (e.g., "Architect", "Senior Software Engineer")
            start_date: Optional filter
            end_date: Optional filter

        Returns:
            List of metric dicts aggregated by person, filtered to matching titles
        """
        if not self.role_map:
            return []

        # Find all (team, user) pairs matching this title
        matching_users = set()
        for (team, user), role in self.role_map.items():
            if role.lower() == title.lower():
                matching_users.add((team, user))

        if not matching_users:
            return []

        # Aggregate metrics for matching users
        aggregated = defaultdict(lambda: {
            'user': '',
            'team': '',
            'role': title,
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

        filtered_data = self.data.copy()
        if start_date or end_date:
            filtered_data = self._filter_by_date(filtered_data, start_date, end_date)

        for record in filtered_data:
            team = record.get('team', '').casefold()
            user = record.get('user', '').casefold()
            if (team, user) in matching_users:
                key = (team, user, record.get('user'))  # Use original case for display
                aggregated[key]['user'] = record.get('user')
                aggregated[key]['team'] = record.get('team')
                aggregated[key]['pr_added'] += record.get('pr_added', 0)
                aggregated[key]['pr_removed'] += record.get('pr_removed', 0)
                aggregated[key]['commits'] += record.get('commits', 0)
                aggregated[key]['approved'] += record.get('approved', 0)
                aggregated[key]['commented_on_others'] += record.get('commented_on_others', 0)
                aggregated[key]['others_commented'] += record.get('others_commented', 0)
                aggregated[key]['tickets_closed'] += record.get('tickets_closed', 0)
                aggregated[key]['prs_merged'] += record.get('prs_merged', 0)
                aggregated[key]['reviews_given'] += record.get('reviews_given', 0)
                aggregated[key]['code_volume'] += record.get('code_volume', 0)
                aggregated[key]['sprint_count'] += 1

        return list(aggregated.values())

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
