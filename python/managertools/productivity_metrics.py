"""Productivity metrics and inference calculations."""

import pandas as pd
from typing import Dict, List, Optional, Tuple
from collections import defaultdict
from datetime import datetime


class ProductivityMetrics:
    """Calculate productivity trends, growth, and comparisons."""

    @staticmethod
    def calculate_productivity_score(metrics: Dict) -> float:
        """Calculate normalized productivity score (0-100).

        Composite of code volume, commits, and tickets closed.
        """
        code_vol = metrics.get('code_volume', 0)
        commits = metrics.get('commits', 0)
        tickets = metrics.get('tickets_closed', 0)

        # Normalize based on realistic org thresholds (P50/P75 range)
        # These should differentiate across your actual team distribution
        code_score = min(code_vol / 50000, 1.0) * 33  # up to 50k lines = 33 pts
        commit_score = min(commits / 400, 1.0) * 33   # up to 400 commits = 33 pts
        ticket_score = min(tickets / 30, 1.0) * 34    # up to 30 tickets = 34 pts

        return round(code_score + commit_score + ticket_score, 1)

    @staticmethod
    def calculate_review_quality_score(metrics: Dict) -> float:
        """Calculate code review quality (0-100).

        High comment/review ratio indicates deeper engagement.
        """
        reviews = metrics.get('approved', 0) + metrics.get('commented_on_others', 0)
        comments = metrics.get('commented_on_others', 0)

        if reviews == 0:
            return 0.0

        # Ratio of detailed reviews (comments) vs. approvals
        comment_ratio = comments / reviews if reviews > 0 else 0
        engagement_score = min(comment_ratio * 100, 100)

        return round(engagement_score, 1)

    @staticmethod
    def calculate_collaboration_score(metrics: Dict) -> float:
        """Calculate collaboration engagement (0-100).

        Based on reviews given and engagement received.
        """
        reviews = metrics.get('reviews_given', 0)
        engagement = metrics.get('others_commented', 0)

        # Normalize: reviews + engagement, with realistic threshold
        # Range typically 50-400+ combined; use 400 to differentiate
        collab = min((reviews + engagement) / 400, 1.0) * 100

        return round(collab, 1)

    @staticmethod
    def compare_to_team_average(person_metrics: Dict, team_metrics: List[Dict],
                               person_user: str) -> Dict[str, str]:
        """Compare person's metrics to team average.

        Returns percentile/comparison for key metrics.
        """
        if not team_metrics:
            return {}

        # Filter out the person to get true team average
        team_avg = {}
        for metric_key in ['code_volume', 'commits', 'prs_merged', 'reviews_given', 'tickets_closed']:
            values = [m.get(metric_key, 0) for m in team_metrics if m.get('user') != person_user]
            team_avg[metric_key] = sum(values) / len(values) if values else 0

        comparisons = {}
        for metric, team_value in team_avg.items():
            person_value = person_metrics.get(metric, 0)
            if team_value == 0:
                comparisons[metric] = "baseline"
            else:
                pct = (person_value / team_value - 1) * 100
                if pct > 20:
                    comparisons[metric] = f"↑{pct:.0f}% above team"
                elif pct < -20:
                    comparisons[metric] = f"↓{abs(pct):.0f}% below team"
                else:
                    comparisons[metric] = "~team average"

        return comparisons

    @staticmethod
    def calculate_velocity_trend(history: List[Dict]) -> Dict:
        """Calculate quarter-over-quarter productivity trend.

        Args:
            history: List of sprint records for a person

        Returns:
            Dict with current, previous, trend, and % change
        """
        if not history:
            return {'status': 'no_data'}

        # Separate by quarter (rough heuristic: count sprints)
        sorted_history = sorted(history, key=lambda x: x.get('sprint', ''))

        if len(sorted_history) < 2:
            return {'status': 'insufficient_data'}

        # Last 2 sprints = current, prior 2 = previous
        current = sorted_history[-2:] if len(sorted_history) >= 2 else sorted_history[-1:]
        previous = sorted_history[-4:-2] if len(sorted_history) >= 4 else sorted_history[:-2]

        if not previous:
            return {'status': 'insufficient_data'}

        def agg_metrics(records):
            total = {
                'code_volume': sum(r.get('code_volume', 0) for r in records),
                'commits': sum(r.get('commits', 0) for r in records),
                'tickets_closed': sum(r.get('tickets_closed', 0) for r in records),
            }
            return total

        current_metrics = agg_metrics(current)
        previous_metrics = agg_metrics(previous)

        # Calculate trend for each metric
        trends = {}
        for metric in ['code_volume', 'commits', 'tickets_closed']:
            prev_val = previous_metrics[metric]
            curr_val = current_metrics[metric]
            if prev_val == 0:
                trends[metric] = {'current': curr_val, 'previous': 0, 'trend': 'new' if curr_val > 0 else 'stable'}
            else:
                pct_change = ((curr_val - prev_val) / prev_val) * 100
                if pct_change > 10:
                    trend_dir = '↑ up'
                elif pct_change < -10:
                    trend_dir = '↓ down'
                else:
                    trend_dir = '→ stable'
                trends[metric] = {
                    'current': curr_val,
                    'previous': prev_val,
                    'pct_change': round(pct_change, 1),
                    'trend': trend_dir
                }

        return {
            'status': 'calculated',
            'current_sprints': [r.get('sprint', '') for r in current],
            'previous_sprints': [r.get('sprint', '') for r in previous],
            'metrics': trends
        }

    @staticmethod
    def rank_by_metric(team_metrics: List[Dict], metric: str, top_n: int = 5) -> List[Tuple[str, float]]:
        """Rank team members by a specific metric.

        Returns: List of (user, value) tuples
        """
        ranked = sorted(
            [(m.get('user'), m.get(metric, 0)) for m in team_metrics],
            key=lambda x: x[1],
            reverse=True
        )
        return ranked[:top_n]

    @staticmethod
    def identify_risk_indicators(person_metrics: Dict, history: List[Dict]) -> List[str]:
        """Identify potential risk or concern flags.

        Returns list of risk descriptions.
        """
        risks = []

        # Check for declining activity
        if len(history) >= 3:
            recent = history[-1].get('code_volume', 0)
            avg_prior = sum(h.get('code_volume', 0) for h in history[:-1]) / len(history[:-1])
            if avg_prior > 0 and recent < (avg_prior * 0.5):
                risks.append(f"Code volume down {((recent/avg_prior - 1)*100):.0f}% vs. average")

        # Check for minimal review participation
        reviews = person_metrics.get('reviews_given', 0)
        if reviews == 0 and person_metrics.get('sprint_count', 0) > 1:
            risks.append("No code reviews given (possible silos)")

        # Check for declining reviews
        if len(history) >= 2:
            recent_reviews = history[-1].get('reviews_given', 0)
            prev_reviews = history[-2].get('reviews_given', 0)
            if prev_reviews > 0 and recent_reviews < (prev_reviews * 0.3):
                risks.append("Review participation dropped significantly")

        return risks

    @staticmethod
    def identify_strengths(person_metrics: Dict, team_metrics: List[Dict], person_user: str) -> List[str]:
        """Identify strengths and contributions.

        Returns list of strength descriptions.
        """
        strengths = []

        # High productivity
        productivity = ProductivityMetrics.calculate_productivity_score(person_metrics)
        if productivity > 75:
            strengths.append(f"High productivity ({productivity:.0f}/100)")

        # Strong review culture
        review_quality = ProductivityMetrics.calculate_review_quality_score(person_metrics)
        if review_quality > 60:
            strengths.append(f"Strong code review engagement ({review_quality:.0f}/100)")

        # High collaboration
        collab = ProductivityMetrics.calculate_collaboration_score(person_metrics)
        if collab > 70:
            strengths.append(f"High collaboration and team engagement ({collab:.0f}/100)")

        # Ticket closer
        tickets = person_metrics.get('tickets_closed', 0)
        if tickets > 5:
            strengths.append(f"Strong ticket resolution ({tickets} closed)")

        return strengths
