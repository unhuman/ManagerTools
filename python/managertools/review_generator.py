"""Performance review data generator."""

from typing import Dict, List, Optional, Any
from datetime import datetime
from productivity_metrics import ProductivityMetrics


class PerformanceReviewGenerator:
    """Generate structured performance review data for individuals."""

    def __init__(self, aggregator, person_user: str, person_team: str, period: str = "Current"):
        """Initialize review generator.

        Args:
            aggregator: MetricsAggregator instance
            person_user: Username
            person_team: Team name
            period: Period label (e.g., "Q3 2026", "Current", "Past Year")
        """
        self.aggregator = aggregator
        self.person_user = person_user
        self.person_team = person_team
        self.period = period

    def generate(self) -> Dict[str, Any]:
        """Generate complete performance review package.

        Returns:
            Dict with sections: person, period, metrics, peer_comparison, trends,
            inferred_insights, recommendations
        """
        # Get person's metrics
        person_metrics = self.aggregator.get_team_metrics(self.person_team)
        person_data = next((m for m in person_metrics if m.get('user') == self.person_user), None)

        if not person_data:
            return {'error': f'No data found for {self.person_user} in {self.person_team}'}

        # Get person's history for trends
        history = self.aggregator.get_individual_history(self.person_team, self.person_user)

        # Get peer comparison data
        team_metrics = self.aggregator.get_team_metrics(self.person_team)

        # Calculate metrics
        productivity_score = ProductivityMetrics.calculate_productivity_score(person_data)
        review_quality = ProductivityMetrics.calculate_review_quality_score(person_data)
        collab_score = ProductivityMetrics.calculate_collaboration_score(person_data)
        velocity_trend = ProductivityMetrics.calculate_velocity_trend(history)
        peer_comparison = ProductivityMetrics.compare_to_team_average(person_data, team_metrics, self.person_user)
        risks = ProductivityMetrics.identify_risk_indicators(person_data, history)
        strengths = ProductivityMetrics.identify_strengths(person_data, team_metrics, self.person_user)

        # Get role if available
        role = self.aggregator.get_role(self.person_team, self.person_user) or "Unknown"

        return {
            'person': {
                'name': self.person_user,
                'team': self.person_team,
                'role': role,
            },
            'period': self.period,
            'generated_at': datetime.now().isoformat(),
            'metrics': {
                'code_volume': person_data.get('code_volume', 0),
                'commits': person_data.get('commits', 0),
                'prs_merged': person_data.get('prs_merged', 0),
                'reviews_given': person_data.get('reviews_given', 0),
                'tickets_closed': person_data.get('tickets_closed', 0),
                'productivity_score': productivity_score,
                'review_quality_score': review_quality,
                'collaboration_score': collab_score,
            },
            'peer_comparison': peer_comparison,
            'velocity_trend': velocity_trend,
            'strengths': strengths,
            'risks': risks,
            'recommendations': self._generate_recommendations(
                productivity_score, review_quality, velocity_trend, risks, strengths
            ),
        }

    @staticmethod
    def _generate_recommendations(productivity: float, review_quality: float, velocity_trend: Dict,
                                 risks: List[str], strengths: List[str]) -> List[str]:
        """Generate actionable recommendations based on metrics."""
        recommendations = []

        # Productivity-based
        if productivity > 80:
            recommendations.append("High performer — consider for leadership or stretch assignments")
        elif productivity < 40:
            recommendations.append("Productivity below expected range — discuss blockers or support needs")

        # Review quality
        if review_quality > 70:
            recommendations.append("Strong code review culture — could mentor others or lead design reviews")
        elif review_quality < 30 and productivity > 50:
            recommendations.append("Consider more detailed code reviews to deepen engagement with team")

        # Velocity trend
        if velocity_trend.get('status') == 'calculated':
            metrics = velocity_trend.get('metrics', {})
            code_vol_trend = metrics.get('code_volume', {}).get('trend', '')
            if '↑' in code_vol_trend:
                recommendations.append("Upward productivity trend — momentum is positive")
            elif '↓' in code_vol_trend:
                recommendations.append("Declining productivity — check in to understand blockers or capacity issues")

        # Risk mitigation
        if risks:
            recommendations.append(f"Address the following risks: {'; '.join(risks)}")

        # Strength amplification
        if strengths:
            recommendations.append(f"Leverage strengths: {'; '.join(strengths)}")

        if not recommendations:
            recommendations.append("Performance is stable and in line with expectations")

        return recommendations
