#!/usr/bin/env python3
"""Interactive Streamlit dashboard for team metrics and productivity analysis."""

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from typing import List, Dict, Optional
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(__file__))

from metrics_aggregator import MetricsAggregator
from productivity_metrics import ProductivityMetrics


def init_aggregator(reports_dir: str) -> MetricsAggregator:
    """Initialize and cache the metrics aggregator."""
    if 'aggregator' not in st.session_state:
        st.session_state.aggregator = MetricsAggregator(reports_dir)
    return st.session_state.aggregator


def render_metric_card(label: str, value, delta: Optional[str] = None, delta_color: str = "off"):
    """Render a metric card with optional delta."""
    col_metric = st.columns(1)[0]
    with col_metric:
        if delta:
            st.metric(label, value, delta=delta, delta_color=delta_color)
        else:
            st.metric(label, value)


def render_team_view(agg: MetricsAggregator, team_name: str):
    """Render team-level metrics and member comparison."""
    st.subheader(f"📊 {team_name} Team Analytics")

    team_metrics = agg.get_team_metrics(team_name)
    if not team_metrics:
        st.warning(f"No data found for team: {team_name}")
        return

    # Team KPIs
    st.markdown("#### Team Summary")
    col1, col2, col3, col4, col5 = st.columns(5)

    total_code_vol = sum(m.get('code_volume', 0) for m in team_metrics)
    total_commits = sum(m.get('commits', 0) for m in team_metrics)
    total_reviews = sum(m.get('reviews_given', 0) for m in team_metrics)
    total_tickets = sum(m.get('tickets_closed', 0) for m in team_metrics)
    team_size = len(team_metrics)

    with col1:
        st.metric("Team Size", team_size)
    with col2:
        st.metric("Total Code Volume", f"{total_code_vol:,}")
    with col3:
        st.metric("Total Commits", total_commits)
    with col4:
        st.metric("Total Reviews", total_reviews)
    with col5:
        st.metric("Tickets Closed", total_tickets)

    st.divider()

    # Individual member comparison
    st.markdown("#### Individual Contributions")

    # Create dataframe for display
    display_data = []
    for m in sorted(team_metrics, key=lambda x: x.get('code_volume', 0), reverse=True):
        productivity = ProductivityMetrics.calculate_productivity_score(m)
        review_quality = ProductivityMetrics.calculate_review_quality_score(m)
        collab_score = ProductivityMetrics.calculate_collaboration_score(m)

        display_data.append({
            'User': m.get('user'),
            'Code Volume': m.get('code_volume', 0),
            'Commits': m.get('commits', 0),
            'PRs Merged': m.get('prs_merged', 0),
            'Reviews Given': m.get('reviews_given', 0),
            'Tickets Closed': m.get('tickets_closed', 0),
            'Productivity': f"{productivity:.0f}",
            'Review Quality': f"{review_quality:.0f}",
            'Collaboration': f"{collab_score:.0f}",
        })

    df_display = pd.DataFrame(display_data)
    st.dataframe(df_display, use_container_width=True, hide_index=True)

    # Productivity trend line chart
    st.markdown("#### Productivity Trends by Member")
    col_chart, col_select = st.columns([3, 1])

    with col_select:
        selected_users = st.multiselect(
            "Select members to compare:",
            options=[m.get('user') for m in team_metrics],
            default=[m.get('user') for m in team_metrics[:3]]
        )

    with col_chart:
        if selected_users:
            # Get sprint history for selected users
            fig_data = []
            for user in selected_users:
                history = agg.get_individual_history(team_name, user)
                if history:
                    sprints = [h.get('sprint', '') for h in history]
                    code_vols = [h.get('code_volume', 0) for h in history]
                    fig_data.append(go.Scatter(
                        x=sprints, y=code_vols, mode='lines+markers',
                        name=user, hovertemplate='<b>%{fullData.name}</b><br>%{x}<br>Code Volume: %{y}<extra></extra>'
                    ))

            if fig_data:
                fig = go.Figure(data=fig_data)
                fig.update_layout(
                    title="Code Volume by Sprint",
                    xaxis_title="Sprint",
                    yaxis_title="Code Volume (lines added + removed)",
                    hovermode='x unified',
                    height=400
                )
                st.plotly_chart(fig, use_container_width=True)

    st.divider()

    # Radar chart comparison (productivity aspects)
    st.markdown("#### Team Performance Radar")
    col_radar1, col_radar2 = st.columns(2)

    with col_radar1:
        selected_for_radar = st.selectbox(
            "Select member to compare:",
            options=[m.get('user') for m in team_metrics]
        )

    with col_radar2:
        st.empty()

    if selected_for_radar:
        selected_metrics = [m for m in team_metrics if m.get('user') == selected_for_radar][0]

        categories = ['Productivity', 'Review Quality', 'Collaboration']
        values = [
            ProductivityMetrics.calculate_productivity_score(selected_metrics),
            ProductivityMetrics.calculate_review_quality_score(selected_metrics),
            ProductivityMetrics.calculate_collaboration_score(selected_metrics),
        ]

        fig_radar = go.Figure(data=go.Scatterpolar(
            r=values,
            theta=categories,
            fill='toself',
            name=selected_for_radar
        ))

        fig_radar.update_layout(
            polar=dict(radialaxis=dict(visible=True, range=[0, 100])),
            height=400,
            showlegend=False
        )
        st.plotly_chart(fig_radar, use_container_width=True)


def render_org_view(agg: MetricsAggregator):
    """Render organization-wide metrics."""
    st.subheader("🏢 Organization-Wide Analytics")

    org_metrics = agg.get_org_metrics()
    if not org_metrics:
        st.warning("No organization data found")
        return

    # Org-wide KPIs
    st.markdown("#### Organization Summary")
    col1, col2, col3, col4, col5 = st.columns(5)

    total_code_vol = sum(m.get('code_volume', 0) for m in org_metrics)
    total_commits = sum(m.get('commits', 0) for m in org_metrics)
    total_reviews = sum(m.get('reviews_given', 0) for m in org_metrics)
    total_tickets = sum(m.get('tickets_closed', 0) for m in org_metrics)
    total_people = len(org_metrics)

    with col1:
        st.metric("Total People", total_people)
    with col2:
        st.metric("Total Code Volume", f"{total_code_vol:,}")
    with col3:
        st.metric("Total Commits", total_commits)
    with col4:
        st.metric("Total Reviews", total_reviews)
    with col5:
        st.metric("Tickets Closed", total_tickets)

    st.divider()

    # Team comparison
    st.markdown("#### Team Comparison")
    teams = agg.get_all_teams()
    team_comparison = []

    for team in teams:
        team_metrics = agg.get_team_metrics(team)
        if team_metrics:
            team_comparison.append({
                'Team': team,
                'Size': len(team_metrics),
                'Avg Code Volume': f"{sum(m.get('code_volume', 0) for m in team_metrics) / len(team_metrics):.0f}",
                'Total Commits': sum(m.get('commits', 0) for m in team_metrics),
                'Total Reviews': sum(m.get('reviews_given', 0) for m in team_metrics),
            })

    df_teams = pd.DataFrame(team_comparison)
    st.dataframe(df_teams, use_container_width=True, hide_index=True)

    st.divider()

    # Top performers
    st.markdown("#### Top Contributors")
    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("**By Code Volume**")
        top_code = ProductivityMetrics.rank_by_metric(org_metrics, 'code_volume', 5)
        for i, (user, value) in enumerate(top_code, 1):
            st.write(f"{i}. {user}: {value:,}")

    with col2:
        st.markdown("**By Commits**")
        top_commits = ProductivityMetrics.rank_by_metric(org_metrics, 'commits', 5)
        for i, (user, value) in enumerate(top_commits, 1):
            st.write(f"{i}. {user}: {value}")

    with col3:
        st.markdown("**By Reviews Given**")
        top_reviews = ProductivityMetrics.rank_by_metric(org_metrics, 'reviews_given', 5)
        for i, (user, value) in enumerate(top_reviews, 1):
            st.write(f"{i}. {user}: {value}")


def main():
    """Main dashboard application."""
    st.set_page_config(page_title="Team Productivity Dashboard", layout="wide")

    st.title("📈 Team Productivity Dashboard")

    # Find reports directory (current dir or parent)
    reports_dir = "."
    if not any(f.startswith("individual-") and f.endswith(".csv") for f in os.listdir(".")):
        # Try parent directory
        parent_dir = os.path.dirname(os.path.abspath(__file__))
        if any(f.startswith("individual-") and f.endswith(".csv") for f in os.listdir(parent_dir)):
            reports_dir = parent_dir

    # Initialize aggregator
    agg = init_aggregator(reports_dir)

    # Sidebar navigation
    st.sidebar.title("Navigation")
    view = st.sidebar.radio("Select View:", ["Team Analysis", "Organization Overview"])

    if view == "Team Analysis":
        st.sidebar.divider()
        teams = agg.get_all_teams()
        selected_team = st.sidebar.selectbox("Select Team:", teams)

        if selected_team:
            render_team_view(agg, selected_team)

    elif view == "Organization Overview":
        render_org_view(agg)

    # Footer
    st.divider()
    st.caption(f"Data loaded from: {reports_dir} | Total teams: {len(agg.get_all_teams())}")


if __name__ == "__main__":
    main()
