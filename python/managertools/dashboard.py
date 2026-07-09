#!/usr/bin/env python3
"""Interactive Streamlit dashboard for team metrics, productivity analysis, and peer comparison.

Features:
- Team productivity: View metrics for individual team members (code volume, reviews, etc.)
- Organization overview: Aggregate metrics across all teams
- Compare by title: View same-title peer metrics across the organization (requires Backstage integration)
- Performance reviews: Generate and export team member performance reviews

Requires backstageServer config for role/title data (optional; dashboard works without it but
"Compare by Title" view won't be available).
"""

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
from rest.backstage_rest import BackstageREST
from util.backstage_cache import BackstageCache
from util.command_line_helper import CommandLineHelper
from review_generator import PerformanceReviewGenerator
from review_exporters import ReviewExporterFactory


def init_aggregator(reports_dir: str) -> MetricsAggregator:
    """Initialize and cache the metrics aggregator."""
    if 'aggregator' not in st.session_state:
        # Try to initialize Backstage client for role data (only if already configured, no prompting)
        backstage_rest = None
        try:
            # Suppress prompts by setting quiet mode
            CommandLineHelper.set_quiet_mode_no_prompts()
            config_helper = CommandLineHelper(".managerTools.cfg")
            # Check if Backstage is configured WITHOUT prompting
            config_mgr = config_helper.get_config_file_manager()
            print(f"[DEBUG] Config manager: {config_mgr}")
            if config_mgr:
                print(f"[DEBUG] Config file exists, checking for backstageServer key")
                if config_mgr.contains_key("backstageServer"):
                    backstage_server = config_mgr.get_value("backstageServer")
                    backstage_auth = config_mgr.get_value("backstageAuth") if config_mgr.contains_key("backstageAuth") else ""
                    backstage_rest = BackstageREST(backstage_server, backstage_auth)
                    print(f"[DEBUG] Backstage initialized: {backstage_server}")
                else:
                    print("[DEBUG] backstageServer key not found in config file")
            else:
                print("[DEBUG] Config file not found or not readable")
        except Exception as e:
            # Silent failure - Backstage is optional
            print(f"[DEBUG] Backstage initialization failed: {e}")

        st.session_state.aggregator = MetricsAggregator(reports_dir, backstage_rest)
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
    st.dataframe(df_display, width='stretch', hide_index=True)

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
                st.plotly_chart(fig, width='stretch')

    st.divider()

    # Export performance review
    st.markdown("#### Export Performance Review")
    export_user = st.selectbox(
        "Select team member for export:",
        options=[m.get('user') for m in team_metrics],
        key="team_export_user"
    )

    if export_user:
        col_fmt, col_btn = st.columns([1, 1])
        with col_fmt:
            export_format = st.selectbox(
                "Format:",
                options=ReviewExporterFactory.supported_formats(),
                key="team_export_format"
            )

        with col_btn:
            if st.button("Generate Export", key="team_export_btn"):
                try:
                    generator = PerformanceReviewGenerator(agg, export_user, team_name, "Current")
                    review_data = generator.generate()

                    if 'error' not in review_data:
                        exporter = ReviewExporterFactory.create(export_format)
                        exported = exporter.export(review_data)

                        # Prepare download filename and MIME type
                        filename = f"{team_name}-{export_user}-review.{export_format}"
                        if export_format == 'pdf':
                            filename = filename.replace('.pdf', '.html')
                            mime_type = "text/html"
                        elif export_format == 'json':
                            mime_type = "application/json"
                        elif export_format == 'markdown':
                            mime_type = "text/markdown"
                        elif export_format == 'png':
                            mime_type = "image/png"
                        else:
                            mime_type = "application/octet-stream"

                        st.success(f"✓ Review generated ({export_format})")
                        st.download_button(
                            label=f"Download {export_format.upper()}",
                            data=exported,
                            file_name=filename,
                            mime=mime_type
                        )
                    else:
                        st.error(f"Failed to generate review: {review_data.get('error')}")
                except Exception as e:
                    st.error(f"Export failed: {e}")

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

        # Debug: show the actual values and underlying metrics
        st.write(f"**Debug - {selected_for_radar}:** Productivity={values[0]}, Review Quality={values[1]}, Collaboration={values[2]}")
        st.write(f"  Underlying: code_volume={selected_metrics.get('code_volume', 0)}, commits={selected_metrics.get('commits', 0)}, reviews_given={selected_metrics.get('reviews_given', 0)}, others_commented={selected_metrics.get('others_commented', 0)}")

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
        st.plotly_chart(fig_radar, width='stretch')


def render_title_comparison_view(agg: MetricsAggregator):
    """Render org-wide comparison by job title/role."""
    st.subheader("👔 Compare by Title/Role")

    if not agg.role_map:
        st.info("Role data not available. Ensure Backstage is configured and backstage_inspect found title fields.")
        return

    # Get titles that have representation in loaded reports (filter out titles with no users in data)
    representable_titles = agg.get_representable_titles()
    if not representable_titles:
        st.warning("No roles found in loaded team data. Ensure sprint reports are loaded for teams with Backstage data.")
        return

    # Let user pick a title
    selected_title = st.selectbox(
        "Select Role/Title:",
        options=representable_titles
    )

    if not selected_title:
        st.info("Select a title to compare")
        return

    # Get metrics for all users with this title
    title_metrics = agg.get_users_by_title(selected_title)
    if not title_metrics:
        st.warning(f"No users found with title: {selected_title}")
        return

    st.markdown(f"#### {selected_title} ({len(title_metrics)} people)")

    # KPIs
    col1, col2, col3, col4, col5 = st.columns(5)
    total_code_vol = sum(m.get('code_volume', 0) for m in title_metrics)
    total_commits = sum(m.get('commits', 0) for m in title_metrics)
    total_reviews = sum(m.get('reviews_given', 0) for m in title_metrics)
    total_tickets = sum(m.get('tickets_closed', 0) for m in title_metrics)

    with col1:
        st.metric("Total Code Volume", f"{total_code_vol:,}")
    with col2:
        st.metric("Total Commits", total_commits)
    with col3:
        st.metric("Total Reviews", total_reviews)
    with col4:
        st.metric("Total Tickets", total_tickets)
    with col5:
        st.metric("Count", len(title_metrics))

    st.divider()

    # Individual metrics table
    st.markdown("#### Individual Contribution")
    display_data = []
    for m in sorted(title_metrics, key=lambda x: x.get('code_volume', 0), reverse=True):
        productivity = ProductivityMetrics.calculate_productivity_score(m)
        review_quality = ProductivityMetrics.calculate_review_quality_score(m)
        collab = ProductivityMetrics.calculate_collaboration_score(m)

        display_data.append({
            'User': m.get('user'),
            'Team': m.get('team'),
            'Code Volume': m.get('code_volume', 0),
            'Commits': m.get('commits', 0),
            'Reviews Given': m.get('reviews_given', 0),
            'Tickets': m.get('tickets_closed', 0),
            'Productivity': f"{productivity:.0f}",
            'Review Quality': f"{review_quality:.0f}",
            'Collaboration': f"{collab:.0f}",
        })

    df_display = pd.DataFrame(display_data)
    st.dataframe(df_display, width='stretch', hide_index=True)

    st.divider()

    # Export performance review
    st.markdown("#### Export Performance Review")
    export_user = st.selectbox(
        "Select person for export:",
        options=[m.get('user') for m in title_metrics],
        key="title_export_user"
    )

    if export_user:
        # Find team for this user
        user_match = next((m for m in title_metrics if m.get('user') == export_user), None)
        if user_match:
            user_team = user_match.get('team')

            col_fmt, col_btn = st.columns([1, 1])
            with col_fmt:
                export_format = st.selectbox(
                    "Format:",
                    options=ReviewExporterFactory.supported_formats(),
                    key="title_export_format"
                )

            with col_btn:
                if st.button("Generate Export", key="title_export_btn"):
                    try:
                        generator = PerformanceReviewGenerator(agg, export_user, user_team, f"{selected_title} Peer Comparison")
                        review_data = generator.generate()

                        if 'error' not in review_data:
                            exporter = ReviewExporterFactory.create(export_format)
                            exported = exporter.export(review_data)

                            # Prepare download filename and MIME type
                            filename = f"{export_user}-review-{selected_title}.{export_format}"
                            if export_format == 'pdf':
                                filename = filename.replace('.pdf', '.html')
                                mime_type = "text/html"
                            elif export_format == 'json':
                                mime_type = "application/json"
                            elif export_format == 'markdown':
                                mime_type = "text/markdown"
                            elif export_format == 'png':
                                mime_type = "image/png"
                            else:
                                mime_type = "application/octet-stream"

                            st.success(f"✓ Review generated ({export_format})")
                            st.download_button(
                                label=f"Download {export_format.upper()}",
                                data=exported,
                                file_name=filename,
                                mime=mime_type
                            )
                        else:
                            st.error(f"Failed to generate review: {review_data.get('error')}")
                    except Exception as e:
                        st.error(f"Export failed: {e}")

    st.divider()

    # Trend comparison for selected users
    st.markdown("#### Productivity Trends")
    selected_users = st.multiselect(
        "Select people to compare trends:",
        options=[m.get('user') for m in title_metrics],
        default=[m.get('user') for m in title_metrics[:3]] if len(title_metrics) >= 3 else []
    )

    if selected_users:
        fig_data = []
        for user in selected_users:
            # Find team+user combo
            user_match = next((m for m in title_metrics if m.get('user') == user), None)
            if user_match:
                team = user_match.get('team')
                history = agg.get_individual_history(team, user)
                if history:
                    sprints = [h.get('sprint', '') for h in history]
                    code_vols = [h.get('code_volume', 0) for h in history]
                    fig_data.append(go.Scatter(
                        x=sprints, y=code_vols, mode='lines+markers',
                        name=f"{user} ({team})",
                        hovertemplate='<b>%{fullData.name}</b><br>%{x}<br>Code Volume: %{y}<extra></extra>'
                    ))

        if fig_data:
            fig = go.Figure(data=fig_data)
            fig.update_layout(
                title=f"Code Volume Trends ({selected_title})",
                xaxis_title="Sprint",
                yaxis_title="Code Volume",
                hovermode='x unified',
                height=400
            )
            st.plotly_chart(fig, width='stretch')


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
    st.dataframe(df_teams, width='stretch', hide_index=True)

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
    view = st.sidebar.radio("Select View:", ["Team Analysis", "Organization Overview", "Compare by Title"])

    if view == "Team Analysis":
        st.sidebar.divider()
        teams = agg.get_all_teams()
        selected_team = st.sidebar.selectbox("Select Team:", teams)

        if selected_team:
            render_team_view(agg, selected_team)

    elif view == "Organization Overview":
        render_org_view(agg)

    elif view == "Compare by Title":
        render_title_comparison_view(agg)

    # Footer
    st.divider()
    st.caption(f"Data loaded from: {reports_dir} | Total teams: {len(agg.get_all_teams())}")


if __name__ == "__main__":
    main()
