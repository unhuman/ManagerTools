#!/usr/bin/env python3
"""
Sprint Report Visualizer — Generate PNG charts from individual analysis CSV files.

Usage:
    python -m managertools.sprint_report_visualizer [--reports-dir ./reports/] [--output-dir ./reports/] [--team TEAM_NAME] [--skip-individual] [--skip-team]
"""

import argparse
import glob
import os
from pathlib import Path
from collections import defaultdict
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
from matplotlib.patches import Polygon
import re


def parse_csv(filepath):
    """Load CSV and return DataFrame, filtering out totals rows."""
    df = pd.read_csv(filepath)
    # Filter out non-data rows
    df = df[~df['SPRINT'].isin(['Sprint Totals', 'Overall Totals'])]
    df = df[df['SPRINT'].notna()]
    df = df[df['SPRINT'].str.strip() != '']
    return df.reset_index(drop=True)


def extract_team_and_user(filename):
    """Extract team and user from 'individualAnalysis-{Team}-{User}.csv'."""
    basename = Path(filename).stem
    match = re.match(r'individualAnalysis-(.+?)-(.+)$', basename)
    if match:
        return match.group(1), match.group(2)
    return None, None


def safe_int(val):
    """Convert value to int, handling empty strings and non-numeric."""
    if pd.isna(val) or val == '':
        return 0
    if isinstance(val, str):
        val = val.strip()
        if val == '':
            return 0
    try:
        return int(float(str(val)))
    except (ValueError, TypeError):
        return 0


def aggregate_metrics(df, user):
    """Aggregate metrics by sprint for a specific user."""
    # Case-insensitive user matching
    user_data = df[df['USER'].str.lower() == user.lower()].copy()

    # Coerce columns to numeric
    numeric_cols = ['PR_ADDED', 'PR_REMOVED', 'COMMITS', 'APPROVED',
                    'COMMENTED_ON_OTHERS', 'OTHERS_COMMENTED']
    for col in numeric_cols:
        if col in user_data.columns:
            user_data[col] = user_data[col].apply(safe_int)
        else:
            user_data[col] = 0

    # Group by sprint and sum
    grouped = user_data.groupby('SPRINT')[numeric_cols].sum()

    # Calculate derived metrics
    grouped['Code Volume'] = grouped['PR_ADDED'] + grouped['PR_REMOVED']
    grouped['Reviews Given'] = grouped['APPROVED'] + grouped['COMMENTED_ON_OTHERS']
    grouped['Engagement Received'] = grouped['OTHERS_COMMENTED']

    # Derive PR counts from MERGED/OPENED columns and AUTHOR
    # Only count PRs the user authored (not ones they reviewed)
    authored = user_data[user_data['AUTHOR'].str.lower() == user.lower()]

    # PRs Merged: count distinct PR_IDs per sprint where MERGED is non-blank
    merged_prs = (authored[authored['MERGED'].notna() & (authored['MERGED'] != '')]
                  .groupby('SPRINT')['PR_ID'].nunique()
                  .rename('PRs Merged'))

    # PRs Opened: count distinct PR_IDs per sprint (authored by user)
    opened_prs = (authored.groupby('SPRINT')['PR_ID'].nunique()
                  .rename('PRs Opened'))

    # Join PR counts into grouped data
    grouped = grouped.join(merged_prs, how='left').join(opened_prs, how='left')
    grouped[['PRs Merged', 'PRs Opened']] = grouped[['PRs Merged', 'PRs Opened']].fillna(0).astype(int)

    return grouped.rename(columns={
        'COMMITS': 'Commits'
    })


def load_reports(reports_dir):
    """Load all individual CSV files and organize by team."""
    teams = defaultdict(lambda: defaultdict(pd.DataFrame))

    csv_files = glob.glob(os.path.join(reports_dir, 'individualAnalysis-*.csv'))

    for filepath in csv_files:
        team, user = extract_team_and_user(filepath)
        if team is None or user is None:
            continue

        df = parse_csv(filepath)
        if df.empty:
            continue

        teams[team][user] = df

    return teams


def calculate_overall_totals(df, user):
    """Calculate overall totals across all sprints for a user."""
    aggregated = aggregate_metrics(df, user)
    if aggregated.empty:
        return {'Code Volume': 0, 'Commits': 0, 'PRs Merged': 0, 'Reviews Given': 0, 'Engagement Received': 0}
    return aggregated[['Code Volume', 'Commits', 'PRs Merged', 'Reviews Given', 'Engagement Received']].sum().to_dict()


def create_team_heatmap(ax, team_members, metrics_dict):
    """Create heatmap of team metrics."""
    metric_names = ['Code Volume', 'Commits', 'PRs Merged', 'Reviews Given', 'Engagement Received']

    # Build matrix: members × metrics
    data = []
    member_names = []
    for member in team_members:
        if member in metrics_dict:
            row = [metrics_dict[member].get(m, 0) for m in metric_names]
            data.append(row)
            member_names.append(member)

    if not data:
        ax.text(0.5, 0.5, 'No data', ha='center', va='center', transform=ax.transAxes)
        return

    data = np.array(data, dtype=float)

    # Normalize by column
    data_normalized = np.zeros_like(data)
    for i in range(data.shape[1]):
        col_max = data[:, i].max()
        if col_max > 0:
            data_normalized[:, i] = data[:, i] / col_max

    im = ax.imshow(data_normalized, cmap='YlOrRd', aspect='auto', vmin=0, vmax=1)

    ax.set_xticks(range(len(metric_names)))
    ax.set_xticklabels(metric_names, rotation=45, ha='right')
    ax.set_yticks(range(len(member_names)))
    ax.set_yticklabels(member_names)

    # Annotate cells with raw values
    for i in range(len(member_names)):
        for j in range(len(metric_names)):
            text = ax.text(j, i, f'{int(data[i, j])}', ha='center', va='center', color='black', fontsize=8)

    ax.set_title('Team Members — Overall Totals', fontsize=10, fontweight='bold')
    plt.colorbar(im, ax=ax, label='Normalized')


def create_team_bar_charts(ax, team_members, metrics_dict):
    """Create side-by-side bar charts for each metric."""
    metric_names = ['Code Volume', 'Commits', 'PRs Merged', 'Reviews Given', 'Engagement Received']

    # Prepare data
    member_names = [m for m in team_members if m in metrics_dict]
    colors = plt.cm.tab20(np.linspace(0, 1, len(member_names)))

    x = np.arange(len(member_names))
    width = 0.8

    for idx, metric in enumerate(metric_names):
        sub_ax = plt.subplot(1, 5, idx + 1)
        values = [metrics_dict[m].get(metric, 0) for m in member_names]
        sub_ax.bar(x, values, width, color=colors)
        sub_ax.set_ylabel(metric, fontsize=9)
        sub_ax.set_xticks(x)
        sub_ax.set_xticklabels(member_names, rotation=45, ha='right', fontsize=8)
        sub_ax.grid(axis='y', alpha=0.3)


def create_radar_chart(ax, member_name, metrics, team_max):
    """Create a single radar chart for a team member."""
    metric_names = ['Code Volume', 'Commits', 'PRs Merged', 'Reviews Given', 'Engagement Received']

    values = [metrics.get(m, 0) for m in metric_names]

    # Normalize by team max
    normalized_values = []
    for i, v in enumerate(values):
        max_val = team_max[i]
        normalized_values.append(v / max_val if max_val > 0 else 0)

    # Close the polygon
    values_plot = normalized_values + [normalized_values[0]]
    angles = np.linspace(0, 2 * np.pi, len(metric_names), endpoint=False).tolist()
    angles += [angles[0]]

    ax.plot(angles, values_plot, 'o-', linewidth=1.5, markersize=4, color='steelblue')
    ax.fill(angles, values_plot, alpha=0.25, color='steelblue')
    ax.set_xticks(angles[:-1])

    # Multi-line labels for radar charts to prevent horizontal overlap
    radar_labels = ['Code\nVolume', 'Commits', 'PRs\nMerged', 'Reviews\nGiven', 'Engagement\nReceived']
    ax.set_xticklabels(radar_labels, fontsize=5)
    ax.xaxis.set_tick_params(pad=6)
    ax.set_ylim(0, 1.0)
    ax.set_title(member_name, fontsize=7, fontweight='bold', pad=8)
    ax.grid(True, alpha=0.3)
    ax.set_yticks([0.25, 0.5, 0.75, 1.0])
    ax.set_yticklabels(['', '', '', ''], fontsize=5)


def generate_team_png(team_name, team_dataframes, output_dir):
    """Generate comprehensive team report PNG."""
    # Calculate overall metrics for each member
    metrics_dict = {}
    team_members = sorted(team_dataframes.keys())

    for user in team_members:
        df = team_dataframes[user]
        metrics_dict[user] = calculate_overall_totals(df, user)

    # Sort by code volume descending
    team_members = sorted(team_members, key=lambda m: metrics_dict[m].get('Code Volume', 0), reverse=True)

    metric_names = ['Code Volume', 'Commits', 'PRs Merged', 'Reviews Given', 'Engagement Received']
    team_max = [max([metrics_dict[m].get(metric, 0) for m in team_members]) for metric in metric_names]
    team_max = [max(1, m) for m in team_max]

    n_members = len(team_members)
    n_spider_cols = 4
    n_spider_rows = (n_members + n_spider_cols - 1) // n_spider_cols

    # Calculate figure height based on content
    heatmap_height = 2.5
    bar_height = 2.5
    spider_height = 3 * n_spider_rows
    total_height = heatmap_height + bar_height + spider_height + 2

    # Create single figure with clear GridSpec
    fig = plt.figure(figsize=(18, total_height))
    n_gs_rows = 2 + n_spider_rows
    height_ratios = [heatmap_height, bar_height] + [3] * n_spider_rows
    gs = gridspec.GridSpec(
        n_gs_rows, 5,
        height_ratios=height_ratios,
        hspace=0.8,
        wspace=0.4
    )

    # Section 1: Heatmap (top, spans all 5 columns)
    ax_heatmap = fig.add_subplot(gs[0, :])
    create_team_heatmap(ax_heatmap, team_members, metrics_dict)

    # Section 2: Bar charts (middle, 5 columns for 5 metrics)
    member_names = team_members
    colors = plt.cm.tab20(np.linspace(0, 1, len(member_names)))
    x = np.arange(len(member_names))
    width = 0.8

    for idx, metric in enumerate(metric_names):
        ax_bar = fig.add_subplot(gs[1, idx])
        values = [metrics_dict[m].get(metric, 0) for m in member_names]
        ax_bar.bar(x, values, width, color=colors)
        ax_bar.set_title(metric, fontsize=8, fontweight='bold', pad=5)
        ax_bar.set_xticks(x)
        ax_bar.set_xticklabels(member_names, rotation=45, ha='right', fontsize=7)
        ax_bar.grid(axis='y', alpha=0.3)
        ax_bar.tick_params(axis='y', labelsize=7)

    # Section 3: Radar charts (bottom, arranged in grid)
    for idx, member in enumerate(team_members):
        row = 2 + (idx // n_spider_cols)
        col = idx % n_spider_cols
        ax_spider = fig.add_subplot(gs[row, col], projection='polar')
        create_radar_chart(ax_spider, member, metrics_dict[member], team_max)

    plt.suptitle(f'{team_name} — Overall Totals Report', fontsize=16, fontweight='bold', y=0.98)

    # Save
    output_path = os.path.join(output_dir, f'team-{team_name}.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

    return output_path


def generate_individual_png(user, df, team_name, output_dir):
    """Generate per-person sprint-over-sprint report PNG."""
    aggregated = aggregate_metrics(df, user)

    if aggregated.empty:
        return None

    # Sprint names as x-axis labels
    sprint_names = aggregated.index.tolist()
    x = np.arange(len(sprint_names))

    # Create 2x3 grid
    fig, axes = plt.subplots(2, 3, figsize=(14, 10))
    axes = axes.flatten()

    # 1. Code Volume
    ax = axes[0]
    ax.bar(x, aggregated['PR_ADDED'], label='Added', color='green', alpha=0.7)
    ax.bar(x, aggregated['PR_REMOVED'], bottom=aggregated['PR_ADDED'], label='Removed', color='red', alpha=0.7)
    ax.set_ylabel('Lines')
    ax.set_title('Code Volume')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    # 2. Commits
    ax = axes[1]
    ax.bar(x, aggregated['Commits'], color='steelblue')
    ax.set_ylabel('Count')
    ax.set_title('Commits')
    ax.grid(axis='y', alpha=0.3)

    # 3. PRs Opened vs Merged
    ax = axes[2]
    width = 0.35
    ax.bar(x - width/2, aggregated['PRs Opened'], width, label='Opened', color='steelblue', alpha=0.7)
    ax.bar(x + width/2, aggregated['PRs Merged'], width, label='Merged', color='green', alpha=0.7)
    ax.set_ylabel('Count')
    ax.set_title('PRs Opened / Merged')
    ax.legend()
    ax.grid(axis='y', alpha=0.3)

    # 4. Reviews Given
    ax = axes[3]
    ax.plot(x, aggregated['Reviews Given'], marker='o', linestyle='-', color='steelblue', linewidth=2)
    ax.fill_between(x, aggregated['Reviews Given'], alpha=0.3, color='steelblue')
    ax.set_ylabel('Count')
    ax.set_title('Reviews Given')
    ax.grid(alpha=0.3)

    # 5. Engagement Received
    ax = axes[4]
    ax.plot(x, aggregated['Engagement Received'], marker='s', linestyle='-', color='orange', linewidth=2)
    ax.fill_between(x, aggregated['Engagement Received'], alpha=0.3, color='orange')
    ax.set_ylabel('Count')
    ax.set_title('Engagement Received')
    ax.grid(alpha=0.3)

    # 6. Summary stats
    ax = axes[5]
    ax.axis('off')

    totals = {
        'Code Volume': aggregated['Code Volume'].sum(),
        'Commits': aggregated['Commits'].sum(),
        'PRs Opened': aggregated['PRs Opened'].sum(),
        'PRs Merged': aggregated['PRs Merged'].sum(),
        'Reviews Given': aggregated['Reviews Given'].sum(),
        'Engagement Received': aggregated['Engagement Received'].sum()
    }

    top_sprints = aggregated['Code Volume'].nlargest(3)

    summary_text = f"Overall Totals:\n"
    for metric, val in totals.items():
        summary_text += f"{metric}: {int(val)}\n"
    summary_text += f"\nTop 3 Sprints (by Code Volume):\n"
    for i, (sprint, val) in enumerate(top_sprints.items(), 1):
        summary_text += f"{i}. {sprint}: {int(val)}\n"

    ax.text(0.1, 0.9, summary_text, transform=ax.transAxes, fontsize=10, verticalalignment='top', family='monospace')

    # Set x-axis labels for all
    for ax in axes[:5]:
        ax.set_xticks(x)
        ax.set_xticklabels(sprint_names, rotation=45, ha='right', fontsize=8)

    plt.suptitle(f'{user} ({team_name}) — Sprint Activity Report', fontsize=12, fontweight='bold')
    plt.tight_layout()

    # Save
    output_path = os.path.join(output_dir, f'individual-{team_name}-{user}-report.png')
    plt.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close()

    return output_path


def main():
    parser = argparse.ArgumentParser(description='Generate PNG reports from individual analysis CSVs.')
    parser.add_argument('--reports-dir', default='./reports/', help='Directory containing individual CSV files')
    parser.add_argument('--output-dir', default='./reports/', help='Directory to save PNG reports')
    parser.add_argument('--team', help='Only process a specific team')
    parser.add_argument('--skip-individual', action='store_true', help='Skip individual PNG generation')
    parser.add_argument('--skip-team', action='store_true', help='Skip team PNG generation')

    args = parser.parse_args()

    # Create output dir if needed
    os.makedirs(args.output_dir, exist_ok=True)

    # Load all reports
    print(f"Loading reports from {args.reports_dir}...")
    teams = load_reports(args.reports_dir)

    if args.team:
        teams = {args.team: teams.get(args.team, {})}

    print(f"Found {len(teams)} teams")

    # Generate reports
    total_individual = 0
    total_team = 0

    for team_name in sorted(teams.keys()):
        print(f"\nProcessing team: {team_name}")
        team_dataframes = teams[team_name]

        if not team_dataframes:
            print(f"  No data for {team_name}")
            continue

        # Generate team PNG
        if not args.skip_team:
            try:
                output_path = generate_team_png(team_name, team_dataframes, args.output_dir)
                print(f"  ✓ Generated {output_path}")
                total_team += 1
            except Exception as e:
                print(f"  ✗ Error generating team PNG: {e}")

        # Generate individual PNGs
        if not args.skip_individual:
            for user in sorted(team_dataframes.keys()):
                try:
                    df = team_dataframes[user]
                    output_path = generate_individual_png(user, df, team_name, args.output_dir)
                    if output_path:
                        print(f"  ✓ Generated {output_path}")
                        total_individual += 1
                except Exception as e:
                    print(f"  ✗ Error for {user}: {e}")

    print(f"\n✓ Done! Generated {total_team} team reports and {total_individual} individual reports.")


if __name__ == '__main__':
    main()
