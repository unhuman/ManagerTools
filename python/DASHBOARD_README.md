# Team Productivity Dashboard

A simple, interactive Streamlit dashboard for analyzing team and organization-wide productivity metrics, including productivity trends, peer comparisons, and growth insights.

## What's Included

### Core Modules

1. **`metrics_aggregator.py`** — Data aggregation layer
   - Loads all individual sprint report CSVs
   - Normalizes metrics by person/team/sprint
   - Provides queryable interface for team and org-wide metrics
   - Supports date/sprint filtering

2. **`productivity_metrics.py`** — Inference engine
   - **Productivity Score**: Composite metric (code volume + commits + tickets)
   - **Velocity Trend**: Current vs. previous period comparison
   - **Review Quality Score**: Indicator of code review engagement depth
   - **Collaboration Score**: Team engagement and review participation
   - **Risk Indicators**: Flags for declining activity, burnout, knowledge gaps
   - **Strength Identification**: Growth areas, mentorship signals
   - **Peer Comparison**: Within-team and org-wide

3. **`dashboard.py`** — Interactive Streamlit app
   - **Team Analysis View**: Individual contribution breakdown, trends, radar charts
   - **Organization Overview**: Team comparison, top contributors, KPIs
   - Responsive to multiple metrics
   - Ready for export functionality (next phase)

## Getting Started

### Install Dependencies

```bash
pip install streamlit pandas plotly
```

### Run the Dashboard

From the `python` directory:

```bash
streamlit run managertools/dashboard.py
```

The dashboard will open in your browser at `http://localhost:8501`

## Features

### Team Analysis View

- **Team Summary KPIs**: Size, total code volume, commits, reviews, tickets closed
- **Individual Contributions Table**: Sortable by any metric
- **Productivity Trends**: Line chart showing code volume by sprint (multi-select)
- **Performance Radar**: 3-axis radar chart (Productivity, Review Quality, Collaboration)
- **Peer Comparison**: Within-team metrics relative to team average

### Organization Overview

- **Org-wide KPIs**: Total headcount, aggregate metrics
- **Team Comparison Table**: Side-by-side team metrics
- **Top Contributors**: Ranked lists by code volume, commits, reviews

## Productivity Metrics Explained

### Productivity Score (0-100)
Normalized composite of:
- **Code Volume**: Lines added + removed (max 1000 = 33 pts)
- **Commits**: Number of commits (max 20 = 33 pts)
- **Tickets Closed**: Issues resolved (max 10 = 34 pts)

**Interpretation**: High score indicates consistent delivery and output. Use alongside review quality to understand holistic contribution.

### Velocity Trend
Compares current period (last 2 sprints) vs. previous period (prior 2 sprints):
- **↑ Up**: 10%+ improvement
- **→ Stable**: Within 10%
- **↓ Down**: 10%+ decline

**Use for**: Growth conversations, identifying burnout, capacity planning.

### Review Quality Score (0-100)
Ratio of detailed code reviews (comments) vs. rubber-stamp approvals:
- **0-30**: Minimal review engagement
- **30-60**: Moderate, mixed review style
- **60-100**: High engagement, detailed feedback

**Interpretation**: Indicates mentorship contribution and code quality focus.

### Risk Indicators
Flags that may indicate problems:
- Code volume down >50% vs. average
- No reviews given (code silos)
- Review participation dropped >70%

**Use for**: Early warning system, support conversations.

## Data Structure

The dashboard loads individual sprint reports in the format:
```
individual-{TeamName}-{UserName}.csv
```

Each CSV contains sprint-by-sprint metrics for a person:
- SPRINT: Sprint name
- USER, AUTHOR: Person identifier
- CODE_VOLUME: PR lines added + removed
- COMMITS: Commit count
- APPROVED: PRs approved
- COMMENTED_ON_OTHERS: Code reviews with comments
- OTHERS_COMMENTED: Review engagement received
- TICKETS_CLOSED: Jira issues closed
- PRs_MERGED: Pull requests merged
- And more...

## Next Steps (Planned)

- **Task #4**: Org-wide and title-level comparison views
- **Task #5**: Configurable export module (JSON/Markdown/PDF) for performance reviews

## Architecture Notes

### Aggregation Layer Design

The `MetricsAggregator` class:
- Loads all CSVs on init (caches in memory)
- Normalizes rows by sprint (sums metrics across PRs/tickets)
- Provides three main query methods:
  - `get_team_metrics()`: Single team aggregated by person
  - `get_org_metrics()`: All teams aggregated by person
  - `get_individual_history()`: Sprint-by-sprint history for a person

### Session Caching

The aggregator is cached in Streamlit's `session_state` to avoid reloading CSVs on every interaction.

## Known Limitations

1. **Title/Role Mapping**: Currently no role inference; next phase will add title-level comparison
2. **Date Filtering**: Supports sprint names or YYYY-MM-DD format; sprint name detection is heuristic
3. **Export Not Yet Implemented**: Task #5 will add JSON/Markdown/PDF export
4. **No Real-time Data**: Loads snapshot of CSVs; requires manual re-run for new data

## Future Enhancements

- Interactive drill-down to PR-level details
- Trend forecast (linear regression on velocity)
- Custom metric definitions per team
- Email/Slack integration for auto-generated reports
- Performance review data package generation
