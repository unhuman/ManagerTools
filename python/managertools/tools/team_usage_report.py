#!/usr/bin/env python3
"""
Generate an interactive HTML report of Claude Code usage by team.

Usage:
  python -m managertools.tools.team_usage_report PARAMS_JSON_FILE OUTPUT_PATH

Input: JSON file with keys: teams, time_period, period_label, members, usage_by_email, models
Output: HTML file written to OUTPUT_PATH
"""
import json
import sys
import os


def escape_html(s):
    """Escape HTML special characters."""
    return (str(s)
            .replace('&', '&amp;')
            .replace('<', '&lt;')
            .replace('>', '&gt;')
            .replace('"', '&quot;')
            .replace("'", '&#39;'))


def format_currency(value):
    """Format a value as USD currency."""
    if value is None or value == 0:
        return "$0.00"
    return f"${float(value):.2f}"


def format_number(value):
    """Format a number with thousand separators."""
    if value is None or value == 0:
        return "0"
    return f"{int(value):,}"


def generate_html(teams, time_period, period_label, members, usage_by_email, models,
                  products=None, forecast_by_email=None, source_usage=None, sources=None):
    """
    Generate a self-contained HTML report.

    Args:
        teams: List of team names
        time_period: 'mtd' or 'past-month'
        period_label: Human-readable period (e.g., "July 2026 (MTD)")
        members: List of dicts with 'name', 'email', 'team'
        usage_by_email: Dict mapping email -> {'cost': float, 'requests': int, 'sessions': int, 'model_costs': {model: cost}, 'product_costs': {product: cost}}
        models: Sorted list of all model names found
        products: Sorted list of all product names found (optional)
        forecast_by_email: Dict mapping email -> forecasted cost (optional, for mtd only)
    """
    # Normalize provider-specific data into the original report's data model.
    # Keeping the legacy renderer as the single HTML implementation preserves
    # its team summary, filters, sorting, forecast, dark mode, and responsive UI.
    source_rows = {}
    if source_usage:
        sources = list(sources or source_usage.keys())
        merged = {}
        merged_models = set()
        merged_products = set()
        for source in sources:
            label = source.title()
            for email, value in source_usage.get(source, {}).items():
                source_rows.setdefault(email, {})[source] = value
                row = merged.setdefault(email, {'cost': 0, 'requests': 0, 'sessions': 0,
                                                'active_days': 0, 'active_day_dates': [],
                                                'model_costs': {}, 'product_costs': {}})
                row['cost'] += float(value.get('cost', 0) or 0)
                row['requests'] += int(value.get('requests', value.get('events', 0)) or 0)
                row['sessions'] += int(value.get('sessions', value.get('conversations', 0)) or 0)
                row['active_day_dates'] = sorted(set(row['active_day_dates']) |
                                                 set(value.get('active_day_dates', [])))
                row['active_days'] = len(row['active_day_dates'])
                for model, amount in value.get('models', value.get('model_costs', {})).items():
                    key = f'{label} · {model}'
                    row['model_costs'][key] = row['model_costs'].get(key, 0) + float(amount or 0)
                    merged_models.add(key)
                for product, amount in value.get('applications', value.get('product_costs', {})).items():
                    key = f'{label} · {product}'
                    row['product_costs'][key] = row['product_costs'].get(key, 0) + float(amount or 0)
                    merged_products.add(key)
        usage_by_email = merged
        models = sorted(merged_models)
        products = sorted(merged_products)
    elif sources:
        sources = list(sources)

    if products is None:
        products = []
    if forecast_by_email is None:
        forecast_by_email = {}

    forecast_ratio = 1.0
    ratios = [forecast_by_email[email] / usage_by_email[email]['cost']
              for email in forecast_by_email
              if email in usage_by_email and usage_by_email[email].get('cost', 0) > 0]
    if ratios:
        forecast_ratio = ratios[0]

    # Aggregate usage by team
    usage_by_team = {}
    for team in teams:
        usage_by_team[team] = {
            'cost': 0,
            'requests': 0,
            'sessions': set(),
            'member_count': 0,
            'active_member_count': 0,
            'model_costs': {},
            'product_costs': {},
            'forecast': 0,
            'source_costs': {},
            'source_active_days': {},
            'source_active_members': {}
        }

    for member in members:
        team = member['team']
        email = member['email']
        if team not in usage_by_team:
            usage_by_team[team] = {
                'cost': 0,
                'requests': 0,
                'sessions': set(),
                'member_count': 0,
                'active_member_count': 0,
                'model_costs': {},
                'product_costs': {},
                'forecast': 0,
                'source_costs': {},
                'source_active_days': {},
                'source_active_members': {}
            }

        usage_by_team[team]['member_count'] += 1
        usage = usage_by_email.get(email, {})
        cost = usage.get('cost', 0)
        requests = usage.get('requests', 0)
        sessions = usage.get('sessions', 0)

        if cost > 0 or requests > 0:
            usage_by_team[team]['active_member_count'] += 1

        usage_by_team[team]['cost'] += cost
        usage_by_team[team]['requests'] += requests
        usage_by_team[team]['forecast'] += forecast_by_email.get(email, 0)

        for source, source_value in source_rows.get(email, {}).items():
            usage_by_team[team]['source_costs'][source] = (
                usage_by_team[team]['source_costs'].get(source, 0) +
                float(source_value.get('cost', 0) or 0)
            )
            active_dates = set(source_value.get('active_day_dates', []))
            usage_by_team[team]['source_active_days'].setdefault(source, set()).update(active_dates)
            if float(source_value.get('cost', 0) or 0) > 0:
                usage_by_team[team]['source_active_members'].setdefault(source, set()).add(email)

        if sessions > 0:
            usage_by_team[team]['sessions'].add(email)

        # Aggregate model and product costs
        for model, model_cost in usage.get('model_costs', {}).items():
            if model not in usage_by_team[team]['model_costs']:
                usage_by_team[team]['model_costs'][model] = 0
            usage_by_team[team]['model_costs'][model] += model_cost

        for product, product_cost in usage.get('product_costs', {}).items():
            if product not in usage_by_team[team]['product_costs']:
                usage_by_team[team]['product_costs'][product] = 0
            usage_by_team[team]['product_costs'][product] += product_cost

    # Build team rows
    team_rows = []
    for team_name in sorted(teams, key=lambda t: usage_by_team[t]['cost'], reverse=True):
        team_data = usage_by_team[team_name]
        cost = team_data['cost']
        requests = team_data['requests']
        sessions = len(team_data['sessions'])
        cost_per_request = (cost / requests) if requests > 0 else 0
        forecast = team_data['forecast']

        team_rows.append({
            'name': escape_html(team_name),
            'cost': cost,
            'requests': requests,
            'sessions': sessions,
            'cost_per_request': cost_per_request,
            'member_count': team_data['member_count'],
            'active_member_count': team_data['active_member_count'],
            'model_costs': team_data['model_costs'],
            'product_costs': team_data['product_costs'],
            'forecast': forecast,
            'cost_formatted': format_currency(cost),
            'requests_formatted': format_number(requests),
            'sessions_formatted': format_number(sessions),
            'cost_per_request_formatted': format_currency(cost_per_request),
            'member_count_formatted': format_number(team_data['member_count']),
            'active_member_count_formatted': format_number(team_data['active_member_count']),
            'forecast_formatted': format_currency(forecast),
            'source_costs': team_data['source_costs'],
            'source_active_days': team_data['source_active_days'],
            'source_active_members': team_data['source_active_members'],
        })

    # Build all rows
    all_rows = []

    for member in members:
        email = member['email']
        name = member['name']
        team = member['team']

        usage = usage_by_email.get(email, {})
        cost = usage.get('cost', 0)
        requests = usage.get('requests', 0)
        sessions = usage.get('sessions', 0)
        cost_per_request = (cost / requests) if requests > 0 else 0
        model_costs = usage.get('model_costs', {})
        product_costs = usage.get('product_costs', {})
        forecast = forecast_by_email.get(email, 0)
        active_days = usage.get('active_days', 0)

        row = {
            'name': escape_html(name),
            'team': escape_html(team),
            'email': escape_html(email),
            'cost': cost,
            'requests': requests,
            'sessions': sessions,
            'cost_per_request': cost_per_request,
            'model_costs': model_costs,
            'product_costs': product_costs,
            'forecast': forecast,
            'active_days': active_days,
            'cost_formatted': format_currency(cost),
            'requests_formatted': format_number(requests),
            'sessions_formatted': format_number(sessions),
            'cost_per_request_formatted': format_currency(cost_per_request),
            'forecast_formatted': format_currency(forecast),
            'source_costs': {source: float(value.get('cost', 0) or 0)
                            for source, value in source_rows.get(email, {}).items()},
            'source_active_days': {source: value.get('active_day_dates', [])
                                  for source, value in source_rows.get(email, {}).items()},
        }

        all_rows.append(row)

    # Sort by cost descending (inactive users with $0 will be at the bottom)
    all_rows.sort(key=lambda r: r['cost'], reverse=True)

    # Extract unique teams from all users
    all_teams = sorted(set(r['team'] for r in all_rows))
    team_filter_html = ''
    if all_teams:
        team_checkboxes = ''.join(
            f'<label><input type="checkbox" class="team-filter" value="{t}" checked> {t}</label>'
            for t in all_teams
        )
        team_filter_html = f'''
    <div class="team-filter-container">
      <label style="font-weight: 600; margin-right: 1rem;">Filter by Team:</label>
      {team_checkboxes}
      <div class="filter-buttons">
        <button id="select-all-teams" class="filter-button">Select All</button>
        <button id="clear-all-teams" class="filter-button">Clear All</button>
      </div>
    </div>
    '''

    # Generate model cost header columns (individual model names)
    def dimension_source(name):
        return name.split(' · ', 1)[0].lower() if ' · ' in name else ''

    model_headers_individual = ''.join(
        f'<th class="sortable model-col" data-source="{dimension_source(m)}" data-model="{escape_html(m)}">{escape_html(m)}</th>'
        for m in models)

    # Generate group header for models (only if there are models)
    model_group_header = f'<th class="group-header model-group-header" colspan="{len(models)}">Model</th>' if models else ''

    model_cells = []
    for row in all_rows:
        cells = ''.join(f'<td class="currency model-cell" data-source="{dimension_source(m)}" data-value="{row["model_costs"].get(m, 0)}">{format_currency(row["model_costs"].get(m, 0))}</td>' for m in models)
        model_cells.append(cells)

    # Generate forecast header and cells (only if forecast_by_email is provided and non-empty)
    forecast_header = ''
    forecast_group_header = ''
    forecast_cells = []
    if forecast_by_email:
        forecast_header = '<th class="sortable forecast-col">Full Month</th>'
        forecast_group_header = '<th class="group-header forecast-group-header">Forecast</th>'
        for row in all_rows:
            cells = f'<td class="currency forecast-col">{row["forecast_formatted"]}</td>'
            forecast_cells.append(cells)

    # Generate product cost header columns (individual product names)
    product_headers_individual = ''.join(
        f'<th class="sortable product-col" data-source="{dimension_source(p)}" data-product="{escape_html(p)}">{escape_html(p)}</th>'
        for p in products)

    # Generate group header for products (only if there are products)
    product_group_header = f'<th class="group-header product-group-header" colspan="{len(products)}">Product</th>' if products else ''

    product_cells = []
    for row in all_rows:
        cells = ''.join(f'<td class="currency product-cell" data-source="{dimension_source(p)}" data-value="{row["product_costs"].get(p, 0)}">{format_currency(row["product_costs"].get(p, 0))}</td>' for p in products)
        product_cells.append(cells)

    # Build table rows
    source_filter_html = ''
    if source_rows:
        labels = {source: source.title() for source in sources}
        checks = ''.join(
            f'<label><input type="checkbox" class="source-filter" value="{escape_html(source)}" checked> {escape_html(labels[source])}</label>'
            for source in sources)
        source_filter_html = f'''\n    <h2 id="sources">Sources</h2>\n    <div class="source-filter-container">\n      {checks}\n      <div class="filter-buttons">\n        <button id="select-all-sources" class="filter-button">Select All</button>\n        <button id="clear-all-sources" class="filter-button">Clear All</button>\n      </div>\n    </div>'''

    all_table_rows = ''
    for i, row in enumerate(all_rows):
        forecast_cell = forecast_cells[i] if forecast_cells else ''
        source_costs = escape_html(json.dumps(row.get('source_costs', {}), separators=(',', ':')))
        source_days = escape_html(json.dumps(row.get('source_active_days', {}), separators=(',', ':')))
        all_table_rows += f'''    <tr data-email="{escape_html(row['email'])}" data-cost="{row['cost']}" data-requests="{row['requests']}" data-sessions="{row['sessions']}" data-source-costs="{source_costs}" data-source-active-days="{source_days}" data-forecast-ratio="{forecast_ratio}">
      <td class="name">{row['name']}</td>
      <td class="team">{row['team']}</td>
      <td class="email">{row['email']}</td>
      <td class="number active-days-cell">{row['active_days']}</td>
      <td class="currency total-cost-cell">{row['cost_formatted']}</td>
      {forecast_cell}{model_cells[i]}{product_cells[i]}
    </tr>
'''

    # Build team table rows
    team_table_rows = ''
    for row in team_rows:
        model_cells = ''.join(f'<td class="currency model-cell" data-source="{dimension_source(m)}" data-value="{row["model_costs"].get(m, 0)}">{format_currency(row["model_costs"].get(m, 0))}</td>' for m in models)
        product_cells = ''.join(f'<td class="currency product-cell" data-source="{dimension_source(p)}" data-value="{row["product_costs"].get(p, 0)}">{format_currency(row["product_costs"].get(p, 0))}</td>' for p in products)
        forecast_cell = f'<td class="currency forecast-col">{row["forecast_formatted"]}</td>' if forecast_by_email else ''
        source_costs = escape_html(json.dumps(row.get('source_costs', {}), separators=(',', ':')))
        source_days = escape_html(json.dumps({source: sorted(days) for source, days in row.get('source_active_days', {}).items()}, separators=(',', ':')))
        source_members = escape_html(json.dumps({source: sorted(emails) for source, emails in row.get('source_active_members', {}).items()}, separators=(',', ':')))
        team_table_rows += f'''    <tr class="team-row" data-cost="{row['cost']}" data-source-costs="{source_costs}" data-source-active-days="{source_days}" data-source-active-members="{source_members}" data-active-members="{row['active_member_count']}" data-member-count="{row['member_count']}" data-forecast-ratio="{forecast_ratio}">
      <td class="name">{row['name']}</td>
      <td class="number active-members-cell">{row['active_member_count_formatted']}/{row['member_count_formatted']}</td>
      <td class="currency total-cost-cell">{row['cost_formatted']}</td>
      {forecast_cell}{model_cells}{product_cells}
    </tr>
'''

    # Compute summary statistics
    total_cost = sum(r['cost'] for r in all_rows)
    total_requests = sum(r['requests'] for r in all_rows)
    total_sessions = sum(r['sessions'] for r in all_rows)
    active_user_count = sum(1 for r in all_rows if r['cost'] > 0 or r['requests'] > 0)
    total_cost_per_request = (total_cost / total_requests) if total_requests > 0 else 0

    summary_html = f'''    <div class="summary-stats">
      <div class="stat-item">
        <span class="stat-label">Users:</span>
        <span class="stat-value">{len(all_rows)}</span>
      </div>
      <div class="stat-item">
        <span class="stat-label">Total Cost:</span>
        <span class="stat-value">{format_currency(total_cost)}</span>
      </div>
      <div class="stat-item">
        <span class="stat-label">Total Requests:</span>
        <span class="stat-value">{format_number(total_requests)}</span>
      </div>
      <div class="stat-item">
        <span class="stat-label">Average Cost/Request:</span>
        <span class="stat-value">{format_currency(total_cost_per_request)}</span>
      </div>
    </div>
'''


    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Claude Code Team Usage Report</title>
  <style>
    :root {{
      --bg: #ffffff;
      --text: #000000;
      --border: #ddd;
      --header-bg: #f5f5f5;
      --hover-bg: #fafafa;
      --accent: #0066cc;
    }}

    @media (prefers-color-scheme: dark) {{
      :root {{
        --bg: #1e1e1e;
        --text: #e0e0e0;
        --border: #444;
        --header-bg: #2d2d2d;
        --hover-bg: #333;
        --accent: #5aa8ff;
      }}
    }}

    :root[data-theme="light"] {{
      --bg: #ffffff;
      --text: #000000;
      --border: #ddd;
      --header-bg: #f5f5f5;
      --hover-bg: #fafafa;
      --accent: #0066cc;
    }}

    :root[data-theme="dark"] {{
      --bg: #1e1e1e;
      --text: #e0e0e0;
      --border: #444;
      --header-bg: #2d2d2d;
      --hover-bg: #333;
      --accent: #5aa8ff;
    }}

    * {{
      margin: 0;
      padding: 0;
      box-sizing: border-box;
    }}

    body {{
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, "Helvetica Neue", Arial, sans-serif;
      background-color: var(--bg);
      color: var(--text);
      padding: 2rem;
      line-height: 1.6;
    }}

    .container {{
      max-width: 100%;
      margin: 0 auto;
    }}

    h1 {{
      margin-bottom: 0.5rem;
      font-size: 2rem;
    }}

    h2 {{
      margin-top: 2rem;
      margin-bottom: 1rem;
      font-size: 1.3rem;
    }}

    .period-label {{
      color: var(--text);
      font-size: 0.9rem;
      margin-bottom: 2rem;
    }}

    .teams-list {{
      margin-bottom: 2rem;
      padding: 1rem;
      background-color: var(--header-bg);
      border-radius: 4px;
    }}

    .teams-list strong {{
      margin-right: 0.5rem;
    }}

    .summary-stats {{
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
      gap: 1rem;
      margin-bottom: 2rem;
    }}

    .stat-item {{
      padding: 1rem;
      background-color: var(--header-bg);
      border-radius: 4px;
      border-left: 4px solid var(--accent);
    }}

    .stat-label {{
      display: block;
      font-size: 0.9rem;
      color: var(--text);
      margin-bottom: 0.5rem;
    }}

    .stat-value {{
      display: block;
      font-size: 1.5rem;
      font-weight: bold;
      color: var(--text);
    }}

    .table-wrapper {{
      max-height: 800px;
      overflow-y: auto;
      border: 1px solid var(--border);
      border-radius: 4px;
      margin-bottom: 2rem;
    }}

    .report-table {{
      width: 100%;
      border-collapse: collapse;
      background-color: var(--bg);
    }}

    .report-table thead {{
      background-color: var(--header-bg);
      font-weight: 600;
    }}

    .report-table th {{
      padding: 1rem;
      text-align: left;
      border-bottom: 1px solid var(--border);
      user-select: none;
      cursor: pointer;
      position: sticky;
      top: 0;
      background-color: var(--header-bg);
      z-index: 10;
    }}

    .report-table th.sortable::after {{
      content: " ⇅";
      opacity: 0.3;
      font-size: 0.9rem;
    }}

    .report-table th.sort-asc::after {{
      content: " ↑";
      opacity: 1;
    }}

    .report-table th.sort-desc::after {{
      content: " ↓";
      opacity: 1;
    }}

    .report-table tbody tr {{
      border-bottom: 1px solid var(--border);
      transition: background-color 0.2s;
    }}

    .report-table tbody tr:hover {{
      background-color: var(--hover-bg);
    }}

    .report-table td {{
      padding: 0.75rem 1rem;
    }}

    .report-table .name {{
      font-weight: 500;
    }}

    .report-table .team {{
      font-size: 0.9rem;
    }}

    .report-table .email {{
      font-size: 0.85rem;
      font-family: monospace;
    }}

    .report-table .currency {{
      text-align: right;
      font-family: "Courier New", monospace;
    }}

    .report-table .number {{
      text-align: right;
      font-family: "Courier New", monospace;
    }}

    .report-table .group-header {{
      font-weight: 600;
      border-bottom: 2px solid var(--border);
      text-align: center;
    }}

    .report-table .model-group-header {{
      background-color: #e8f0ff;
    }}

    @media (prefers-color-scheme: dark) {{
      .report-table .model-group-header {{
        background-color: #1a3a52;
      }}
    }}

    :root[data-theme="light"] .report-table .model-group-header {{
      background-color: #e8f0ff;
    }}

    :root[data-theme="dark"] .report-table .model-group-header {{
      background-color: #1a3a52;
    }}

    .report-table .product-group-header {{
      background-color: #f0e8ff;
    }}

    @media (prefers-color-scheme: dark) {{
      .report-table .product-group-header {{
        background-color: #3a1a52;
      }}
    }}

    :root[data-theme="light"] .report-table .product-group-header {{
      background-color: #f0e8ff;
    }}

    :root[data-theme="dark"] .report-table .product-group-header {{
      background-color: #3a1a52;
    }}

    .report-table .model-col {{
      font-size: 0.85rem;
      background-color: #e8f0ff;
      position: sticky;
      top: 0;
      z-index: 10;
    }}

    @media (prefers-color-scheme: dark) {{
      .report-table .model-col {{
        background-color: #1a3a52;
      }}
    }}

    :root[data-theme="light"] .report-table .model-col {{
      background-color: #e8f0ff;
    }}

    :root[data-theme="dark"] .report-table .model-col {{
      background-color: #1a3a52;
    }}

    .report-table .model-cell {{
      font-size: 0.85rem;
      text-align: right;
      background-color: #f5f8ff;
    }}

    @media (prefers-color-scheme: dark) {{
      .report-table .model-cell {{
        background-color: #0f1e2e;
      }}
    }}

    :root[data-theme="light"] .report-table .model-cell {{
      background-color: #f5f8ff;
    }}

    :root[data-theme="dark"] .report-table .model-cell {{
      background-color: #0f1e2e;
    }}

    .report-table .product-col {{
      font-size: 0.85rem;
      background-color: #f0e8ff;
      position: sticky;
      top: 0;
      z-index: 10;
    }}

    @media (prefers-color-scheme: dark) {{
      .report-table .product-col {{
        background-color: #3a1a52;
      }}
    }}

    :root[data-theme="light"] .report-table .product-col {{
      background-color: #f0e8ff;
    }}

    :root[data-theme="dark"] .report-table .product-col {{
      background-color: #3a1a52;
    }}

    .report-table .product-cell {{
      font-size: 0.85rem;
      text-align: right;
      background-color: #faf5ff;
    }}

    @media (prefers-color-scheme: dark) {{
      .report-table .product-cell {{
        background-color: #2e0f3e;
      }}
    }}

    :root[data-theme="light"] .report-table .product-cell {{
      background-color: #faf5ff;
    }}

    :root[data-theme="dark"] .report-table .product-cell {{
      background-color: #2e0f3e;
    }}

    .report-table .forecast-group-header {{
      background-color: #e8ffe8;
    }}

    @media (prefers-color-scheme: dark) {{
      .report-table .forecast-group-header {{
        background-color: #1a521a;
      }}
    }}

    :root[data-theme="light"] .report-table .forecast-group-header {{
      background-color: #e8ffe8;
    }}

    :root[data-theme="dark"] .report-table .forecast-group-header {{
      background-color: #1a521a;
    }}

    .report-table th.forecast-col {{
      font-size: 0.85rem;
      background-color: #e8ffe8;
      position: sticky;
      top: 0;
      z-index: 11;
      text-align: right;
    }}

    .report-table td.forecast-col {{
      font-size: 0.85rem;
      background-color: #e8ffe8;
      text-align: right;
    }}

    @media (prefers-color-scheme: dark) {{
      .report-table th.forecast-col {{
        background-color: #1a521a;
      }}
      .report-table td.forecast-col {{
        background-color: #1a521a;
      }}
    }}

    :root[data-theme="light"] .report-table th.forecast-col {{
      background-color: #e8ffe8;
    }}

    :root[data-theme="light"] .report-table td.forecast-col {{
      background-color: #e8ffe8;
    }}

    :root[data-theme="dark"] .report-table th.forecast-col {{
      background-color: #1a521a;
    }}

    :root[data-theme="dark"] .report-table td.forecast-col {{
      background-color: #1a521a;
    }}

.toc {{
      background-color: var(--header-bg);
      border: 1px solid var(--border);
      border-radius: 4px;
      padding: 1rem;
      margin: 1.5rem 0;
    }}

    .toc ul {{
      list-style: none;
      padding-left: 1rem;
      margin: 0.5rem 0 0 0;
    }}

    .toc li {{
      margin: 0.5rem 0;
    }}

    .toc a {{
      color: var(--accent);
      text-decoration: none;
      transition: opacity 0.2s;
    }}

    .toc a:hover {{
      opacity: 0.8;
      text-decoration: underline;
    }}

    .team-table .name {{
      font-weight: 600;
    }}

    .team-row {{
      background-color: var(--header-bg);
      font-weight: 500;
    }}

    .team-row:hover {{
      background-color: var(--hover-bg) !important;
    }}

    .team-filter-container {{
      margin: 1.5rem 0;
      padding: 1rem;
      background-color: var(--header-bg);
      border: 1px solid var(--border);
      border-radius: 4px;
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 1.5rem;
    }}

    .team-filter-container label {{
      display: flex;
      align-items: center;
      gap: 0.5rem;
      cursor: pointer;
      user-select: none;
    }}

    .team-filter-container input[type="checkbox"] {{
      cursor: pointer;
    }}

    .source-filter-container {{
      margin: 1.5rem 0;
      padding: 1rem;
      background-color: var(--header-bg);
      border: 1px solid var(--border);
      border-radius: 4px;
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 1.5rem;
    }}

    .source-filter-container label {{
      display: flex;
      align-items: center;
      gap: 0.5rem;
      cursor: pointer;
      user-select: none;
    }}

    .source-filter-container input[type="checkbox"] {{
      cursor: pointer;
    }}

    .filter-buttons {{
      display: flex;
      gap: 0.5rem;
      margin-left: auto;
    }}

    .filter-button {{
      padding: 0.4rem 1rem;
      background-color: var(--accent);
      color: white;
      border: none;
      border-radius: 4px;
      cursor: pointer;
      font-size: 0.9rem;
      transition: opacity 0.2s;
    }}

    .filter-button:hover {{
      opacity: 0.8;
    }}

    .filter-button:active {{
      opacity: 0.7;
    }}

    @media (max-width: 1024px) {{
      .report-table {{
        font-size: 0.9rem;
      }}

      .report-table th,
      .report-table td {{
        padding: 0.5rem;
      }}

      .model-col {{
        display: none;
      }}

      .model-cell {{
        display: none;
      }}
    }}
  </style>
</head>
<body>
  <div class="container">
    <h1>Claude Code Team Usage Report</h1>
    <div class="period-label">Period: {period_label}</div>
    <div class="teams-list">
      <strong>Teams:</strong> {', '.join(escape_html(t) for t in teams)}
    </div>

    <nav class="toc">
      <strong>Contents:</strong>
      <ul>
        <li><a href="#filters">Filters</a></li>
        <li><a href="#sources">Sources</a></li>
        <li><a href="#overall-stats">Overall Stats</a></li>
        <li><a href="#team-summary">Team Summary</a></li>
        <li><a href="#individual-users">Users</a></li>
      </ul>
    </nav>

    <h2 id="filters">Filters</h2>
    {team_filter_html}
    {source_filter_html}

    <h2 id="overall-stats">Overall Stats</h2>
    {summary_html}

    <h2 id="team-summary">Team Summary ({len(team_rows)})</h2>
    <div class="table-wrapper">
      <table class="report-table team-table" id="team-summary-table">
        <thead>
          <tr>
            <th colspan="3" style="border: none; background: none;"></th>
            {forecast_group_header}{model_group_header}{product_group_header}
          </tr>
          <tr>
            <th class="sortable">Team</th>
            <th class="sortable">Active/Members</th>
            <th class="sortable">Total Cost</th>
            {forecast_header}{model_headers_individual}{product_headers_individual}
          </tr>
        </thead>
        <tbody>
          {team_table_rows}
        </tbody>
      </table>
    </div>

    <h2 id="individual-users">Users ({len(all_rows)})</h2>
    <div class="table-wrapper">
      <table class="report-table" id="active-users-table">
        <thead>
          <tr>
            <th colspan="5" style="border: none; background: none;"></th>
            {forecast_group_header}{model_group_header}{product_group_header}
          </tr>
          <tr>
            <th class="sortable">Name</th>
            <th class="sortable">Team</th>
            <th class="sortable">Email</th>
            <th class="sortable">Days Active</th>
            <th class="sortable">Total Cost</th>
            {forecast_header}{model_headers_individual}{product_headers_individual}
          </tr>
        </thead>
        <tbody>
          {all_table_rows}
        </tbody>
      </table>
    </div>
  </div>

  <script>
    let sortState = {{}};

    function getSortKey(header) {{
      const text = header.textContent.trim();
      const index = Array.from(header.parentNode.children).indexOf(header);
      return {{text, index}};
    }}

    function parseValue(text) {{
      // Try currency
      if (text.startsWith('$')) {{
        return parseFloat(text.substring(1).replace(/,/g, ''));
      }}
      // Try number
      const num = parseFloat(text.replace(/,/g, ''));
      if (!isNaN(num)) {{
        return num;
      }}
      // String
      return text.toLowerCase();
    }}

    function sortTable(header) {{
      const key = getSortKey(header);
      const table = header.closest('table');
      const tbody = table.querySelector('tbody');
      const rows = Array.from(tbody.querySelectorAll('tr'));

      // Remove previous sort indicators
      table.querySelectorAll('th').forEach(h => {{
        h.classList.remove('sort-asc', 'sort-desc');
      }});

      // Determine sort direction
      let ascending = true;
      if (sortState.lastHeader === header) {{
        ascending = !sortState.lastAscending;
      }}

      // Sort rows
      rows.sort((a, b) => {{
        const aVal = parseValue(a.children[key.index].textContent);
        const bVal = parseValue(b.children[key.index].textContent);

        if (typeof aVal === 'number' && typeof bVal === 'number') {{
          return ascending ? aVal - bVal : bVal - aVal;
        }}

        const aStr = String(aVal);
        const bStr = String(bVal);
        const cmp = aStr.localeCompare(bStr);
        return ascending ? cmp : -cmp;
      }});

      // Re-append sorted rows
      rows.forEach(row => tbody.appendChild(row));

      // Update sort indicator
      header.classList.add(ascending ? 'sort-asc' : 'sort-desc');
      sortState = {{lastHeader: header, lastAscending: ascending}};
    }}

    // Attach click handlers to sortable headers
    document.querySelectorAll('th.sortable').forEach(header => {{
      header.addEventListener('click', () => sortTable(header));
    }});

    function updateDimensionVisibility(selectedSources) {{
      document.querySelectorAll('.report-table').forEach(table => {{
        const bodyRows = Array.from(table.querySelectorAll('tbody tr'))
          .filter(row => row.style.display !== 'none');
        table.querySelectorAll('th.model-col, th.product-col').forEach(header => {{
          const index = Array.from(header.parentNode.children).indexOf(header);
          const sourceVisible = !selectedSources || selectedSources.includes(header.dataset.source);
          const hasValue = bodyRows.some(row => Number(row.children[index]?.dataset.value || 0) !== 0);
          const visible = sourceVisible && hasValue;
          header.style.display = visible ? '' : 'none';
          bodyRows.forEach(row => {{
            if (row.children[index]) row.children[index].style.display = visible ? '' : 'none';
          }});
        }});
        const modelColumns = Array.from(table.querySelectorAll('th.model-col'))
          .filter(header => header.style.display !== 'none');
        const productColumns = Array.from(table.querySelectorAll('th.product-col'))
          .filter(header => header.style.display !== 'none');
        const modelGroup = table.querySelector('.model-group-header');
        const productGroup = table.querySelector('.product-group-header');
        if (modelGroup) {{
          modelGroup.colSpan = modelColumns.length;
          modelGroup.style.display = modelColumns.length ? '' : 'none';
        }}
        if (productGroup) {{
          productGroup.colSpan = productColumns.length;
          productGroup.style.display = productColumns.length ? '' : 'none';
        }}
      }});
    }}

    // Team filter functionality
    function applyTeamFilter() {{
      const selectedTeams = Array.from(document.querySelectorAll('.team-filter:checked')).map(cb => cb.value);
      const sourceFilters = Array.from(document.querySelectorAll('.source-filter'));
      const selectedSources = sourceFilters.length ? sourceFilters.filter(cb => cb.checked).map(cb => cb.value) : null;

      function selectedCost(row) {{
        if (!selectedSources) return Number(row.dataset.cost || 0);
        const values = JSON.parse(row.dataset.sourceCosts || '{{}}');
        return selectedSources.reduce((sum, source) => sum + Number(values[source] || 0), 0);
      }}

      function selectedDays(row) {{
        if (!selectedSources) return Number(row.querySelector('.active-days-cell')?.textContent || 0);
        const values = JSON.parse(row.dataset.sourceActiveDays || '{{}}');
        return new Set(selectedSources.flatMap(source => values[source] || [])).size;
      }}

      function selectedActiveMembers(row) {{
        if (!selectedSources) return Number(row.dataset.activeMembers || 0);
        const values = JSON.parse(row.dataset.sourceActiveMembers || '{{}}');
        return new Set(selectedSources.flatMap(source => values[source] || [])).size;
      }}

      // Filter Team Summary table (team in column 0)
      const teamTable = document.getElementById('team-summary-table');
      if (teamTable) {{
        const rows = teamTable.querySelector('tbody').querySelectorAll('tr');
        rows.forEach(row => {{
          const teamCell = row.children[0].textContent.trim();
          const isVisible = selectedTeams.length > 0 && selectedTeams.includes(teamCell);
          row.style.display = isVisible ? '' : 'none';
          row.dataset.selectedCost = selectedCost(row);
          const teamCost = row.querySelector('.total-cost-cell');
          if (teamCost) teamCost.textContent = '$' + Number(row.dataset.selectedCost).toFixed(2);
          const teamMembers = row.querySelector('.active-members-cell');
          if (teamMembers) teamMembers.textContent = selectedActiveMembers(row) + '/' + row.dataset.memberCount;
          const teamForecast = row.querySelector('.forecast-col');
          if (teamForecast) teamForecast.textContent = '$' + (Number(row.dataset.selectedCost) * Number(row.dataset.forecastRatio || 1)).toFixed(2);
        }});
      }}

      // Filter Users table (team in column 1) and collect stats
      const usersTable = document.getElementById('active-users-table');
      let totalCost = 0, totalRequests = 0, totalSessions = 0;
      if (usersTable) {{
        const rows = usersTable.querySelector('tbody').querySelectorAll('tr');
        rows.forEach(row => {{
          const teamCell = row.children[1].textContent.trim();
          const isVisible = selectedTeams.length > 0 && selectedTeams.includes(teamCell);
          row.style.display = isVisible ? '' : 'none';

          if (isVisible) {{
            const cost = selectedCost(row);
            row.querySelector('.total-cost-cell').textContent = '$' + cost.toFixed(2);
            row.querySelector('.active-days-cell').textContent = selectedDays(row);
            const userForecast = row.querySelector('.forecast-col');
            if (userForecast) userForecast.textContent = '$' + (cost * Number(row.dataset.forecastRatio || 1)).toFixed(2);
            const requests = parseInt(row.dataset.requests || 0);
            const sessions = parseInt(row.dataset.sessions || 0);
            totalCost += cost;
            totalRequests += requests;
            totalSessions += sessions;
          }}
        }});
      }}

      // Update summary stats
      const userCount = usersTable ? Array.from(usersTable.querySelector('tbody').querySelectorAll('tr')).filter(r => r.style.display !== 'none').length : 0;
      const costPerRequest = totalRequests > 0 ? totalCost / totalRequests : 0;

      const statsItems = document.querySelectorAll('.stat-item');
      statsItems.forEach(item => {{
        const label = item.querySelector('.stat-label').textContent.trim();
        if (label === 'Users:') {{
          item.querySelector('.stat-value').textContent = userCount;
        }} else if (label === 'Total Cost:') {{
          item.querySelector('.stat-value').textContent = '$' + totalCost.toFixed(2);
        }} else if (label === 'Total Requests:') {{
          item.querySelector('.stat-value').textContent = totalRequests.toLocaleString();
        }} else if (label === 'Average Cost/Request:') {{
          item.querySelector('.stat-value').textContent = '$' + costPerRequest.toFixed(2);
        }}
      }});

      updateDimensionVisibility(selectedSources);
    }}

    // Attach filter change handlers
    document.querySelectorAll('.team-filter').forEach(checkbox => {{
      checkbox.addEventListener('change', applyTeamFilter);
    }});
    document.querySelectorAll('.source-filter').forEach(checkbox => {{
      checkbox.addEventListener('change', applyTeamFilter);
    }});

    const selectAllSourcesButton = document.getElementById('select-all-sources');
    if (selectAllSourcesButton) {{
      selectAllSourcesButton.addEventListener('click', () => {{
        document.querySelectorAll('.source-filter').forEach(checkbox => checkbox.checked = true);
        applyTeamFilter();
      }});
    }}

    const clearAllSourcesButton = document.getElementById('clear-all-sources');
    if (clearAllSourcesButton) {{
      clearAllSourcesButton.addEventListener('click', () => {{
        document.querySelectorAll('.source-filter').forEach(checkbox => checkbox.checked = false);
        applyTeamFilter();
      }});
    }}

    // Select All button handler
    const selectAllButton = document.getElementById('select-all-teams');
    if (selectAllButton) {{
      selectAllButton.addEventListener('click', () => {{
        document.querySelectorAll('.team-filter').forEach(checkbox => {{
          checkbox.checked = true;
        }});
        applyTeamFilter();
      }});
    }}

    // Clear All button handler
    const clearAllButton = document.getElementById('clear-all-teams');
    if (clearAllButton) {{
      clearAllButton.addEventListener('click', () => {{
        document.querySelectorAll('.team-filter').forEach(checkbox => {{
          checkbox.checked = false;
        }});
        applyTeamFilter();
      }});
    }}

    // Initialize tables with default sort
    window.addEventListener('DOMContentLoaded', () => {{
      // Sort Team Summary table by Team (first column) ascending
      const teamTable = document.getElementById('team-summary-table');
      if (teamTable) {{
        const teamHeader = teamTable.querySelector('tbody').parentElement.querySelector('tr:last-child th:first-child');
        if (teamHeader) {{
          sortTable(teamHeader);
        }}
      }}

      // Sort Users table by Name (first column) ascending
      const usersTable = document.getElementById('active-users-table');
      if (usersTable) {{
        const nameHeader = usersTable.querySelector('tbody').parentElement.querySelector('tr:last-child th:first-child');
        if (nameHeader) {{
          sortTable(nameHeader);
        }}
      }}
      applyTeamFilter();
    }});
  </script>
</body>
</html>'''

    return html


def main(params_file, output_path):
    """Generate report from params file."""
    with open(params_file, 'r') as f:
        params = json.load(f)

    teams = params.get('teams', [])
    time_period = params.get('time_period', 'mtd')
    period_label = params.get('period_label', 'Month to Date')
    members = params.get('members', [])
    usage_by_email = params.get('usage_by_email', {})
    models = sorted(params.get('models', []))

    html = generate_html(teams, time_period, period_label, members, usage_by_email, models)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(html)

    print(json.dumps({'success': True, 'output': output_path}))


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print(json.dumps({"error": "Usage: python -m managertools.tools.team_usage_report PARAMS_JSON OUTPUT_PATH"}), file=sys.stderr)
        sys.exit(1)

    try:
        main(sys.argv[1], sys.argv[2])
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        sys.exit(1)
