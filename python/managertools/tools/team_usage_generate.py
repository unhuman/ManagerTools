#!/usr/bin/env python3
"""
Generate a complete Claude Code team usage report end-to-end.

Fetches rosters from Backstage, queries Datadog for usage, builds params,
and generates an interactive HTML report - all in one command.

Usage:
  python -m managertools.tools.team_usage_generate TEAMS TIME_PERIOD [--output PATH]

Arguments:
  TEAMS: Team name(s) comma-separated, or 'org' to use orgTeams from config
  TIME_PERIOD: 'mtd' (month-to-date) or 'past-month'
  --output PATH: Optional output HTML path (default: ~/claude_team_usage.html)

Configuration required in ~/.managerTools.cfg:
  - backstageServer: Backstage FQDN
  - datadogPAT: Datadog Personal Access Token
  - orgTeams: Array of team names (if using 'org' parameter)
"""
import sys
import json
import os
import socket
import urllib.request
import urllib.parse
import time
from datetime import date, datetime, timedelta

from managertools.util.config_file_manager import ConfigFileManager
from managertools.rest.backstage_rest import BackstageREST
from managertools.util.backstage_cache import BackstageCache
from managertools.tools.team_usage_report import generate_html


def get_date_range(time_period):
    """Get start and end timestamps for the period (milliseconds since epoch)."""
    today = date.today()

    if time_period == 'mtd':
        start_date = date(today.year, today.month, 1)
        end_date = today
    elif time_period == 'past-month':
        if today.month == 1:
            start_date = date(today.year - 1, 12, 1)
            end_date = date(today.year - 1, 12, 31)
        else:
            start_date = date(today.year, today.month - 1, 1)
            end_date = date(today.year, today.month, 1) - timedelta(days=1)
    else:
        raise ValueError("time_period must be 'mtd' or 'past-month'")

    # Convert to milliseconds since epoch (Datadog API expects this format)
    start_dt = datetime.combine(start_date, datetime.min.time())
    end_dt = datetime.combine(end_date, datetime.max.time())

    start_ms = int(start_dt.timestamp() * 1000)
    end_ms = int(end_dt.timestamp() * 1000)

    return start_ms, end_ms, start_date, end_date


def get_period_label(time_period):
    """Generate human-readable period label."""
    today = date.today()
    if time_period == 'mtd':
        month_name = today.strftime('%B %Y')
        return f"{month_name} (MTD)"
    elif time_period == 'past-month':
        if today.month == 1:
            month_name = date(today.year - 1, 12, 1).strftime('%B %Y')
        else:
            month_name = date(today.year, today.month - 1, 1).strftime('%B %Y')
        return month_name
    return time_period


def fetch_rosters(teams, config_mgr):
    """Fetch team rosters from Backstage."""
    if not config_mgr.contains_key('backstageServer'):
        raise RuntimeError("backstageServer not configured in ~/.managerTools.cfg")

    backstage_server = config_mgr.get_value('backstageServer')
    backstage_auth = config_mgr.get_value('backstageAuth') if config_mgr.contains_key('backstageAuth') else None
    backstage_cache_days = int(config_mgr.get_value('backstageCacheDays')) if config_mgr.contains_key('backstageCacheDays') else 7

    backstage = BackstageREST(backstage_server, backstage_auth)
    cache = BackstageCache(cache_ttl_days=backstage_cache_days)

    members_by_email = {}

    for team_name in teams:
        roster = cache.get(team_name)
        if roster is None:
            roster = backstage.get_team_roster(team_name)
            if roster:
                cache.put(team_name, roster)

        if roster:
            for member in roster:
                raw_entity = member.get('raw_entity', {})
                email_raw = raw_entity.get('spec', {}).get('profile', {}).get('email')

                if email_raw:
                    email = email_raw.lower()
                    if email not in members_by_email:
                        members_by_email[email] = {
                            'name': member.get('display_name', ''),
                            'email': email,
                            'team': team_name
                        }

    return list(members_by_email.values())


def query_datadog(config_mgr, start_ms, end_ms, resume_id=None):
    """Query Datadog for all Anthropic product usage by email, model, and product."""
    if not config_mgr.contains_key('datadogPAT'):
        raise RuntimeError("datadogPAT not configured in ~/.managerTools.cfg")

    pat = config_mgr.get_value('datadogPAT')

    # Datadog logs query API endpoint - include all services (claude-code, claude-web, etc.)
    query = "service:claude* @event.name:api_request"

    headers = {
        'Authorization': f'Bearer {pat}',
        'Content-Type': 'application/json'
    }

    usage_by_email_model_product = {}
    page = 0
    cursor = None
    total_logs = 0

    # Try to resume from saved checkpoint
    checkpoint_file = None
    if resume_id:
        checkpoint_file = f"/tmp/datadog_checkpoint_{resume_id}.json"
        if os.path.exists(checkpoint_file):
            try:
                with open(checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    cursor = checkpoint.get('cursor')
                    page = checkpoint.get('page', 0)
                    total_logs = checkpoint.get('total_logs', 0)

                    # Restore accumulated usage data
                    saved_usage = checkpoint.get('usage', {})
                    for key_str, data in saved_usage.items():
                        parts = key_str.split('|')
                        email, model, product = parts[0], parts[1], parts[2] if len(parts) > 2 else ''
                        usage_by_email_model_product[(email, model, product)] = {
                            'cost': data['cost'],
                            'requests': data['requests'],
                            'sessions': set(data['sessions'])
                        }

                    print(f"\n✓ Resuming from checkpoint: Page {page}, Total: {total_logs}, Recovered {len(usage_by_email_model_product)} user/model/product tuples", file=sys.stderr, flush=True)
            except Exception as e:
                print(f"\n⚠ Failed to load checkpoint: {e}", file=sys.stderr)
                pass

    try:
        while True:
            page += 1

            # Build URL with pagination
            base_url = f"https://api.datadoghq.com/api/v2/logs/events?filter[query]={urllib.parse.quote(query)}&filter[from]={start_ms}&filter[to]={end_ms}&page[limit]=1000"
            if cursor:
                url = f"{base_url}&page[cursor]={urllib.parse.quote(cursor)}"
            else:
                url = base_url

            # Retry logic for connection issues - infinite retries with exponential backoff
            retry_count = 0
            max_retry_wait = 300  # Cap backoff at 5 minutes
            data = None

            while data is None:
                try:
                    req = urllib.request.Request(url, headers=headers)
                    with urllib.request.urlopen(req, timeout=30) as response:
                        data = json.loads(response.read().decode('utf-8'))

                        # Check rate limit headers
                        remaining = response.headers.get('X-RateLimit-Remaining')
                        limit = response.headers.get('X-RateLimit-Limit')
                        reset = response.headers.get('X-RateLimit-Reset')

                        if remaining and limit:
                            remaining_int = int(remaining)
                            limit_int = int(limit)

                            # If we're running low on requests, sleep until reset
                            if remaining_int < 5:
                                reset_int = int(reset) if reset else None
                                if reset_int:
                                    wait_until_reset = reset_int - int(time.time())
                                    if wait_until_reset > 0:
                                        print(f"\r⏱ Rate limit approaching ({remaining_int}/{limit_int} remaining), sleeping {wait_until_reset}s...", file=sys.stderr, flush=True)
                                        time.sleep(wait_until_reset + 1)  # +1 to be safe

                except (urllib.error.URLError, ConnectionResetError, BrokenPipeError, TimeoutError, socket.timeout) as e:
                    retry_count += 1
                    wait_time = min(2 ** retry_count, max_retry_wait)  # Exponential backoff, capped at 5 minutes
                    error_type = type(e).__name__
                    print(f"\r⚠ {error_type} on page {page}, retrying in {wait_time}s (attempt {retry_count})...", file=sys.stderr, flush=True)
                    time.sleep(wait_time)

            if data is None:
                break

            if not data.get('data'):
                break

            logs_in_page = len(data['data'])
            total_logs += logs_in_page

            # Extract timestamp from last entry in page for progress indication
            latest_timestamp = ""
            if data['data']:
                last_log = data['data'][-1]
                ts_raw = last_log.get('attributes', {}).get('attributes', {}).get('event', {}).get('timestamp')
                if ts_raw:
                    # Convert ISO format to readable format
                    try:
                        # Parse ISO timestamp and format as readable date/time
                        from datetime import datetime as dt_module
                        dt_obj = dt_module.fromisoformat(ts_raw.replace('Z', '+00:00'))
                        latest_timestamp = f" - Latest: {dt_obj.strftime('%Y-%m-%d %H:%M:%S')}"
                    except:
                        pass

            print(f"\rPage {page}: Fetched {logs_in_page} log entries (total: {total_logs}){latest_timestamp}", file=sys.stderr, end='', flush=True)

            # Process logs
            for log in data['data']:
                # Extract product from top-level service field
                product = log.get('attributes', {}).get('service', '')
                attrs = log.get('attributes', {}).get('attributes', {})

                # Extract email from user object
                user = attrs.get('user', {})
                email = (user.get('normalized_email') or user.get('email') or '').lower()

                # Extract model and cost
                model = attrs.get('model', '')
                cost = float(attrs.get('cost_usd', 0) or 0)

                # Extract session ID
                session_id = attrs.get('session', {}).get('id', '')

                if email and model and product:
                    key = (email, model, product)
                    if key not in usage_by_email_model_product:
                        usage_by_email_model_product[key] = {
                            'cost': 0,
                            'requests': 0,
                            'sessions': set()
                        }

                    usage_by_email_model_product[key]['cost'] += cost
                    usage_by_email_model_product[key]['requests'] += 1

                    if session_id:
                        usage_by_email_model_product[key]['sessions'].add(session_id)

            # Save checkpoint for resume capability
            if checkpoint_file:
                next_cursor = data.get('meta', {}).get('page', {}).get('after')
                checkpoint_data = {
                    'page': page,
                    'cursor': next_cursor,
                    'total_logs': total_logs,
                    'usage': {
                        f"{email}|{model}|{product}": {
                            'cost': usage_data['cost'],
                            'requests': usage_data['requests'],
                            'sessions': list(usage_data['sessions'])
                        }
                        for (email, model, product), usage_data in usage_by_email_model_product.items()
                    }
                }
                try:
                    with open(checkpoint_file, 'w') as f:
                        json.dump(checkpoint_data, f)
                except:
                    pass  # Fail silently if checkpoint can't be saved

            # Check for next page
            cursor = data.get('meta', {}).get('page', {}).get('after')
            if not cursor:
                break

    except urllib.error.HTTPError as e:
        error_body = e.read().decode('utf-8')
        print(f"Datadog API error ({e.code}): {error_body}", file=sys.stderr)
        raise RuntimeError(f"Failed to query Datadog: {e}")
    except Exception as e:
        print(f"Error querying Datadog: {e}", file=sys.stderr)
        raise

    # Clean up checkpoint file on successful completion
    if checkpoint_file and os.path.exists(checkpoint_file):
        try:
            os.remove(checkpoint_file)
        except:
            pass  # Fail silently if cleanup fails

    # Convert to flat array, grouped by email, with breakdown by model and product
    usage_by_email = {}
    for (email, model, product), data in usage_by_email_model_product.items():
        if email not in usage_by_email:
            usage_by_email[email] = {
                'cost': 0,
                'requests': 0,
                'sessions': set(),
                'model_costs': {},
                'product_costs': {}
            }

        usage_by_email[email]['cost'] += data['cost']
        usage_by_email[email]['requests'] += data['requests']
        usage_by_email[email]['sessions'].update(data['sessions'])

        # Track cost by model
        if model not in usage_by_email[email]['model_costs']:
            usage_by_email[email]['model_costs'][model] = 0
        usage_by_email[email]['model_costs'][model] += data['cost']

        # Track cost by product
        if product not in usage_by_email[email]['product_costs']:
            usage_by_email[email]['product_costs'][product] = 0
        usage_by_email[email]['product_costs'][product] += data['cost']

    # Convert to flat array for downstream processing
    usage_data = []
    for email, data in usage_by_email.items():
        usage_data.append({
            'email': email,
            'cost': round(data['cost'], 2),
            'requests': data['requests'],
            'sessions': len(data['sessions']),
            'model_costs': {m: round(c, 2) for m, c in data['model_costs'].items()},
            'product_costs': {p: round(c, 2) for p, c in data['product_costs'].items()}
        })

    print(f"\nPagination complete: Fetched {total_logs} total log entries across {page} page(s)", file=sys.stderr)

    return usage_data


def build_params(roster, usage_data, teams, time_period):
    """Build params from roster and usage data."""
    usage_by_email = {}
    models = set()
    products = set()

    # Initialize from roster
    for member in roster:
        email = member['email']
        usage_by_email[email] = {
            'cost': 0,
            'requests': 0,
            'sessions': 0,
            'model_costs': {},
            'product_costs': {}
        }

    # Merge usage data
    for row in usage_data:
        email = row.get('email', '').lower()
        cost = row.get('cost', 0)
        requests = row.get('requests', 0)
        sessions = row.get('sessions', 0)

        if email in usage_by_email:
            usage_by_email[email]['cost'] += cost
            usage_by_email[email]['requests'] += requests
            usage_by_email[email]['sessions'] = max(usage_by_email[email]['sessions'], sessions)

            # Merge model costs
            model_costs = row.get('model_costs', {})
            for model, model_cost in model_costs.items():
                usage_by_email[email]['model_costs'][model] = model_cost
                models.add(model)

            # Merge product costs
            product_costs = row.get('product_costs', {})
            for product, product_cost in product_costs.items():
                usage_by_email[email]['product_costs'][product] = product_cost
                products.add(product)

    params = {
        'teams': teams,
        'time_period': time_period,
        'period_label': get_period_label(time_period),
        'members': roster,
        'usage_by_email': usage_by_email,
        'models': sorted(list(models)),
        'products': sorted(list(products))
    }

    return params


def main(teams_str, time_period, output_path=None):
    """Main entry point."""
    if time_period not in ('mtd', 'past-month'):
        raise ValueError("time_period must be 'mtd' or 'past-month'")

    if output_path is None:
        output_path = os.path.expanduser('~/claude_team_usage.html')
    else:
        output_path = os.path.expanduser(output_path)

    # Load config
    config_mgr = ConfigFileManager('.managerTools.cfg')

    # Parse teams
    if teams_str.lower() == 'org':
        if not config_mgr.contains_key('orgTeams'):
            raise RuntimeError("orgTeams not configured in ~/.managerTools.cfg")
        teams = config_mgr.get_value('orgTeams')
    else:
        teams = [t.strip() for t in teams_str.split(',')]

    print(f"Fetching rosters for teams: {', '.join(teams)}", file=sys.stderr)
    roster = fetch_rosters(teams, config_mgr)
    print(f"Found {len(roster)} team members", file=sys.stderr)

    # Get date range
    start_ms, end_ms, start_date, end_date = get_date_range(time_period)
    print(f"Querying Datadog for {start_date} to {end_date}", file=sys.stderr)
    print(f"Timestamps: {start_ms} to {end_ms} (ms)", file=sys.stderr)

    # Debug: show what the dates convert to
    from datetime import datetime as dt_module
    print(f"Start: {dt_module.fromtimestamp(start_ms/1000)} UTC", file=sys.stderr)
    print(f"End: {dt_module.fromtimestamp(end_ms/1000)} UTC", file=sys.stderr)

    # Query Datadog with resume capability
    # Use teams_str + time_period as resume ID so same query can resume
    resume_id = f"{teams_str}_{time_period}".replace(' ', '_').replace(',', '')
    usage_data = query_datadog(config_mgr, start_ms, end_ms, resume_id=resume_id)
    print(f"Found usage data for {len(set(u['email'] for u in usage_data))} users", file=sys.stderr)

    # Build params
    params = build_params(roster, usage_data, teams, time_period)

    # Generate HTML
    html = generate_html(
        teams,
        time_period,
        params['period_label'],
        roster,
        params['usage_by_email'],
        params['models'],
        params['products']
    )

    # Write output
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w') as f:
        f.write(html)

    print(json.dumps({
        'success': True,
        'output': output_path,
        'teams': teams,
        'members': len(roster),
        'active_users': sum(1 for u in params['usage_by_email'].values() if u['cost'] > 0),
        'total_cost': round(sum(u['cost'] for u in params['usage_by_email'].values()), 2),
        'models': params['models'],
        'products': params['products']
    }))


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print(json.dumps({
            "error": "Usage: python -m managertools.tools.team_usage_generate TEAMS TIME_PERIOD [--output PATH]"
        }), file=sys.stderr)
        sys.exit(1)

    teams = sys.argv[1]
    time_period = sys.argv[2]
    output_path = None

    if len(sys.argv) > 3 and sys.argv[3] == '--output' and len(sys.argv) > 4:
        output_path = sys.argv[4]

    try:
        main(teams, time_period, output_path)
    except Exception as e:
        print(json.dumps({"error": str(e)}), file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        sys.exit(1)
