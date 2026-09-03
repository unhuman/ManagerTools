#!/usr/bin/env python3
"""Generate a separate Codex usage report from Datadog log events.

Usage:
  python -m managertools.tools.codex_usage_generate -t TEAM mtd report.html

Codex usage is derived from log event counts rather than Cloud Cost metrics:
conversation_starts are sessions, sse_event entries are interactions, and
tool_result entries are tool calls.
"""
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import date, datetime, timedelta, timezone

from managertools.rest.backstage_rest import BackstageREST
from managertools.util.backstage_cache import BackstageCache
from managertools.util.config_file_manager import ConfigFileManager


EVENT_QUERIES = {
    'events': 'service:codex* env:cvent @event.name:codex.sse_event @user.email:* -@service_tier:*',
    'conversations': 'service:codex* env:cvent @event.name:codex.conversation_starts',
    'tool_calls': 'service:codex* env:cvent @event.name:codex.tool_result @tool_name:*',
}


def get_date_range(time_period):
    today = date.today()
    if time_period == 'mtd':
        start, end = date(today.year, today.month, 1), today
    elif time_period == 'last-month':
        end = date(today.year, today.month, 1) - timedelta(days=1)
        start = date(end.year, end.month, 1)
    elif time_period == 'past-month':
        start, end = today - timedelta(days=29), today
    elif time_period.endswith('d') and time_period[:-1].isdigit() and 1 <= int(time_period[:-1]) <= 30:
        start, end = today - timedelta(days=int(time_period[:-1]) - 1), today
    else:
        raise ValueError("time_period must be 'mtd', 'last-month', 'past-month', or Nd (1-30)")
    return start, end


def period_label(time_period, start, end):
    if time_period == 'mtd':
        return f"{start.strftime('%B')} MTD ({start.strftime('%b %d')} - {end.strftime('%b %d, %Y')})"
    if time_period == 'last-month':
        return f"{start.strftime('%B %Y')} ({start.strftime('%b %d')} - {end.strftime('%b %d, %Y')})"
    return f"{start.strftime('%b %d')} - {end.strftime('%b %d, %Y')}"


def _value(attributes, path, default=''):
    if isinstance(attributes, dict):
        for key in (path, f'@{path}'):
            if key in attributes and attributes[key] not in (None, ''):
                return attributes[key]
    current = attributes
    for part in path.split('.'):
        if not isinstance(current, dict):
            return default
        current = current.get(part, current.get(f'@{part}', default))
    return current if current not in (None, '') else default


def _event_attributes(item):
    attrs = item.get('attributes', {})
    nested = attrs.get('attributes', {}) if isinstance(attrs, dict) else {}
    return {**(nested if isinstance(nested, dict) else {}), **(attrs if isinstance(attrs, dict) else {})}


def query_log_aggregate(config_mgr, start, end, query, include_sessions=False):
    """Ask Datadog for grouped counts instead of downloading individual logs."""
    if not config_mgr.contains_key('datadogPAT'):
        raise RuntimeError("datadogPAT not configured in ~/.managerTools.cfg")
    start_dt = datetime.combine(start, datetime.min.time(), timezone.utc)
    end_dt = datetime.combine(end, datetime.max.time(), timezone.utc)
    computes = [{'aggregation': 'count', 'type': 'total'}]
    if include_sessions:
        computes.append({'aggregation': 'cardinality', 'metric': '@session_id', 'type': 'cardinality'})
    body = {
        'compute': computes,
        'filter': {'from': start_dt.isoformat(), 'to': end_dt.isoformat(), 'query': query},
        'group_by': [
            {'facet': '@user.email', 'limit': 10000},
            {'facet': '@model', 'limit': 1000},
        ],
    }
    if '@tool_name:' in query:
        body['group_by'].append({'facet': '@tool_name', 'limit': 1000})
    request = urllib.request.Request(
        'https://api.datadoghq.com/api/v2/logs/analytics/aggregate',
        data=json.dumps(body).encode(),
        headers={'DD-APPLICATION-KEY': config_mgr.get_value('datadogPAT'),
                 'Content-Type': 'application/json'}, method='POST')
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = json.loads(response.read().decode())
    except urllib.error.HTTPError as exc:
        if exc.code in (401, 403):
            raise RuntimeError(f"Datadog Logs API returned HTTP {exc.code}; verify that datadogPAT is valid and has logs_read_data and logs_read_index_data scopes") from exc
        raise RuntimeError(f"Datadog Logs API returned HTTP {exc.code}; retry later and verify the Datadog site/endpoint") from exc
    return payload.get('data', {}).get('buckets', [])


def _compute(bucket, index):
    computes = bucket.get('computes', {})
    if isinstance(computes, list):
        return computes[index] if index < len(computes) else 0
    return computes.get(f'c{index}', 0) or 0


def _bucket_value(bucket, facet, default=''):
    by = bucket.get('by', {})
    return _value(by, facet.lstrip('@'), by.get(facet, default))


def fetch_rosters(teams, config_mgr):
    if not config_mgr.contains_key('backstageServer'):
        raise RuntimeError("backstageServer not configured in ~/.managerTools.cfg")
    server = config_mgr.get_value('backstageServer')
    auth = config_mgr.get_value('backstageAuth') if config_mgr.contains_key('backstageAuth') else None
    cache = BackstageCache(cache_ttl_days=int(config_mgr.get_value('backstageCacheDays')) if config_mgr.contains_key('backstageCacheDays') else 7)
    backstage = BackstageREST(server, auth)
    members = {}
    for team in teams:
        roster = cache.get(team)
        if roster is None:
            roster = backstage.get_team_roster(team)
            if roster:
                cache.put(team, roster)
        for member in roster or []:
            email = _value(member.get('raw_entity', {}).get('spec', {}).get('profile', {}), 'email').lower()
            if email and email not in members:
                members[email] = {'name': member.get('display_name', ''), 'email': email, 'team': team}
    return list(members.values())


def aggregate_logs(config_mgr, start, end, roster):
    """Aggregate Codex events server-side and normalize them for the report."""
    allowed = {member['email'].lower() for member in roster}
    usage = {}
    for kind, query in EVENT_QUERIES.items():
        print(f"📊 Codex: aggregating {kind}...", file=sys.stderr, flush=True)
        buckets = query_log_aggregate(config_mgr, start, end, query, include_sessions=(kind == 'conversations'))
        print(f"   ✓ {kind}: received {len(buckets)} grouped result(s)", file=sys.stderr, flush=True)
        for bucket in buckets:
            email = str(_bucket_value(bucket, '@user.email')).lower()
            if email not in allowed:
                continue
            model = str(_bucket_value(bucket, '@model', 'Unknown'))
            tool = str(_bucket_value(bucket, '@tool_name', 'Unknown'))
            entry = usage.setdefault(email, {'events': 0, 'conversations': 0, 'tool_calls': 0,
                                             'sessions': 0, 'active_days': 0, 'models': {}, 'tools': {}})
            count = int(_compute(bucket, 0))
            entry[kind] += count
            if kind == 'conversations':
                entry['sessions'] += int(_compute(bucket, 1))
            if kind in ('events', 'conversations'):
                entry['models'][model] = entry['models'].get(model, 0) + count
            if kind == 'tool_calls':
                entry['tools'][tool] = entry['tools'].get(tool, 0) + count

    current = start
    while current <= end:
        print(f"📅 Codex: checking active users for {current.isoformat()}...", file=sys.stderr, flush=True)
        for bucket in query_log_aggregate(config_mgr, current, current, EVENT_QUERIES['conversations']):
            email = str(_bucket_value(bucket, '@user.email')).lower()
            if email in usage and _compute(bucket, 0):
                usage[email]['active_days'] += 1
        current += timedelta(days=1)
    print(f"✓ Codex aggregation complete: {len(usage)} user(s) with usage", file=sys.stderr, flush=True)
    return usage


def main(teams_str, time_period, output_path):
    config = ConfigFileManager('.managerTools.cfg')
    teams = config.get_value('orgTeams') if teams_str.lower() == 'org' else [t.strip() for t in teams_str.split(',')]
    start, end = get_date_range(time_period)
    roster = fetch_rosters(teams, config)
    usage = aggregate_logs(config, start, end, roster)
    params = {'teams': teams, 'time_period': time_period, 'period_label': period_label(time_period, start, end),
              'members': roster, 'usage_by_email': usage,
              'models': sorted({m for row in usage.values() for m in row['models']}),
              'tools': sorted({t for row in usage.values() for t in row['tools']})}
    from managertools.tools.codex_usage_report import generate_html
    output_path = os.path.expanduser(output_path)
    os.makedirs(os.path.dirname(output_path) or '.', exist_ok=True)
    with open(output_path, 'w') as output:
        output.write(generate_html(params))
    print(json.dumps({'success': True, 'output': output_path, 'members': len(roster),
                      'active_users': len(usage), 'events': sum(v['events'] for v in usage.values()),
                      'conversations': sum(v['conversations'] for v in usage.values()),
                      'tool_calls': sum(v['tool_calls'] for v in usage.values())}))


if __name__ == '__main__':
    if len(sys.argv) != 5 or sys.argv[1] != '-t':
        print('Usage: python -m managertools.tools.codex_usage_generate -t TEAMS TIME_PERIOD OUTPUT_PATH', file=sys.stderr)
        sys.exit(1)
    try:
        main(sys.argv[2], sys.argv[3], sys.argv[4])
    except Exception as exc:
        print(json.dumps({'error': str(exc)}), file=sys.stderr)
        sys.exit(1)
