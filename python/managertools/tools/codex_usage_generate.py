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


def query_logs(config_mgr, start, end, query):
    if not config_mgr.contains_key('datadogPAT'):
        raise RuntimeError("datadogPAT not configured in ~/.managerTools.cfg")
    start_dt = datetime.combine(start, datetime.min.time(), timezone.utc)
    end_dt = datetime.combine(end, datetime.max.time(), timezone.utc)
    body = {'filter': {'from': start_dt.isoformat(), 'to': end_dt.isoformat(), 'query': query},
            'sort': 'timestamp', 'page': {'limit': 1000}}
    results = []
    cursor = None
    while True:
        request_body = dict(body)
        if cursor:
            request_body['page'] = {'limit': 1000, 'cursor': cursor}
        request = urllib.request.Request(
            'https://api.datadoghq.com/api/v2/logs/events/search',
            data=json.dumps(request_body).encode(),
            headers={'DD-API-KEY': config_mgr.get_value('datadogPAT'),
                     'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                payload = json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            if exc.code in (401, 403):
                raise RuntimeError(f"Datadog Logs API returned HTTP {exc.code}; check datadogPAT and logs_read_data/logs_read_index_data scopes") from exc
            raise RuntimeError(f"Datadog Logs API returned HTTP {exc.code}; retry later and verify the Datadog site/endpoint") from exc
        results.extend(payload.get('data', []))
        cursor = payload.get('meta', {}).get('page', {}).get('after')
        if not cursor:
            return results


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


def aggregate_logs(logs_by_type, roster):
    allowed = {member['email'].lower() for member in roster}
    usage = {}
    for kind, logs in logs_by_type.items():
        for item in logs:
            attrs = _event_attributes(item)
            email = str(_value(attrs, 'user.email')).lower()
            if email not in allowed:
                continue
            model = str(_value(attrs, 'model', 'Unknown'))
            tool = str(_value(attrs, 'tool_name', 'Unknown')) if kind == 'tool_calls' else ''
            timestamp = attrs.get('timestamp') or item.get('attributes', {}).get('timestamp', '')
            day = str(timestamp)[:10]
            entry = usage.setdefault(email, {'events': 0, 'conversations': 0, 'tool_calls': 0, 'sessions': set(), 'active_days': set(), 'models': {}, 'tools': {}})
            entry[kind] += 1
            if day:
                entry['active_days'].add(day)
            session = _value(attrs, 'session_id')
            if session:
                entry['sessions'].add(str(session))
            if kind in ('events', 'conversations'):
                entry['models'][model] = entry['models'].get(model, 0) + 1
            if kind == 'tool_calls':
                entry['tools'][tool] = entry['tools'].get(tool, 0) + 1
    for entry in usage.values():
        entry['sessions'] = len(entry['sessions'])
        entry['active_days'] = len(entry['active_days'])
    return usage


def main(teams_str, time_period, output_path):
    config = ConfigFileManager('.managerTools.cfg')
    teams = config.get_value('orgTeams') if teams_str.lower() == 'org' else [t.strip() for t in teams_str.split(',')]
    start, end = get_date_range(time_period)
    roster = fetch_rosters(teams, config)
    logs = {kind: query_logs(config, start, end, query) for kind, query in EVENT_QUERIES.items()}
    usage = aggregate_logs(logs, roster)
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
