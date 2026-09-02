#!/usr/bin/env python3
"""Render the separate Codex usage report."""
import html


def _n(value):
    return f'{int(value or 0):,}'


def generate_html(params):
    members = params.get('members', [])
    usage = params.get('usage_by_email', {})
    models = params.get('models', [])
    tools = params.get('tools', [])
    rows = []
    for member in members:
        email = member['email'].lower()
        row = usage.get(email, {})
        rows.append((member, row))
    rows.sort(key=lambda pair: pair[1].get('events', 0), reverse=True)
    model_headers = ''.join(f'<th>{html.escape(m)}</th>' for m in models)
    tool_headers = ''.join(f'<th>{html.escape(t)}</th>' for t in tools)
    body = ''
    for member, row in rows:
        model_cells = ''.join(f'<td>{_n(row.get("models", {}).get(m))}</td>' for m in models)
        tool_cells = ''.join(f'<td>{_n(row.get("tools", {}).get(t))}</td>' for t in tools)
        body += '<tr>' + ''.join(f'<td>{html.escape(str(member.get(k, "")))}</td>' for k in ('name', 'team', 'email'))
        body += ''.join(f'<td>{_n(row.get(k))}</td>' for k in ('active_days', 'events', 'conversations', 'sessions', 'tool_calls'))
        body += model_cells + tool_cells + '</tr>'
    return f'''<!doctype html><html><head><meta charset="utf-8"><title>Codex Team Usage Report</title>
<style>body{{font:14px system-ui,sans-serif;margin:24px;color:#182230}}h1{{margin-bottom:4px}}.meta{{color:#64748b;margin-bottom:20px}}.wrap{{overflow:auto;max-height:75vh}}table{{border-collapse:collapse;white-space:nowrap}}th,td{{padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:right}}th{{position:sticky;top:0;background:#e8f0ff;text-align:right}}th:first-child,td:first-child,th:nth-child(2),td:nth-child(2),th:nth-child(3),td:nth-child(3){{text-align:left}}tr:hover{{background:#f8fafc}}</style></head><body>
<h1>Codex Team Usage Report</h1><div class="meta">{html.escape(params.get('period_label', ''))} · {len(rows)} roster members</div>
<div class="wrap"><table><thead><tr><th>Name</th><th>Team</th><th>Email</th><th>Active Days</th><th>Events</th><th>Conversations</th><th>Sessions</th><th>Tool Calls</th>{model_headers}{tool_headers}</tr></thead><tbody>{body}</tbody></table></div></body></html>'''
