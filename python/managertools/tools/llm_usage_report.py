"""Render a source-selectable Claude/Codex usage report."""
import html
import json


def _n(value):
    return f'{int(value or 0):,}'


def generate_html(params):
    members = params['members']
    sources = params['sources']
    source_labels = {'claude': 'Claude', 'codex': 'Codex'}
    columns = [('cost', 'Cost'), ('events', 'Events'), ('conversations', 'Conversations'),
               ('sessions', 'Sessions'), ('tool_calls', 'Tool Calls')]
    dimensions = []
    for source in sources:
        source_rows = params['usage'].get(source, {})
        models = sorted({model for row in source_rows.values() for model in row.get('models', {})})
        apps = sorted({app for row in source_rows.values() for app in row.get('applications', {})})
        for model in models:
            dimensions.append((source, 'models', model, f'{source_labels.get(source, source.title())}: {model}'))
        for app in apps:
            dimensions.append((source, 'applications', app, f'{source_labels.get(source, source.title())}: {app}'))
    headers = ''.join(f'<th class="source-{s}" data-source="{s}">{html.escape(label)}</th>' for s, _, _, label in dimensions)
    rows_html = ''
    for member in members:
        email = member['email'].lower()
        values = {source: params['usage'].get(source, {}).get(email, {}) for source in sources}
        safe_values = json.dumps(values, separators=(',', ':')).replace('&', '\\u0026')
        cells = ''.join(f'<td class="source-{source}" data-source="{source}">{_n(values.get(source, {}).get(kind, 0))}</td>' for kind in () for source in ())
        dim_cells = ''.join(f'<td class="source-{source}" data-source="{source}">{_n(values.get(source, {}).get(group, {}).get(key, 0))}</td>' for source, group, key, _ in dimensions)
        rows_html += f'''<tr data-sources='{html.escape(safe_values, quote=True)}'>
<td>{html.escape(str(member.get('name', '')))}</td><td>{html.escape(str(member.get('team', '')))}</td><td>{html.escape(email)}</td>
<td class="aggregate active-days">0</td><td class="aggregate cost">0</td><td class="aggregate events">0</td><td class="aggregate conversations">0</td><td class="aggregate sessions">0</td><td class="aggregate tool-calls">0</td>{dim_cells}</tr>'''
    source_checks = ''.join(f'<label><input type="checkbox" class="source-toggle" value="{s}" checked> {html.escape(source_labels.get(s, s.title()))}</label>' for s in sources)
    return f'''<!doctype html><html><head><meta charset="utf-8"><title>LLM Team Usage Report</title>
<style>body{{font:14px system-ui,sans-serif;margin:24px;color:#182230}}h1{{margin-bottom:4px}}.meta{{color:#64748b;margin-bottom:16px}}.sources{{display:flex;gap:18px;margin:12px 0 18px}}.wrap{{overflow:auto;max-height:75vh}}table{{border-collapse:collapse;white-space:nowrap}}th,td{{padding:8px 10px;border-bottom:1px solid #e2e8f0;text-align:right}}th{{position:sticky;top:0;background:#e8f0ff}}th:first-child,td:first-child,th:nth-child(2),td:nth-child(2),th:nth-child(3),td:nth-child(3){{text-align:left}}.hidden{{display:none}}</style></head><body>
<h1>LLM Team Usage Report</h1><div class="meta">{html.escape(params.get('period_label', ''))}</div><div class="sources">{source_checks}</div>
<div class="wrap"><table><thead><tr><th>Name</th><th>Team</th><th>Email</th><th>Active Days</th><th>Cost</th><th>Events</th><th>Conversations</th><th>Sessions</th><th>Tool Calls</th>{headers}</tr></thead><tbody>{rows_html}</tbody></table></div>
<script>
const toggles=[...document.querySelectorAll('.source-toggle')];
function refresh(){{const selected=new Set(toggles.filter(x=>x.checked).map(x=>x.value));
document.querySelectorAll('[data-source]').forEach(x=>x.classList.toggle('hidden',!selected.has(x.dataset.source)));
document.querySelectorAll('tbody tr').forEach(row=>{{const data=JSON.parse(row.dataset.sources), active=new Set(), total={{cost:0,events:0,conversations:0,sessions:0,tool_calls:0}};
selected.forEach(source=>{{const value=data[source]||{{}}; Object.keys(total).forEach(k=>total[k]+=Number(value[k]||0)); (value.active_day_dates||[]).forEach(day=>active.add(day));}});
row.querySelector('.active-days').textContent=active.size.toLocaleString(); row.querySelector('.cost').textContent='$'+total.cost.toFixed(2); row.querySelector('.events').textContent=total.events.toLocaleString(); row.querySelector('.conversations').textContent=total.conversations.toLocaleString(); row.querySelector('.sessions').textContent=total.sessions.toLocaleString(); row.querySelector('.tool-calls').textContent=total.tool_calls.toLocaleString();}});}}
toggles.forEach(x=>x.addEventListener('change',refresh)); refresh();
</script></body></html>'''
