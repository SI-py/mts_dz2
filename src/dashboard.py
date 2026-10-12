"""Self-contained HTML dashboard. No CDN, server, external JS or personal data."""
import json
from datetime import datetime, timezone
from pathlib import Path

from .db import SQL_DIR


def create_dashboard(client, output: Path):
    states = client.query((SQL_DIR / '06_dashboard_states.sql').read_text()).named_results()
    states = list(states)
    if not states:
        raise ValueError('Нет транзакций. Сначала выполните load.')
    categories = list(client.query((SQL_DIR / '07_dashboard_categories.sql').read_text()).named_results())
    datasets = list(client.query('''
        SELECT dataset_id, count() AS rows FROM homework.transactions
        GROUP BY dataset_id ORDER BY dataset_id
    ''').named_results())
    payload = {'states': states, 'categories': categories, 'datasets': datasets,
               'generated': datetime.now(timezone.utc).isoformat(timespec='seconds'),
               'version': client.command('SELECT version()')}
    # A CSV-controlled string must never terminate the script element.
    serialized = json.dumps(payload, ensure_ascii=False).replace('<', '\\u003c').replace('>', '\\u003e').replace('&', '\\u0026')
    template = (Path(__file__).parent / 'dashboard.html').read_text()
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(template.replace('__PAYLOAD__', serialized), encoding='utf-8')
    return len(states)
