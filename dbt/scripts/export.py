"""Выгрузка витрин в CSV и итогов запуска в JSON."""
import csv
import json
from importlib.metadata import version
from pathlib import Path

from load import client

ROOT = Path('/app')
MARTS = ['mart_daily_state_metrics', 'mart_fraud_by_category', 'mart_fraud_by_state',
         'mart_customer_risk_profile', 'mart_hourly_fraud_pattern', 'mart_merchant_analytics']


def main():
    connection = client()
    summary = {'versions': {name: version(name) for name in ['dbt-core', 'dbt-clickhouse', 'sqlfluff']},
               'source_rows': connection.command('select count() from transactions_db.transactions'),
               'fraud_rows': connection.command('select sum(target) from transactions_db.transactions'),
               'models': {}}
    result_dir = ROOT / 'results'
    result_dir.mkdir(exist_ok=True)
    for model in MARTS:
        result = connection.query(f'select * from analytics_marts.{model} order by 1, 2')
        with (result_dir / f'{model}.csv').open('w', newline='') as stream:
            writer = csv.writer(stream)
            writer.writerow(result.column_names)
            writer.writerows(result.result_rows)
        summary['models'][model] = len(result.result_rows)
    with (result_dir / 'model_counts.csv').open('w', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['model', 'rows'])
        writer.writerows(summary['models'].items())
    result = json.loads((result_dir / 'dbt_test_results.json').read_text())
    summary['test_results'] = {status: sum(r['status'] == status for r in result['results'])
                               for status in sorted({r['status'] for r in result['results']})}
    (result_dir / 'summary.json').write_text(json.dumps(summary, indent=2) + '\n')
    print(json.dumps(summary, indent=2))


if __name__ == '__main__':
    main()
