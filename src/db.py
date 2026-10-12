import os
from pathlib import Path
import clickhouse_connect

SQL_DIR = Path(__file__).resolve().parent.parent / 'sql'
QUERIES = {
    'baseline': '03_baseline_query.sql',
    'raw': '04_optimized_raw_query.sql',
    'aggregate': '02_max_transaction_by_state.sql',
}


def connect():
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.getenv('CLICKHOUSE_PORT', '8123')),
        username=os.getenv('CLICKHOUSE_USER', 'homework'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'homework_local'),
        database=os.getenv('CLICKHOUSE_DATABASE', 'homework'),
        connect_timeout=10, send_receive_timeout=120,
    )


def query_text(mode='aggregate'):
    return (SQL_DIR / QUERIES[mode]).read_text()
