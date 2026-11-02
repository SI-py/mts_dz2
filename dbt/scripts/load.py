"""Загрузка train.csv в transactions_db.transactions."""
import argparse
import csv
import hashlib
import math
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import clickhouse_connect

STRINGS = ['merch', 'cat_id', 'name_1', 'name_2', 'gender', 'street', 'one_city',
           'us_state', 'post_code', 'jobs']
FLOATS = ['amount', 'lat', 'lon', 'merchant_lat', 'merchant_lon']
COLUMNS = ['transaction_id', 'source_row', 'dataset_id', 'transaction_time', *STRINGS,
           *FLOATS, 'population_city', 'target']


def client():
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.getenv('CLICKHOUSE_PORT', '18124')),
        username=os.getenv('CLICKHOUSE_USER', 'homework'),
        password=os.getenv('CLICKHOUSE_PASSWORD', 'homework_local'),
    )


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def records(path, digest):
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        required = set(STRINGS + FLOATS + ['transaction_time', 'population_city', 'target'])
        if not required.issubset(reader.fieldnames or []):
            raise ValueError(f'Missing columns: {sorted(required - set(reader.fieldnames or []))}. '
                             'Use labeled train.csv, not test.csv.')
        for number, row in enumerate(reader, 1):
            try:
                if None in row or any(value is None for value in row.values()):
                    raise ValueError('Wrong number of CSV columns')
                floats = [float(row[name]) for name in FLOATS]
                if not all(math.isfinite(value) for value in floats):
                    raise ValueError('Non-finite number')
                amount = Decimal(row['amount'])
                if amount < 0 or amount != amount.quantize(Decimal('0.01')):
                    raise ValueError('Amount must be nonnegative with at most two decimals')
                if int(row['target']) not in (0, 1):
                    raise ValueError('target must be 0 or 1')
                if not all(row[name].strip() for name in STRINGS):
                    raise ValueError('Empty categorical field')
                population = int(row['population_city'])
                if population < 0:
                    raise ValueError('Negative population')
                timestamp = datetime.fromisoformat(row['transaction_time'])
                if timestamp.tzinfo is not None:
                    raise ValueError('Expected timestamp without timezone')
                yield [hashlib.sha256(f'{digest}:{number}'.encode()).hexdigest(), number, digest,
                       timestamp, *(row[name] for name in STRINGS), *floats,
                       population, int(row['target'])]
            except Exception as exc:
                raise ValueError(f'{path.name}: row {number}: {exc}') from exc


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--csv', required=True, type=Path)
    args = parser.parse_args()
    digest = fingerprint(args.csv)
    count = sum(1 for _ in records(args.csv, digest))
    if not count:
        raise ValueError('Empty CSV')
    connection = client()
    if connection.command('select count() from transactions_db.transactions'):
        raise ValueError('Source already contains data. Use run.py full --reset for a clean load.')
    batch = []
    for row in records(args.csv, digest):
        batch.append(row)
        if len(batch) == 5000:
            connection.insert('transactions_db.transactions', batch, column_names=COLUMNS)
            batch = []
    if batch:
        connection.insert('transactions_db.transactions', batch, column_names=COLUMNS)
    if fingerprint(args.csv) != digest:
        raise ValueError('CSV changed while loading; reset and retry')
    actual = connection.command('select count() from transactions_db.transactions')
    if actual != count:
        raise ValueError(f'Row count mismatch: {actual} != {count}')
    print(f'Loaded {count:,} labeled transactions; sha256={digest}', flush=True)


if __name__ == '__main__':
    main()
