"""Friendly Click CLI; all data writes go through Kafka, never direct INSERT."""
import functools
import json
import os
import statistics
import time
from pathlib import Path
from uuid import uuid4

import click
from rich.console import Console
from rich.table import Table

from .data import fingerprint, inspect_csv, oracle, write_report
from .db import SQL_DIR, connect, query_text
from .producer import publish
from .dashboard import create_dashboard

console = Console()
CSV_PATH = click.Path(exists=True, dir_okay=False, path_type=Path)


def errors(function):
    @functools.wraps(function)
    def wrapped(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except click.ClickException:
            raise
        except Exception as exc:
            raise click.ClickException(str(exc)) from exc
    return wrapped


@click.group()
def cli():
    """Домашнее задание №2: CSV → Kafka → ClickHouse."""


@cli.command()
@errors
def status():
    """Check ClickHouse and show rows/bytes for all storage tables."""
    client = connect()
    console.print(f'ClickHouse {client.command("SELECT version()")}, database homework')
    rows = client.query("""
        SELECT table, sum(rows), sum(bytes_on_disk)
        FROM system.parts WHERE active AND database = 'homework'
        GROUP BY table ORDER BY table
    """).result_rows
    table = Table('Table', 'Physical rows', 'Disk bytes')
    for name, count, size in rows:
        table.add_row(name, f'{count:,}', f'{size:,}')
    console.print(table)


@cli.command()
@click.option('--csv', 'path', required=True, type=CSV_PATH)
@click.option('--timeout', default=300, type=click.IntRange(min=1), show_default=True)
@errors
def load(path, timeout):
    """Validate, publish and wait for EVERY source row in both storage tables."""
    digest, expected, _ = inspect_csv(path)
    console.print(f'Validated {expected:,} rows • sha256={digest}')
    client = connect()
    already = client.command(
        'SELECT count() FROM homework.transactions WHERE dataset_id = {digest:String}',
        parameters={'digest': digest},
    )
    if already:
        raise click.ClickException('This CSV is already present (possibly partially). '
                                   'For a clean retry: docker compose down -v, then up again.')
    publish(path, digest, expected, os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'), 'transactions')
    deadline = time.monotonic() + timeout
    while True:
        counts = []
        for name in ('transactions', 'transactions_baseline'):
            counts.append(client.query(f'''
                SELECT count(), uniqExact(source_row) FROM homework.{name}
                WHERE dataset_id = {{digest:String}}
            ''', parameters={'digest': digest}).first_row)
        if any(total > expected for total, unique in counts):
            raise RuntimeError('Duplicate delivery detected; reset the stack before retrying.')
        if all(total == unique == expected for total, unique in counts):
            break
        if time.monotonic() > deadline:
            raise RuntimeError(f'Ingestion timeout: expected={expected}, actual={counts}. '
                               'Check ClickHouse logs before retrying.')
        time.sleep(1)
    console.print(f'[green]Loaded and counted: {expected:,} rows in both tables.[/green]')


@cli.command()
@click.option('--output', default='results/max_transaction_by_state.csv', type=click.Path(path_type=Path))
@click.option('--mode', type=click.Choice(['aggregate', 'raw', 'baseline']), default='aggregate')
@click.option('--csv', 'path', type=CSV_PATH, help='Report for one CSV only, using a filtered raw SQL query.')
@errors
def report(output, mode, path):
    """Execute the separate SQL file and export its real result as CSV."""
    client = connect()
    if path:
        rows = client.query((SQL_DIR / '05_max_transaction_for_dataset.sql').read_text(),
                            parameters={'dataset_id': fingerprint(path)}).result_rows
    else:
        rows = client.query(query_text(mode)).result_rows
    if not rows:
        raise click.ClickException('No transactions. Run load first.')
    write_report(output, rows)
    table = Table('State', 'Category', 'Maximum amount', 'Transaction ID')
    for state, category, amount, trans_num in rows:
        table.add_row(state, category, f'{amount:.2f}', trans_num[:16] + '…')
    console.print(table)
    console.print(f'[green]SQL result → {output} ({len(rows)} states)[/green]')


@cli.command()
@click.option('--csv', 'paths', multiple=True, required=True, type=CSV_PATH)
@errors
def verify(paths):
    """Compare all three SQL queries against Python and check row completeness."""
    manifest, expected = oracle(paths)
    if len({item['sha256'] for item in manifest}) != len(manifest):
        raise click.ClickException('The same dataset was specified twice.')
    client = connect()
    for name in ('transactions', 'transactions_baseline'):
        actual = client.query(f'''
            SELECT dataset_id, count(), uniqExact(source_row), min(source_row), max(source_row)
            FROM homework.{name} GROUP BY dataset_id ORDER BY dataset_id
        ''').result_rows
        wanted = sorted((m['sha256'], m['rows'], m['rows'], 1, m['rows']) for m in manifest)
        if actual != wanted:
            raise RuntimeError(f'{name}: row counts or dataset list differ: {actual} != {wanted}')
    for mode in ('baseline', 'raw', 'aggregate'):
        actual = client.query(query_text(mode)).result_rows
        if actual != expected:
            raise RuntimeError(f'{mode}: SQL differs from Python oracle')
        console.print(f'[green]✓ {mode}: {len(actual)} states, exact amounts and IDs[/green]')
    console.print('[green]✓ Dataset hashes, row counts and source row uniqueness match[/green]')


@cli.command()
@click.option('--csv', 'paths', multiple=True, required=True, type=CSV_PATH)
@click.option('--output', default='results/reference.csv', type=click.Path(path_type=Path))
@errors
def reference(paths, output):
    """Compute a CSV-only reference (does not claim a ClickHouse execution)."""
    manifest, rows = oracle(paths)
    write_report(output, rows)
    output.with_suffix('.manifest.json').write_text(json.dumps({
        'method': 'Python Decimal oracle; not a ClickHouse query result',
        'inputs': manifest, 'states': len(rows),
    }, ensure_ascii=False, indent=2) + '\n')
    console.print(f'Reference: {output}, {len(rows)} states')


@cli.command()
@click.option('--runs', default=5, type=click.IntRange(1, 30), show_default=True)
@click.option('--output', default='results/benchmark.json', type=click.Path(path_type=Path))
@errors
def benchmark(runs, output):
    """Measure query-log read rows/bytes/time and physical storage, cache disabled."""
    client = connect()
    if not client.command('SELECT count() FROM homework.transactions'):
        raise click.ClickException('Load data before benchmarking.')
    measured = {}
    expected = client.query(query_text('baseline')).result_rows
    for mode in ('baseline', 'raw', 'aggregate'):
        client.query(query_text(mode), settings={'use_query_cache': 0})  # warm-up
        samples = []
        for _ in range(runs):
            query_id = str(uuid4())
            started = time.perf_counter()
            result = client.query(query_text(mode), settings={'use_query_cache': 0, 'query_id': query_id})
            elapsed = (time.perf_counter() - started) * 1000
            if result.result_rows != expected:
                raise RuntimeError(f'{mode}: benchmark results differ from baseline')
            client.command('SYSTEM FLUSH LOGS')
            stats = client.query('''
                SELECT query_duration_ms, read_rows, read_bytes, memory_usage
                FROM system.query_log
                WHERE query_id = {qid:String} AND type = 'QueryFinish'
                ORDER BY event_time_microseconds DESC LIMIT 1
            ''', parameters={'qid': query_id}).result_rows
            if not stats:
                raise RuntimeError('Missing query_log entry; ensure query logging is enabled.')
            duration, read_rows, read_bytes, memory = stats[0]
            samples.append({'client_ms': round(elapsed, 3), 'server_ms': duration,
                            'read_rows': read_rows, 'read_bytes': read_bytes, 'memory_bytes': memory})
        measured[mode] = {'median_client_ms': round(statistics.median(x['client_ms'] for x in samples), 3),
                          'samples': samples}
    storage = client.query('''
        SELECT table, sum(rows), sum(data_compressed_bytes), sum(data_uncompressed_bytes), sum(bytes_on_disk)
        FROM system.parts WHERE active AND database = 'homework'
        GROUP BY table ORDER BY table
    ''').result_rows
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps({'clickhouse_version': client.command('SELECT version()'),
        'runs': runs, 'query_cache': False, 'warmup': True, 'queries': measured,
        'storage_columns': ['table', 'physical_rows', 'compressed_bytes', 'uncompressed_bytes', 'disk_bytes'],
        'storage': storage}, indent=2) + '\n')
    console.print_json(data=measured)
    console.print(f'Benchmark → {output}')


@cli.command()
@click.option('--output', default='results/dashboard.html', type=click.Path(path_type=Path))
@errors
def dashboard(output):
    """Export an interactive standalone HTML report from real ClickHouse SQL."""
    states = create_dashboard(connect(), output)
    console.print(f'[green]Transaction Atlas → {output} ({states} states)[/green]')


if __name__ == '__main__':
    cli()
