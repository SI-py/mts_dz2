"""Streaming validation and normalization, independent from Kafka/ClickHouse."""
import csv
import hashlib
import json
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path

ALIASES = {
    'trans_time': ('transaction_time', 'trans_date_trans_time'),
    'state': ('us_state', 'state'),
    'category': ('cat_id', 'category'),
    'amt': ('amount', 'amt'),
}


def fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b''):
            digest.update(chunk)
    return digest.hexdigest()


def records(path: Path, dataset_id: str):
    """Keep all source columns as strings; never round money through float."""
    with path.open(encoding='utf-8-sig', newline='') as stream:
        reader = csv.DictReader(stream)
        headers = reader.fieldnames or []
        if len(set(headers)) != len(headers):
            raise ValueError('Duplicate CSV column names')
        mapping = {}
        for name, aliases in ALIASES.items():
            found = [a for a in aliases if a in headers]
            if len(found) != 1:
                raise ValueError(f'Expected exactly one column from {aliases}, got {found}')
            mapping[name] = found[0]
        for number, row in enumerate(reader, 1):
            try:
                if None in row or any(v is None for v in row.values()):
                    raise ValueError('CSV row has wrong number of columns')
                amount = Decimal(row[mapping['amt']])
                if not amount.is_finite() or amount < 0 or amount >= Decimal('10000000000000000'):
                    raise ValueError('amount outside Decimal(18,2), or negative/non-finite')
                if amount != amount.quantize(Decimal('0.01')):
                    raise ValueError('amount has more than 2 decimal places')
                state = row[mapping['state']].strip().upper()
                if not re.fullmatch('[A-Z]{2}', state):
                    raise ValueError('state must contain two letters')
                category = row[mapping['category']].strip()
                if not category:
                    raise ValueError('empty category')
                dt = datetime.fromisoformat(row[mapping['trans_time']].strip())
                if dt.tzinfo is not None or dt.microsecond or not (1970 <= dt.year <= 2105):
                    raise ValueError('expected naive timestamp, seconds precision, 1970..2105')
                raw = json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(',', ':'))
                trans_num = row.get('trans_num', '').strip() or hashlib.sha256(raw.encode()).hexdigest()
                yield {
                    'trans_num': trans_num,
                    'trans_time': dt.strftime('%Y-%m-%d %H:%M:%S'),
                    'state': state,
                    'category': category,
                    'amt': format(amount, '.2f'),
                    'dataset_id': dataset_id,
                    'source_row': number,
                    'raw_data': raw,
                }
            except (ValueError, InvalidOperation, TypeError) as exc:
                raise ValueError(f'{path.name}: data row {number}: {exc}') from exc


def inspect_csv(path: Path):
    dataset_id = fingerprint(path)
    count = 0
    winners = {}
    for record in records(path, dataset_id):
        count += 1
        state = record['state']
        candidate = (Decimal(record['amt']), record['trans_num'])
        current = winners.get(state)
        if current is None or candidate > (Decimal(current['amt']), current['trans_num']):
            winners[state] = record
    if not count:
        raise ValueError(f'{path}: empty dataset')
    if fingerprint(path) != dataset_id:
        raise ValueError('CSV changed during validation')
    return dataset_id, count, winners


def oracle(paths):
    """An independent Python maximum, used to check the SQL report."""
    winners = {}
    manifest = []
    for path in paths:
        digest, count, local = inspect_csv(path)
        manifest.append({'file': path.name, 'sha256': digest, 'rows': count})
        for state, record in local.items():
            current = winners.get(state)
            if current is None or (Decimal(record['amt']), record['trans_num']) > (
                Decimal(current['amt']), current['trans_num']
            ):
                winners[state] = record
    result = [(s, r['category'], Decimal(r['amt']), r['trans_num']) for s, r in sorted(winners.items())]
    return manifest, result


def write_report(path: Path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.writer(stream)
        writer.writerow(['state', 'category', 'max_amount', 'trans_num'])
        writer.writerows((s, c, format(Decimal(a), '.2f'), t) for s, c, a, t in rows)
