"""Standalone producer: python -m src.producer --csv data/test.csv."""
import json
import os
import time
from pathlib import Path

import click
from confluent_kafka import Producer

from .data import fingerprint, inspect_csv, records


def publish(path: Path, dataset_id: str, expected_count: int, brokers: str, topic: str):
    producer = Producer({
        'bootstrap.servers': brokers,
        'enable.idempotence': True,
        'acks': 'all',
        'compression.type': 'lz4',
        'linger.ms': 20,
        'delivery.timeout.ms': 120000,
    })
    delivered = 0
    failures = []

    def callback(error, message):
        nonlocal delivered
        if error:
            failures.append(str(error))
        else:
            delivered += 1

    sent = 0
    with click.progressbar(records(path, dataset_id), length=expected_count, label='CSV → Kafka') as bar:
        for record in bar:
            payload = json.dumps(record, ensure_ascii=False).encode('utf-8')
            deadline = time.monotonic() + 120
            while True:
                if failures:
                    raise RuntimeError(f'Kafka delivery failed: {failures[0]}')
                try:
                    producer.produce(topic, key=record['state'].encode(), value=payload, on_delivery=callback)
                    break
                except BufferError:
                    producer.poll(0.1)
                    if time.monotonic() > deadline:
                        raise RuntimeError('Kafka producer queue stayed full for 120 seconds')
            sent += 1
            producer.poll(0)
    remaining = producer.flush(120)
    if remaining or failures or delivered != sent:
        raise RuntimeError(f'Incomplete Kafka delivery: sent={sent}, delivered={delivered}, '
                           f'pending={remaining}, errors={failures[:3]}')
    if sent != expected_count or fingerprint(path) != dataset_id:
        raise RuntimeError('CSV changed while producing. Reset the stack before retrying.')
    return delivered


@click.command()
@click.option('--csv', 'path', required=True, type=click.Path(exists=True, dir_okay=False, path_type=Path))
@click.option('--brokers', default=lambda: os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092'), show_default=True)
@click.option('--topic', default='transactions', show_default=True)
def main(path, brokers, topic):
    """Validate the entire CSV before sending any messages to Kafka."""
    try:
        digest, count, _ = inspect_csv(path)
        click.echo(f'Validated {count:,} rows, sha256={digest}')
        click.echo(f'Delivered: {publish(path, digest, count, brokers, topic):,}')
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc


if __name__ == '__main__':
    main()
