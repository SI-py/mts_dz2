#!/usr/bin/env python3
"""Portable Docker scenarios. Host dependencies: Python 3.9+ and Docker only."""
import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(args, env, capture=False, check=True):
    command = ['docker', 'compose', *args]
    print('\n→ ' + ' '.join(command), flush=True)
    return subprocess.run(command, cwd=ROOT, env=env, check=check,
                          text=True, capture_output=capture)


def main():
    parser = argparse.ArgumentParser(description='CSV → Kafka → ClickHouse: запуск и проверка')
    parser.add_argument('scenario', choices=['smoke', 'full', 'dashboard'])
    parser.add_argument('--reset', action='store_true',
                        help='Удалить volumes выбранного стенда перед запуском (все его данные)')
    args = parser.parse_args()
    if not shutil.which('docker'):
        parser.error('Docker не найден. Установите и запустите Docker Desktop.')
    if args.scenario == 'dashboard' and args.reset:
        parser.error('--reset поддерживается только для smoke/full')
    env = os.environ.copy()
    if args.scenario == 'smoke':
        env['COMPOSE_PROJECT_NAME'] = env.get('SMOKE_PROJECT_NAME', 'mts-dz2-smoke')
        env['CLICKHOUSE_HTTP_PORT'] = env.get('SMOKE_HTTP_PORT', '18123')
    else:
        env['COMPOSE_PROJECT_NAME'] = 'mts-dz2'
    if args.scenario == 'full':
        for name in ('train.csv', 'test.csv'):
            if not (ROOT / 'data' / name).is_file():
                parser.error(f'Сначала скачайте {name} из Kaggle и поместите в data/')
    try:
        run(['config', '--quiet'], env)
        if args.reset:
            print(f'Удаление данных стенда {env["COMPOSE_PROJECT_NAME"]}', flush=True)
            run(['down', '-v'], env)
        run(['build', 'cli'], env)
        run(['up', '-d', '--wait', '--wait-timeout', '240', 'kafka', 'clickhouse'], env)
        def cli(*options, **kwargs):
            return run(['run', '--rm', 'cli', *options], env, **kwargs)
        if args.scenario == 'smoke':
            cli('load', '--csv', 'data/demo.csv')
            cli('verify', '--csv', 'data/demo.csv')
            cli('report', '--output', 'results/demo_max_transaction_by_state.csv')
            cli('benchmark', '--runs', '3', '--output', 'results/demo_benchmark.json')
            cli('dashboard', '--output', 'results/demo_dashboard.html')
            duplicate = cli('load', '--csv', 'data/demo.csv', capture=True, check=False)
            if duplicate.returncode == 0 or 'This CSV is already present' not in duplicate.stdout + duplicate.stderr:
                raise RuntimeError('Повторный CSV не был отклонён по ожидаемой причине:\n'
                                   + duplicate.stdout + duplicate.stderr)
            run(['restart', 'kafka', 'clickhouse'], env)
            run(['up', '-d', '--wait', '--wait-timeout', '240', 'kafka', 'clickhouse'], env)
            cli('verify', '--csv', 'data/demo.csv')
            print('\n✓ Smoke пройден: доставка, SQL, дубли, HTML и сохранность после рестарта.')
            print(f'Остановить demo: docker compose -p {env["COMPOSE_PROJECT_NAME"]} down')
        elif args.scenario == 'full':
            for name in ('train.csv', 'test.csv'):
                cli('load', '--csv', f'data/{name}', '--timeout', '600')
            cli('verify', '--csv', 'data/train.csv', '--csv', 'data/test.csv')
            cli('report', '--output', 'results/max_transaction_by_state.csv')
            cli('report', '--csv', 'data/test.csv', '--output', 'results/max_transaction_by_state_test.csv')
            cli('benchmark', '--runs', '5', '--output', 'results/benchmark.json')
            cli('dashboard', '--output', 'results/dashboard.html')
            cli('status')
        else:
            cli('dashboard', '--output', 'results/dashboard.html')
        print('\nОтчёты: ' + str(ROOT / 'results'))
    except (subprocess.CalledProcessError, RuntimeError, OSError) as exc:
        print(f'\nОшибка: {exc}', file=sys.stderr)
        print(f'Логи: docker compose -p {env["COMPOSE_PROJECT_NAME"]} logs --tail 100', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
