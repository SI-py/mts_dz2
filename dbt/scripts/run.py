#!/usr/bin/env python3
"""Запуск проекта и отдельных команд через Docker Compose."""
import argparse
import hashlib
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def run(command, env, log=None):
    print('\n> ' + ' '.join(command), flush=True)
    if log is None:
        subprocess.run(command, cwd=ROOT, env=env, check=True)
        return
    with log.open('w') as stream:
        process = subprocess.Popen(command, cwd=ROOT, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True)
        for line in process.stdout:
            print(line, end='', flush=True)
            stream.write(re.sub(r"\x1b\[[0-?]*[ -/]*[@-~]", "", line).rstrip() + "\n")
        code = process.wait()
        if code:
            raise subprocess.CalledProcessError(code, command)


def main():
    parser = argparse.ArgumentParser(description='ClickHouse + dbt: raw -> staging -> 6 marts')
    parser.add_argument('command', choices=['smoke', 'full', 'build', 'test', 'docs', 'lint', 'stop'])
    parser.add_argument('--reset', action='store_true', help='Delete this project data before smoke/full')
    args = parser.parse_args()
    env = os.environ.copy()
    if args.command == 'smoke':
        env['COMPOSE_PROJECT_NAME'] = 'mts-dbt-smoke'
        env['CLICKHOUSE_HTTP_PORT'] = '18125'
        env['DOCS_PORT'] = '18081'
        env['DBT_ARTIFACTS_DIR'] = 'target_smoke'
    else:
        env['COMPOSE_PROJECT_NAME'] = 'mts-dbt'
        env['DBT_ARTIFACTS_DIR'] = 'target'
    result_dir = ROOT / 'results'
    result_dir.mkdir(exist_ok=True)
    def compose(*parts, log=None):
        run(['docker', 'compose', *parts], env, log)
    def dbt(*parts, log=None):
        compose('run', '--rm', 'dbt', *parts, log=log)
    def python(script, *parts):
        compose('run', '--rm', '--entrypoint', 'python', 'dbt', f'/app/scripts/{script}', *parts)
    try:
        if args.reset and args.command not in ('smoke', 'full'):
            parser.error('--reset is only supported for smoke/full')
        if args.command == 'stop':
            compose('down')
            return 0
        compose('config', '--quiet')
        if args.reset:
            compose('down', '-v')
        compose('build', 'dbt')
        compose('up', '-d', '--wait', '--wait-timeout', '180', 'clickhouse')
        if args.command in ('smoke', 'full'):
            path = 'demo.csv' if args.command == 'smoke' else 'train.csv'
            if not (ROOT / 'data' / path).is_file():
                if args.command == 'full' and (ROOT.parent / 'data' / path).is_file():
                    input_path = f'/app/input/{path}'
                else:
                    raise ValueError(f'Put labeled {path} into data/ or ../data/')
            else:
                input_path = f'/app/data/{path}'
            python('load.py', '--csv', input_path)
        package_root = ROOT / 'dbt_packages'
        stamp = package_root / '.requirements.sha256'
        def package_hash():
            content = (ROOT / 'packages.yml').read_bytes()
            lock = ROOT / 'package-lock.yml'
            if lock.exists():
                content += lock.read_bytes()
            return hashlib.sha256(content).hexdigest()
        installed = all((package_root / name / 'dbt_project.yml').exists()
                        for name in ('dbt_utils', 'dbt_date', 'dbt_expectations'))
        if installed and stamp.exists() and stamp.read_text() == package_hash():
            print('Using verified package cache', flush=True)
        else:
            for attempt in range(3):
                try:
                    dbt('deps')
                    stamp.write_text(package_hash())
                    break
                except subprocess.CalledProcessError:
                    if attempt == 2:
                        raise
                    print('Retrying package download...', flush=True)
                    time.sleep(2)
        if args.command in ('smoke', 'full', 'build'):
            dbt('seed')
            dbt('run')
        if args.command in ('smoke', 'full', 'build', 'test'):
            prefix = 'smoke_' if args.command == 'smoke' else ''
            dbt('test', log=result_dir / f'{prefix}dbt_test.log')
            shutil.copyfile(ROOT / env['DBT_ARTIFACTS_DIR'] / 'run_results.json',
                            result_dir / f'{prefix}dbt_test_results.json')
        if args.command in ('smoke', 'full', 'build', 'lint'):
            compose('run', '--rm', '--workdir', '/app/dbt', '--entrypoint', 'sqlfluff', 'dbt',
                    'lint', 'models', 'tests', log=result_dir / ('smoke_lint.log' if args.command == 'smoke' else 'lint.log'))
        if args.command in ('smoke', 'full', 'build', 'docs'):
            dbt('docs', 'generate', '--static')
            compose('up', '-d', 'docs')
        if args.command in ('full', 'build'):
            python('export.py')
        print('\nDone. Docs: http://localhost:' + env.get('DOCS_PORT', '18080'))
        return 0
    except (subprocess.CalledProcessError, ValueError, OSError) as exc:
        print(f'\nError: {exc}', file=sys.stderr)
        print(f'Logs: docker compose -p {env["COMPOSE_PROJECT_NAME"]} logs --tail 100', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
