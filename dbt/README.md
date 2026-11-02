# ДЗ4. Аналитика транзакций в dbt

## Запуск

Нужны Docker Compose v2.20+ и Python 3.9+. Команды выполняются из каталога `dbt/`.

```bash
python3 scripts/run.py smoke --reset
```

Для полного набора положите размеченный [train.csv](https://www.kaggle.com/t/1918f3f6435300327d38d6c596f97394) в `../data/` (каталог предыдущей домашки) или локальный `data/`:

```bash
python3 scripts/run.py full --reset
```

`--reset` удаляет данные выбранного стенда. Docs: [18080](http://localhost:18080), для demo - [18081](http://localhost:18081).
Остановка: `make stop`; demo: `docker compose -p mts-dbt-smoke down`.

## Проект

`transactions_db.transactions` -> `stg_transactions` -> [6 витрин](models/marts).
Метрики по датам, штатам, категориям, клиентам, часам и мерчантам.
Пакеты: dbt_utils, dbt_date, dbt_expectations. Макросы: amount_bucket, risk_level.
Клиент определяется по имени и адресу; fraud_rate считается в процентах.
Проверки: `make test`, `make lint`. Документация: `make docs`.

## Результаты

786 431 транзакция, 4 504 фрода. **111 тестов прошли**: 104 data tests и 7 unit tests.
SQLFluff и pre-commit прошли. Версии: dbt-core 1.12.6, dbt-clickhouse 1.10.3, ClickHouse 26.8.36.3.
[Лог тестов](results/dbt_test.log), [DAG](results/dbt_docs.png), [итоги и размеры витрин](results/summary.json).
