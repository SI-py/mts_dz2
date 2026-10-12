CREATE DATABASE IF NOT EXISTS homework;

-- Baseline: the same logical data, plain strings, no useful sorting key.
CREATE TABLE IF NOT EXISTS homework.transactions_baseline
(
    trans_num String,
    trans_time DateTime,
    state String,
    category String,
    amt Decimal(18, 2),
    dataset_id String,
    source_row UInt64,
    raw_data String
)
ENGINE = MergeTree
ORDER BY tuple();

-- Final, optimized DDL: money stays exact; all CSV fields survive in raw_data.
CREATE TABLE IF NOT EXISTS homework.transactions
(
    trans_num String CODEC(ZSTD(3)),
    trans_time DateTime CODEC(Delta, ZSTD(3)),
    state LowCardinality(String),
    category LowCardinality(String),
    amt Decimal(18, 2) CODEC(ZSTD(3)),
    dataset_id LowCardinality(String),
    source_row UInt64 CODEC(Delta, ZSTD(3)),
    raw_data String CODEC(ZSTD(3))
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(trans_time)
ORDER BY (state, trans_time, trans_num);

-- Only an aggregate state per state/insert block: merge it at SELECT time.
CREATE TABLE IF NOT EXISTS homework.max_by_state
(
    state LowCardinality(String),
    winner AggregateFunction(argMax,
        Tuple(String, Decimal(18, 2), String),
        Tuple(Decimal(18, 2), String))
)
ENGINE = AggregatingMergeTree
ORDER BY state;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.transactions_to_baseline
TO homework.transactions_baseline AS
SELECT trans_num, trans_time, state, category, amt, dataset_id, source_row, raw_data
FROM homework.transactions;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.transactions_to_max
TO homework.max_by_state AS
SELECT state,
       argMaxState(tuple(toString(category), amt, trans_num), tuple(amt, trans_num)) AS winner
FROM homework.transactions
GROUP BY state;

-- Create downstream views before enabling consumption from Kafka.
CREATE TABLE IF NOT EXISTS homework.transactions_kafka
(
    trans_num String,
    trans_time DateTime,
    state String,
    category String,
    amt Decimal(18, 2),
    dataset_id String,
    source_row UInt64,
    raw_data String
)
ENGINE = Kafka
SETTINGS kafka_broker_list = 'kafka:9092',
         kafka_topic_list = 'transactions',
         kafka_group_name = 'clickhouse-homework-v1',
         kafka_format = 'JSONEachRow',
         kafka_num_consumers = 3,
         kafka_thread_per_consumer = 1,
         kafka_skip_broken_messages = 0,
         kafka_flush_interval_ms = 1000;

CREATE MATERIALIZED VIEW IF NOT EXISTS homework.kafka_to_transactions
TO homework.transactions AS
SELECT trans_num, trans_time, state, category, amt, dataset_id, source_row, raw_data
FROM homework.transactions_kafka;
