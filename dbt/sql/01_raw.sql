CREATE DATABASE IF NOT EXISTS transactions_db;
CREATE TABLE IF NOT EXISTS transactions_db.transactions
(
    transaction_id String,
    source_row UInt64,
    dataset_id String,
    transaction_time DateTime,
    merch String,
    cat_id String,
    amount Float64,
    name_1 String,
    name_2 String,
    gender String,
    street String,
    one_city String,
    us_state String,
    post_code String,
    lat Float64,
    lon Float64,
    population_city UInt64,
    jobs String,
    merchant_lat Float64,
    merchant_lon Float64,
    target UInt8
)
ENGINE = MergeTree
ORDER BY (us_state, transaction_time, transaction_id);
