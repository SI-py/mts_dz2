select 'row_count_mismatch' as failure
where
    (select count() from {{ source('raw', 'transactions') }})
    != (select count() from {{ ref('stg_transactions') }})
