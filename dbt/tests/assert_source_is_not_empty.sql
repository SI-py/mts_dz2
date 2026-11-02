select 'empty_source' as failure
where (select count() from {{ source('raw', 'transactions') }}) = 0
