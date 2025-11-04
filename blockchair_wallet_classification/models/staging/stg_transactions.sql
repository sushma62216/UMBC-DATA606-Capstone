{{ config(materialized="view") }}

select
    transaction_hash,
    block_id,
    time::date as tx_date,
    input_total_ltc,
    output_total_ltc,
    fee_ltc
from {{ source("raw", "transactions") }}
