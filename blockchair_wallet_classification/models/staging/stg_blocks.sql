{{ config(materialized="view") }}

select
    block_id,
    time::date as block_date,
    size,
    transaction_count,
    reward_ltc,
    guessed_miner
from {{ source("raw", "blocks") }}
