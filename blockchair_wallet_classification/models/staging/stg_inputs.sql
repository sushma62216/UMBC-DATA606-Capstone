{{ config(materialized="view") }}

select
    transaction_hash,
    input_address as address,
    input_value_ltc,
    null::date as tx_date -- date will come from transaction table in join
from {{ source("raw", "inputs") }}
