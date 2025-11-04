{{ config(materialized="view") }}

select
    transaction_hash,
    output_address as address,
    output_value_ltc,
    null::date as tx_date
from {{ source("raw", "outputs") }}
