{{ config(materialized="view") }}

select
    address,
    balance_ltc
from {{ source("raw", "addresses") }}