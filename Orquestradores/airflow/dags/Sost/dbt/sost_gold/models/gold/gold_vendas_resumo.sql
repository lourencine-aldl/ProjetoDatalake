with base as (
    select *
    from {{ ref('stg_silver_pcnfsaid') }}
)

select
    current_date as data_referencia,
    count(*) as qtd_linhas_pcnfsaid
from base
