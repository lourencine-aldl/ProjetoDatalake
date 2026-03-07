
  
    

  create  table "datawarehouse"."public_public"."gold_vendas_resumo__dbt_tmp"
  
  
    as
  
  (
    with base as (
    select *
    from "datawarehouse"."public_public"."stg_silver_pcnfsaid"
)

select
    current_date as data_referencia,
    count(*) as qtd_linhas_pcnfsaid
from base
  );
  