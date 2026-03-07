
  create view "datawarehouse"."public_public"."stg_silver_pcnfsaid__dbt_tmp"
    
    
  as (
    select *
from "datawarehouse"."public"."silver_pcnfsaid"
  );