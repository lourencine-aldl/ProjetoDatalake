-- Camada Silver a partir da Bronze 001 (Dremio)
-- Host Dremio: http://76.13.232.59:9047/
-- Objetivo: padronizar, tipar e deduplicar registros da Bronze

with bronze as (
    select *
    from Minio."datalake-sost".bronze."001_bronze_pcsupervisor"
),

silver_treated as (
    select
        -- Datas
        cast(dtadmissao as timestamp) as dtadmissao,
        cast(dtdemissao as timestamp) as dtdemissao,

        -- Numéricos decimais
        cast(percomissao as double) as percomissao,
        cast(vlcorrente as double) as vlcorrente,
        cast(cod_cadrca as double) as cod_cadrca,

        -- Numéricos inteiros
        cast(percpartvendaprev as integer) as percpartvendaprev,
        cast(percmargemprev as integer) as percmargemprev,
        cast(codcoordenador as integer) as codcoordenador,
        cast(vllimcred as bigint) as vllimcred,
        cast(dt_vmais as bigint) as dt_vmais,

        -- Metadados de ingestão
        cast(_ingestion_ts_utc as varchar) as _ingestion_ts_utc,
        cast(_ingestion_date as date) as _ingestion_date
    from bronze
),

silver as (
    select distinct *
    from silver_treated
)

select *
from silver;
