-- Camada Silver a partir da Bronze 006 (Dremio)
-- Host Dremio: http://76.13.232.59:9047/
-- Objetivo: padronizar, tipar, limpar texto e deduplicar

with bronze as (
    select *
    from Minio."datalake-sost".bronze."006_bronze_tab_faturamento_full"
),

silver_treated as (
    select
        -- Chaves / datas
        cast(codfilial as integer) as codfilial,
        cast(data_faturamento as timestamp) as data_faturamento,
        cast(numnota as bigint) as numnota,
        cast(codigo as bigint) as codigo,
        cast(codcli as integer) as codcli,

        -- Tempo
        cast(mes as integer) as mes,
        cast(ano as integer) as ano,
        cast(horafat as integer) as horafat,
        cast(minutofat as integer) as minutofat,

        -- Dimensões numéricas
        cast(codativprinc as decimal(18,4)) as codativprinc,
        cast(codfornec as bigint) as codfornec,
        cast(codsupervisor as integer) as codsupervisor,
        cast(codusur as integer) as codusur,
        cast(codsec as integer) as codsec,
        cast(codcategoria as bigint) as codcategoria,
        cast(codsubcategoria as bigint) as codsubcategoria,
        cast(codicmtab as decimal(18,6)) as codicmtab,
        cast(codativ as decimal(18,4)) as codativ,
        cast(codplpag as decimal(38,0)) as codplpag,
        cast(numped as decimal(38,0)) as numped,
        cast(numnotadev as bigint) as numnotadev,

        -- Métricas
        cast(qtd as decimal(18,6)) as qtd,
        cast(qtdevol as decimal(18,6)) as qtdevol,
        cast(peso as decimal(18,6)) as peso,
        cast(pesoliq as decimal(18,6)) as pesoliq,
        cast(valortotal as decimal(18,6)) as valortotal,
        cast(ptabela as decimal(18,6)) as ptabela,
        cast(valor_liquido as decimal(18,6)) as valor_liquido,
        cast(custofinest as decimal(18,6)) as custofinest,
        cast(custoreal as decimal(18,6)) as custoreal,
        cast(perc_lucro as decimal(18,6)) as perc_lucro,
        cast(valorbonificado as decimal(18,6)) as valorbonificado,
        cast(vldevolucao as decimal(18,6)) as vldevolucao,
        cast(percdescdsv as decimal(18,6)) as percdescdsv,
        cast(valor_dsv as decimal(18,6)) as valor_dsv,
        cast(valor_dsv_2 as decimal(18,6)) as valor_dsv_2,
        cast(percdescfin as decimal(18,6)) as percdescfin,

        -- Textos limpos
        nullif(trim(cast(cliente as varchar)), '') as cliente,
        nullif(trim(cast(ramo as varchar)), '') as ramo,
        nullif(trim(cast(ramoprincipal as varchar)), '') as ramoprincipal,
        nullif(trim(cast(tiporamo as varchar)), '') as tiporamo,
        nullif(trim(cast(tipovenda as varchar)), '') as tipovenda,
        nullif(trim(cast(produto as varchar)), '') as produto,
        nullif(trim(cast(embalagem as varchar)), '') as embalagem,
        nullif(trim(cast(litragem as varchar)), '') as litragem,
        nullif(trim(cast(fornecedor as varchar)), '') as fornecedor,
        nullif(trim(cast(supervisor as varchar)), '') as supervisor,
        nullif(trim(cast(nomerca as varchar)), '') as nomerca,
        nullif(trim(cast(secao as varchar)), '') as secao,
        nullif(trim(cast(contrato as varchar)), '') as contrato,
        nullif(trim(cast(categoria as varchar)), '') as categoria,
        nullif(trim(cast(subcategoria as varchar)), '') as subcategoria,

        -- Metadados
        cast(
            replace(
                substr(cast(_ingestion_ts_utc as varchar), 1, 19),
                'T',
                ' '
            ) as timestamp
        ) as _ingestion_ts_utc,
        cast(_ingestion_date as date) as _ingestion_date,
        current_timestamp as _silver_ts_utc

    from bronze
    where codfilial is not null
      and data_faturamento is not null
      and numnota is not null
      and codigo is not null
      and codcli is not null
),

silver as (
    select distinct *
    from silver_treated
)

select *
from silver;
