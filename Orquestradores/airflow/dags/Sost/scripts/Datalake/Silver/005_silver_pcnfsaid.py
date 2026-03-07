#!/usr/bin/env python3
"""
Silver Layer ETL (DuckDB):
PCNFSAID MinIO (Bronze Parquet) -> PostgreSQL DW (public.silver_pcnfsaid)

✅ DuckDB lê TODOS os parquets em: s3://<bucket>/bronze/005_bronze_pcnfsaid/**/*.parquet
✅ Normaliza MINIO_ENDPOINT do .env (aceita http://host:porta) para o formato que o DuckDB espera (host:porta)
✅ Lê múltiplos parquets com schemas divergentes (union_by_name=true)
✅ Evita erro de DECIMAL “apertado” do parquet convertendo colunas numéricas via VARCHAR -> DECIMAL(18,2)/(18,6)
✅ Transformações em SQL DuckDB (datas, ints, decimais pt-br e notação científica)
✅ UPSERT no Postgres (ON CONFLICT) por PK composta: (especie, serie, numnota, codfilial)
✅ Garante CREATE SCHEMA + CREATE TABLE antes do upsert

Requisitos:
  pip install duckdb psycopg2-binary python-dotenv
"""

from __future__ import annotations

import os
import logging
from dataclasses import dataclass
from typing import Iterable, List, Sequence, Tuple
from urllib.parse import urlparse

import duckdb
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv


# ===========================
# SETTINGS EDITÁVEIS
# ===========================
LOGGER_NAME = "silver_pcnfsaid_duckdb"

BRONZE_PREFIX = "bronze/005_bronze_pcnfsaid"  # pasta no MinIO (sem bucket)
TARGET_TABLE = "public.silver_pcnfsaid"       # tabela destino no Postgres
CHUNK_SIZE = 5000                             # batch upsert

PK_COLS = ["especie", "serie", "numnota", "codfilial"]


# ===========================
# LOGGING
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(LOGGER_NAME)


# ===========================
# DATACLASSES
# ===========================
@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class MinioConfig:
    endpoint: str           # no .env: pode ser http://209.50...:9000
    bucket: str
    access_key: str
    secret_key: str
    region: str = "us-east-1"


# ===========================
# CONFIG LOADERS
# ===========================
def load_postgres_config(env_path: str) -> PostgresConfig:
    load_dotenv(env_path)

    host = os.getenv("POSTGRES_DW_HOST", "").strip()
    port = int((os.getenv("POSTGRES_DW_PORT", "5432") or "5432").strip())
    database = os.getenv("POSTGRES_DW_DB", "").strip()
    user = os.getenv("POSTGRES_DW_USER", "").strip()
    password = os.getenv("POSTGRES_DW_PASSWORD", "").strip()

    if not (host and database and user and password):
        raise ValueError("Configuração PostgreSQL incompleta no .env")

    return PostgresConfig(host=host, port=port, database=database, user=user, password=password)


def load_minio_config(env_path: str) -> MinioConfig:
    load_dotenv(env_path)

    endpoint = os.getenv("MINIO_ENDPOINT", "").strip()
    bucket = os.getenv("MINIO_BUCKET_SOST", "").strip()
    access_key = os.getenv("MINIO_ROOT_USER", "").strip()
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "").strip()
    region = (os.getenv("MINIO_REGION", "us-east-1") or "us-east-1").strip()

    if not (endpoint and bucket and access_key and secret_key):
        raise ValueError("Configuração MinIO incompleta no .env")

    return MinioConfig(
        endpoint=endpoint,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )


# ===========================
# NORMALIZA ENDPOINT (CRÍTICO)
# ===========================
def normalize_minio_endpoint(endpoint: str) -> tuple[str, bool]:
    """
    DuckDB espera s3_endpoint como "host:porta" (SEM http://).
    Seu .env pode estar como "http://209.50.228.232:9000" (ok para s3fs),
    então normalizamos aqui.

    Retorna:
      (host_port, use_ssl)
    """
    ep = (endpoint or "").strip().rstrip("/")
    if ep.startswith("http://") or ep.startswith("https://"):
        u = urlparse(ep)
        host_port = u.netloc  # "209.50.228.232:9000"
        use_ssl = (u.scheme == "https")
        return host_port, use_ssl
    return ep, False


# ===========================
# CONNECTIONS
# ===========================
def pg_connection(cfg: PostgresConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
    )


def duckdb_connection(minio_cfg: MinioConfig) -> duckdb.DuckDBPyConnection:
    """
    Cria conexão DuckDB e configura acesso S3 (MinIO) via httpfs.
    """
    con = duckdb.connect(database=":memory:")

    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")

    host_port, use_ssl = normalize_minio_endpoint(minio_cfg.endpoint)

    # MinIO geralmente precisa path-style
    con.execute("SET s3_url_style='path';")
    con.execute(f"SET s3_endpoint='{host_port}';")
    con.execute(f"SET s3_use_ssl={'true' if use_ssl else 'false'};")
    con.execute(f"SET s3_access_key_id='{minio_cfg.access_key}';")
    con.execute(f"SET s3_secret_access_key='{minio_cfg.secret_key}';")
    con.execute(f"SET s3_region='{minio_cfg.region}';")
    con.execute("SET s3_session_token='';")

    return con


# ===========================
# DEST TABLE (DDL)
# ===========================
def ensure_schema_and_table(conn) -> None:
    """
    Garante schema e tabela antes de qualquer operação.
    """
    schema, _table = TARGET_TABLE.split(".", 1)

    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        -- PK composta
        especie           VARCHAR(10)   NOT NULL,
        serie             INTEGER       NOT NULL,
        numnota           BIGINT        NOT NULL,
        codfilial         INTEGER       NOT NULL,

        -- datas
        dtsaida           DATE,
        dtentrega         DATE,
        dtcancel          DATE,

        -- valores
        vltotal           NUMERIC(18,2),
        vltotger          NUMERIC(18,2),
        vltabela          NUMERIC(18,2),
        vldesconto        NUMERIC(18,2),
        tipovenda         VARCHAR(10),
        vlcustoreal       NUMERIC(18,2),
        vlcustofin        NUMERIC(18,2),
        vloutrasdesp      NUMERIC(18,2),
        totpeso           NUMERIC(18,6),
        totvolume         NUMERIC(18,6),
        vldevolucao       NUMERIC(18,2),

        -- códigos
        codcont           INTEGER,
        codfiscal         INTEGER,
        codcli            INTEGER,
        codusur           INTEGER,
        codpraca          INTEGER,
        numitens          INTEGER,
        codemitente       INTEGER,
        numcar            BIGINT,
        codcob            VARCHAR(20),
        numped            NUMERIC(38,0),
        codfornec         BIGINT,
        numtransvenda     BIGINT,
        codplpag          BIGINT,
        codfunccancel     BIGINT,
        numtab            BIGINT,
        numseq            BIGINT,
        codsupervisor     BIGINT,
        condvenda         BIGINT,

        -- metadados
        _ingestion_ts_utc TIMESTAMP,
        _ingestion_date   DATE,
        _silver_ts_utc    TIMESTAMP,
        created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (especie, serie, numnota, codfilial)
    );

    CREATE INDEX IF NOT EXISTS idx_silver_pcnfsaid_ingestion_date
        ON {TARGET_TABLE}(_ingestion_date);
    """

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (TARGET_TABLE,))
        exists = cur.fetchone()[0]
    if not exists:
        raise RuntimeError(f"Falha ao criar/encontrar a tabela {TARGET_TABLE}. Verifique database/schema do .env.")

    logger.info("✅ Tabela %s criada/verificada", TARGET_TABLE)


# ===========================
# DUCKDB TRANSFORM SQL (robusto a schema divergente)
# ===========================
def _build_duckdb_transform_sql(s3_glob: str) -> str:
    """
    Estratégia robusta para múltiplos parquets com schemas diferentes:
    - read_parquet(..., union_by_name=true)
    - cria colunas *_txt para numéricos antes de converter para DECIMAL grande
    """
    return f"""
    WITH src AS (
        SELECT * FROM read_parquet('{s3_glob}', union_by_name=true)
    ),
    src_txt AS (
        SELECT
            *,
            CAST(vltotger AS VARCHAR)    AS vltotger_txt,
            CAST(vltotal AS VARCHAR)     AS vltotal_txt,
            CAST(vltabela AS VARCHAR)    AS vltabela_txt,
            CAST(vldesconto AS VARCHAR)  AS vldesconto_txt,
            CAST(vlcustoreal AS VARCHAR) AS vlcustoreal_txt,
            CAST(vlcustofin AS VARCHAR)  AS vlcustofin_txt,
            CAST(vloutrasdesp AS VARCHAR) AS vloutrasdesp_txt,
            CAST(totpeso AS VARCHAR)     AS totpeso_txt,
            CAST(totvolume AS VARCHAR)   AS totvolume_txt,
            CAST(vldevolucao AS VARCHAR) AS vldevolucao_txt,
            CAST(numped AS VARCHAR)      AS numped_txt
        FROM src
    ),
    norm AS (
        SELECT
            -- PK
            NULLIF(TRIM(CAST(especie AS VARCHAR)), '') AS especie,
            TRY_CAST(serie AS BIGINT)      AS serie,
            TRY_CAST(numnota AS BIGINT)    AS numnota,
            TRY_CAST(codfilial AS BIGINT)  AS codfilial,

            -- datas
            COALESCE(
                TRY_STRPTIME(CAST(dtsaida AS VARCHAR), '%d/%m/%y')::DATE,
                TRY_STRPTIME(CAST(dtsaida AS VARCHAR), '%d/%m/%Y')::DATE,
                TRY_CAST(dtsaida AS DATE)
            ) AS dtsaida,
            COALESCE(
                TRY_STRPTIME(CAST(dtentrega AS VARCHAR), '%d/%m/%y')::DATE,
                TRY_STRPTIME(CAST(dtentrega AS VARCHAR), '%d/%m/%Y')::DATE,
                TRY_CAST(dtentrega AS DATE)
            ) AS dtentrega,
            COALESCE(
                TRY_STRPTIME(CAST(dtcancel AS VARCHAR), '%d/%m/%y')::DATE,
                TRY_STRPTIME(CAST(dtcancel AS VARCHAR), '%d/%m/%Y')::DATE,
                TRY_CAST(dtcancel AS DATE)
            ) AS dtcancel,

            -- decimais (via *_txt)
            TRY_CAST(REPLACE(REPLACE(TRIM(vltotal_txt), '.', ''), ',', '.') AS DECIMAL(18,2)) AS vltotal,
            TRY_CAST(REPLACE(REPLACE(TRIM(vltotger_txt), '.', ''), ',', '.') AS DECIMAL(18,2)) AS vltotger,
            TRY_CAST(REPLACE(REPLACE(TRIM(vltabela_txt), '.', ''), ',', '.') AS DECIMAL(18,2)) AS vltabela,
            TRY_CAST(REPLACE(REPLACE(TRIM(vldesconto_txt), '.', ''), ',', '.') AS DECIMAL(18,2)) AS vldesconto,
            NULLIF(TRIM(CAST(tipovenda AS VARCHAR)), '') AS tipovenda,
            TRY_CAST(REPLACE(REPLACE(TRIM(vlcustoreal_txt), '.', ''), ',', '.') AS DECIMAL(18,2)) AS vlcustoreal,
            TRY_CAST(REPLACE(REPLACE(TRIM(vlcustofin_txt), '.', ''), ',', '.') AS DECIMAL(18,2)) AS vlcustofin,
            TRY_CAST(REPLACE(REPLACE(TRIM(vloutrasdesp_txt), '.', ''), ',', '.') AS DECIMAL(18,2)) AS vloutrasdesp,
            TRY_CAST(REPLACE(REPLACE(TRIM(totpeso_txt), '.', ''), ',', '.') AS DECIMAL(18,6)) AS totpeso,
            TRY_CAST(REPLACE(REPLACE(TRIM(totvolume_txt), '.', ''), ',', '.') AS DECIMAL(18,6)) AS totvolume,
            TRY_CAST(REPLACE(REPLACE(TRIM(vldevolucao_txt), '.', ''), ',', '.') AS DECIMAL(18,2)) AS vldevolucao,

            -- códigos
            TRY_CAST(codcont       AS BIGINT) AS codcont,
            TRY_CAST(codfiscal     AS BIGINT) AS codfiscal,
            TRY_CAST(codcli        AS BIGINT) AS codcli,
            TRY_CAST(codusur       AS BIGINT) AS codusur,
            TRY_CAST(codpraca      AS BIGINT) AS codpraca,
            TRY_CAST(numitens      AS BIGINT) AS numitens,
            TRY_CAST(codemitente   AS BIGINT) AS codemitente,
            TRY_CAST(numcar        AS BIGINT) AS numcar,
            NULLIF(TRIM(CAST(codcob AS VARCHAR)), '') AS codcob,
            TRY_CAST(REPLACE(REPLACE(TRIM(numped_txt), '.', ''), ',', '.') AS DECIMAL(38,0)) AS numped,
            TRY_CAST(codfornec     AS BIGINT) AS codfornec,
            TRY_CAST(numtransvenda AS BIGINT) AS numtransvenda,
            TRY_CAST(codplpag      AS BIGINT) AS codplpag,
            TRY_CAST(codfunccancel AS BIGINT) AS codfunccancel,
            TRY_CAST(numtab        AS BIGINT) AS numtab,
            TRY_CAST(numseq        AS BIGINT) AS numseq,
            TRY_CAST(codsupervisor AS BIGINT) AS codsupervisor,
            TRY_CAST(condvenda     AS BIGINT) AS condvenda
        FROM src_txt
    ),
    stamped AS (
        SELECT
            *,
            NOW() AT TIME ZONE 'UTC' AS _ingestion_ts_utc,
            CAST(NOW() AT TIME ZONE 'UTC' AS DATE) AS _ingestion_date,
            NOW() AT TIME ZONE 'UTC' AS _silver_ts_utc
        FROM norm
        WHERE especie IS NOT NULL
          AND serie IS NOT NULL
          AND numnota IS NOT NULL
          AND codfilial IS NOT NULL
    ),
    dedup AS (
        SELECT *
        FROM (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY especie, serie, numnota, codfilial
                    ORDER BY _silver_ts_utc DESC
                ) AS rn
            FROM stamped
        )
        WHERE rn = 1
    )
    SELECT
        especie, serie, numnota, codfilial,
        dtsaida, dtentrega, dtcancel,
        vltotal, vltotger, vltabela, vldesconto, tipovenda,
        vlcustoreal, vlcustofin, vloutrasdesp, totpeso, totvolume, vldevolucao,
        codcont, codfiscal, codcli, codusur, codpraca, numitens, codemitente,
        numcar, codcob, numped, codfornec, numtransvenda, codplpag, codfunccancel,
        numtab, numseq, codsupervisor, condvenda,
        _ingestion_ts_utc, _ingestion_date, _silver_ts_utc
    FROM dedup;
    """


def extract_silver_rows_duckdb(
    con: duckdb.DuckDBPyConnection,
    s3_glob: str,
    arrow_batch_size: int = 100_000,
) -> Tuple[List[str], Iterable[Tuple]]:
    """
    Retorna (colunas, iterável de linhas) vindo do DuckDB.
    Lê em batches (Arrow) para evitar explodir memória.
    """
    sql = _build_duckdb_transform_sql(s3_glob)
    rel = con.sql(sql)

    columns = [d[0] for d in rel.description]
    logger.info("🧠 DuckDB: %s colunas", len(columns))

    def row_iter() -> Iterable[Tuple]:
        for batch in rel.fetch_record_batch(arrow_batch_size):
            arrays = [batch.column(i).to_pylist() for i in range(batch.num_columns)]
            for row in zip(*arrays):
                yield row

    return columns, row_iter()


# ===========================
# UPSERT (Postgres)
# ===========================
def upsert_rows(
    conn,
    columns: Sequence[str],
    rows: Iterable[Tuple],
    chunk_size: int = CHUNK_SIZE,
) -> int:
    if not columns:
        logger.info("⚠️ Nenhuma coluna retornada do DuckDB.")
        return 0

    update_cols = [c for c in columns if c not in PK_COLS]

    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT ({', '.join(PK_COLS)}) DO UPDATE SET
        {', '.join([f"{c}=EXCLUDED.{c}" for c in update_cols])}
    """

    total = 0
    buffer: List[Tuple] = []

    with conn.cursor() as cur:
        for row in rows:
            buffer.append(row)
            if len(buffer) >= chunk_size:
                psycopg2.extras.execute_values(cur, insert_sql, buffer, page_size=chunk_size)
                total += len(buffer)
                buffer.clear()

        if buffer:
            psycopg2.extras.execute_values(cur, insert_sql, buffer, page_size=len(buffer))
            total += len(buffer)

    conn.commit()
    return total


# ===========================
# MAIN
# ===========================
def main() -> None:
    script_dir = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.normpath(os.path.join(script_dir, "..", ".env"))
    logger.info("📄 Usando .env: %s", env_path)

    pg_cfg = load_postgres_config(env_path)
    minio_cfg = load_minio_config(env_path)

    host_port, use_ssl = normalize_minio_endpoint(minio_cfg.endpoint)
    logger.info("🧩 MINIO_ENDPOINT (.env) = %s", minio_cfg.endpoint)
    logger.info("🧩 DuckDB s3_endpoint     = %s | s3_use_ssl=%s", host_port, use_ssl)

    # Glob recursivo (pega subpastas e partições)
    s3_glob = f"s3://{minio_cfg.bucket}/{BRONZE_PREFIX}/**/*.parquet"
    logger.info("📦 Bronze glob: %s", s3_glob)

    duck = duckdb_connection(minio_cfg)
    pg = pg_connection(pg_cfg)

    try:
        # Teste rápido: garante que lê os parquets
        duck.sql(f"SELECT COUNT(*) AS n FROM read_parquet('{s3_glob}', union_by_name=true)").show()

        ensure_schema_and_table(pg)

        cols, rows_iter = extract_silver_rows_duckdb(duck, s3_glob)
        loaded = upsert_rows(pg, cols, rows_iter, chunk_size=CHUNK_SIZE)

        logger.info("✅ Carga concluída em %s (%s linhas upsert)", TARGET_TABLE, loaded)

    finally:
        try:
            duck.close()
        except Exception:
            pass
        pg.close()


if __name__ == "__main__":
    main()