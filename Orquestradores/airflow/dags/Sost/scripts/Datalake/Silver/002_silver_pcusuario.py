#!/usr/bin/env python3
"""
Silver Layer ETL: PCUSUARIO MinIO (Bronze) -> PostgreSQL DW (public.silver_pcusuario)

Baseado no "Excel" enviado para definir tipos:
- codusur INTEGER (PK)
- tipovend CHAR(1)
- estado CHAR(2)
- cep VARCHAR(10)
- telefone1/telefone2 VARCHAR(30)
- cpf VARCHAR(14)
- bloqueio CHAR(1)
- datas como TIMESTAMP (no Excel vem com 00:00:00.000)

✅ Corrige o erro 42P01: garante CREATE SCHEMA + CREATE TABLE antes do upsert.
✅ Lê todos os parquets em: s3://<bucket>/bronze/002_bronze_pcusuario/*.parquet
✅ Upsert (ON CONFLICT) em batches.
"""

import io
import os
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import List, Tuple, Optional

import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv


# ===========================
# SETTINGS EDITÁVEIS
# ===========================
LOGGER_NAME = "silver_pcusuario"

BRONZE_PREFIX = "bronze/002_bronze_pcusuario"          # pasta no MinIO (sem bucket)
TARGET_TABLE = "public.silver_pcusuario"               # tabela destino no Postgres
CHUNK_SIZE = 5000                                      # batch upsert


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
    endpoint: str
    bucket: str
    access_key: str
    secret_key: str


# ===========================
# CONFIG LOADERS
# ===========================
def load_postgres_config(env_path: str) -> PostgresConfig:
    load_dotenv(env_path)

    host = os.getenv("POSTGRES_DW_HOST", "").strip()
    port = int(os.getenv("POSTGRES_DW_PORT", "5432").strip() or "5432")
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

    if not (endpoint and bucket and access_key and secret_key):
        raise ValueError("Configuração MinIO incompleta no .env")

    return MinioConfig(endpoint=endpoint, bucket=bucket, access_key=access_key, secret_key=secret_key)


# ===========================
# CONNECTIONS
# ===========================
def minio_fs(cfg: MinioConfig) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=cfg.access_key,
        secret=cfg.secret_key,
        client_kwargs={"endpoint_url": cfg.endpoint},
        config_kwargs={"signature_version": "s3v4"},
        use_ssl=False,
        default_fill_cache=False,
    )


def pg_connection(cfg: PostgresConfig):
    return psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
    )


# ===========================
# BRONZE READ
# ===========================
def list_bronze_parquets(fs: s3fs.S3FileSystem, bucket: str) -> List[str]:
    pattern = f"{bucket}/{BRONZE_PREFIX}/*.parquet"
    files = sorted(fs.glob(pattern))
    if not files:
        raise FileNotFoundError(f"Nenhum parquet encontrado em s3://{pattern}")
    logger.info("📦 %s parquet(s) encontrados no Bronze (%s)", len(files), f"s3://{pattern}")
    return files


def read_bronze_dataframe(fs: s3fs.S3FileSystem, files: List[str]) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in files:
        s3_uri = f"s3://{path}"
        logger.info("📥 Lendo %s", s3_uri)
        with fs.open(s3_uri, "rb") as f:
            table = pq.read_table(io.BytesIO(f.read()))
            frames.append(table.to_pandas())

    if not frames:
        raise RuntimeError("Falha ao carregar dados do Bronze")

    df = pd.concat(frames, ignore_index=True)
    logger.info("📊 Linhas lidas do Bronze (concat): %s", len(df))
    return df


# ===========================
# DEST TABLE (DDL)
# ===========================
def ensure_schema_and_table(conn) -> None:
    """
    Garante schema e tabela antes de qualquer operação.
    Isso evita: ERROR 42P01 relation does not exist
    """
    schema, table = TARGET_TABLE.split(".", 1)

    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        codusur           INTEGER PRIMARY KEY,
        nome              TEXT,
        tipovend          CHAR(1),
        endereco          TEXT,
        cidade            VARCHAR(120),
        estado            CHAR(2),
        cep               VARCHAR(10),
        telefone1         VARCHAR(30),
        telefone2         VARCHAR(30),
        cpf               VARCHAR(14),
        bloqueio          CHAR(1),

        dtinicio          TIMESTAMP,
        dttermino         TIMESTAMP,
        motivo            TEXT,
        dtnasc            TIMESTAMP,
        cgc               VARCHAR(20),
        bairro            VARCHAR(120),
        codsupervisor     INTEGER,
        dtultvenda        TIMESTAMP,
        email             VARCHAR(255),

        _ingestion_ts_utc TIMESTAMP,
        _ingestion_date   DATE,
        _silver_ts_utc    TIMESTAMP,
        created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_silver_pcusuario_ingestion_date
        ON {TARGET_TABLE}(_ingestion_date);
    """

    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()

    # valida existência (ótimo para debug de schema/DB errado)
    with conn.cursor() as cur:
        cur.execute("SELECT to_regclass(%s)", (TARGET_TABLE,))
        exists = cur.fetchone()[0]
    if not exists:
        raise RuntimeError(f"Falha ao criar/encontrar a tabela {TARGET_TABLE}. Verifique database/schema do .env.")
    logger.info("✅ Tabela %s criada/verificada", TARGET_TABLE)


def get_table_columns(conn) -> List[Tuple[str, str]]:
    schema, table = TARGET_TABLE.split(".", 1)
    sql = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [(row[0], row[1]) for row in cur.fetchall()]


# ===========================
# HELPERS (limpeza/conversão)
# ===========================
def _clean_str(x) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    return s


def _to_int(series: pd.Series) -> pd.Series:
    num = pd.to_numeric(series, errors="coerce")
    num = num.where(num.isna() | ((num % 1) == 0), pd.NA)
    return num.astype("Int64")


def _to_ts(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


def prepare_dataframe(df: pd.DataFrame, table_schema: List[Tuple[str, str]]) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]

    if "codusur" not in out.columns:
        raise ValueError("Coluna codusur não encontrada no parquet Bronze")

    # carimbo silver
    out["_silver_ts_utc"] = datetime.now(timezone.utc)

    # limpa strings principais
    for c in [
        "nome", "tipovend", "endereco", "cidade", "estado", "cep", "telefone1", "telefone2", "cpf",
        "bloqueio", "motivo", "cgc", "bairro", "email"
    ]:
        if c in out.columns:
            out[c] = out[c].apply(_clean_str)

    # PK
    out["codusur"] = _to_int(out["codusur"])
    out = out.dropna(subset=["codusur"])
    out["codusur"] = out["codusur"].astype("int64")

    # inteiros adicionais
    if "codsupervisor" in out.columns:
        out["codsupervisor"] = _to_int(out["codsupervisor"])

    # timestamps (mantém como TIMESTAMP)
    for c in ["dtinicio", "dttermino", "dtnasc", "dtultvenda", "_ingestion_ts_utc"]:
        if c in out.columns:
            out[c] = _to_ts(out[c])

    # ingestion_date (DATE)
    if "_ingestion_date" in out.columns:
        out["_ingestion_date"] = pd.to_datetime(out["_ingestion_date"], errors="coerce").dt.date

    # coerção final baseada no schema destino
    integer_types = {"smallint", "integer", "bigint"}
    datetime_types = {"date", "timestamp without time zone", "timestamp with time zone"}

    for col, data_type in table_schema:
        col_l = col.lower()
        if col_l not in out.columns or col_l == "created_at":
            continue

        if data_type in integer_types:
            out[col_l] = _to_int(out[col_l])

        elif data_type in datetime_types:
            if data_type == "date":
                out[col_l] = pd.to_datetime(out[col_l], errors="coerce").dt.date
            else:
                out[col_l] = _to_ts(out[col_l])

        else:
            out[col_l] = out[col_l].apply(_clean_str)

    # mantém só colunas do destino (sem created_at)
    target_cols = [c.lower() for c, _ in table_schema if c.lower() in out.columns and c.lower() != "created_at"]
    if "codusur" not in target_cols:
        raise ValueError("Coluna codusur não está disponível para carga")

    out = out[target_cols]

    # dedup por PK
    out = out.drop_duplicates(subset=["codusur"], keep="last")

    # NaN/NaT -> None
    out = out.astype(object).where(pd.notnull(out), None)
    return out


# ===========================
# UPSERT
# ===========================
def upsert_dataframe(conn, df: pd.DataFrame, chunk_size: int = CHUNK_SIZE) -> int:
    if df.empty:
        logger.info("⚠️ DataFrame vazio, nada para carregar.")
        return 0

    columns = list(df.columns)
    update_cols = [c for c in columns if c != "codusur"]

    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (codusur) DO UPDATE SET
        {', '.join([f"{c}=EXCLUDED.{c}" for c in update_cols])}
    """

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    total = len(rows)

    with conn.cursor() as cur:
        for i in range(0, total, chunk_size):
            batch = rows[i:i + chunk_size]
            psycopg2.extras.execute_values(cur, insert_sql, batch, page_size=chunk_size)

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

    fs = minio_fs(minio_cfg)
    files = list_bronze_parquets(fs, minio_cfg.bucket)
    bronze_df = read_bronze_dataframe(fs, files)

    conn = pg_connection(pg_cfg)
    try:
        ensure_schema_and_table(conn)          # ✅ garante existir antes de tudo
        table_schema = get_table_columns(conn)

        silver_df = prepare_dataframe(bronze_df, table_schema)
        loaded = upsert_dataframe(conn, silver_df, chunk_size=CHUNK_SIZE)

        logger.info("✅ Carga concluída em %s (%s linhas upsert)", TARGET_TABLE, loaded)
    finally:
        conn.close()


if __name__ == "__main__":
    main()