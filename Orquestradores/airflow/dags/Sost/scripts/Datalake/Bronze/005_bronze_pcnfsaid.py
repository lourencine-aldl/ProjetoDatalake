#!/usr/bin/env python3
"""
Bronze Layer ETL: Extração PCNFSAID PostgreSQL → MinIO (streaming + low memory)
- Polars direto (sem Pandas)
- Mantém somente a última extração (limpa prefixo antes de extrair)
"""

import os
import gc
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from contextlib import contextmanager

import polars as pl
import pandas as pd
import psycopg2
import s3fs
from dotenv import load_dotenv

# ===========================
# LOGGING
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("bronze_pcnfsaid")

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
    region: str = "us-east-1"

@dataclass(frozen=True)
class TableSpec:
    schema: str
    name: str
    chunk_rows: int = 25_000   # linhas por chunk (fetchmany)
    itersize: int = 5_000      # buffer do cursor server-side

# ===========================
# CONFIG LOADERS
# ===========================
def load_postgres_config(env_path: str = ".env") -> PostgresConfig:
    load_dotenv(env_path)

    host = os.getenv("POSTGRES_DW_HOST", "").strip()
    port = int(os.getenv("POSTGRES_DW_PORT", "5432").strip() or "5432")
    database = os.getenv("POSTGRES_DW_DB", "").strip()
    user = os.getenv("POSTGRES_DW_USER", "").strip()
    password = os.getenv("POSTGRES_DW_PASSWORD", "").strip()

    if not (host and database and user and password):
        raise ValueError("Configuração PostgreSQL incompleta no .env")

    return PostgresConfig(host=host, port=port, database=database, user=user, password=password)

def load_minio_config(env_path: str = ".env") -> MinioConfig:
    load_dotenv(env_path)

    endpoint = os.getenv("MINIO_ENDPOINT", "").strip()
    bucket = os.getenv("MINIO_BUCKET_SOST", "").strip()
    access_key = os.getenv("MINIO_ROOT_USER", "").strip()
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "").strip()
    region = os.getenv("MINIO_REGION", "us-east-1").strip() or "us-east-1"

    if not (endpoint and bucket and access_key and secret_key):
        raise ValueError("Configuração MinIO incompleta no .env")

    return MinioConfig(endpoint=endpoint, bucket=bucket, access_key=access_key, secret_key=secret_key, region=region)

# ===========================
# CONNECTIONS
# ===========================
def minio_fs(cfg: MinioConfig) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=cfg.access_key,
        secret=cfg.secret_key,
        client_kwargs={"endpoint_url": cfg.endpoint, "region_name": cfg.region},
        default_fill_cache=False,
        config_kwargs={"max_pool_connections": 20},
    )

@contextmanager
def pg_connection(cfg: PostgresConfig):
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password,
    )
    try:
        conn.autocommit = False
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

# ===========================
# SQL UTILS
# ===========================
def build_select_sql(spec: TableSpec) -> str:
    return f'SELECT * FROM "{spec.schema}"."{spec.name}"'

# ===========================
# PATHS
# ===========================
def make_s3_path(bucket: str, base_prefix: str, part: int) -> str:
    return f"s3://{bucket}/{base_prefix}/part-{part + 1:05d}.parquet"

def list_parquets(fs: s3fs.S3FileSystem, bucket: str, base_prefix: str) -> list[str]:
    target_prefix = f"{bucket}/{base_prefix.strip('/')}/"
    return sorted(fs.glob(f"{target_prefix}part-*.parquet"))

def clear_minio_prefix(fs: s3fs.S3FileSystem, bucket: str, base_prefix: str) -> int:
    target_prefix = f"{bucket}/{base_prefix.strip('/')}"
    old_files = fs.find(target_prefix)
    if old_files:
        fs.rm(old_files)
        logger.info("🧹 MinIO limpo: prefixo=%s | removidos=%s", f"{target_prefix}/", len(old_files))
    else:
        logger.info("🧹 MinIO sem arquivos antigos em: %s", f"{target_prefix}/")
    return len(old_files)

# ===========================
# WRITE
# ===========================
def write_parquet_to_minio(df: pl.DataFrame, fs: s3fs.S3FileSystem, s3_path: str, ingestion_dt: datetime) -> None:
    df2 = df.with_columns([
        pl.lit(ingestion_dt.isoformat()).alias("_ingestion_ts_utc"),
        pl.lit(ingestion_dt.date().isoformat()).alias("_ingestion_date"),
    ])
    with fs.open(s3_path, "wb", block_size=64 * 1024 * 1024) as f:
        df2.write_parquet(f, compression="snappy")

# ===========================
# COPY TO LOCAL (opcional)
# ===========================
def copy_parquet_to_local(fs: s3fs.S3FileSystem, s3_path: str, local_file: str) -> str | None:
    try:
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        logger.info("📥 Copiando %s → %s", s3_path, local_file)
        with fs.open(s3_path, "rb") as src, open(local_file, "wb") as dst:
            while True:
                chunk = src.read(8 * 1024 * 1024)  # 8MB
                if not chunk:
                    break
                dst.write(chunk)
        logger.info("✅ Arquivo copiado: %s", local_file)
        return local_file
    except Exception as e:
        logger.warning("⚠️ Falha ao copiar arquivo local (continuando): %s", e)
        return None

# ===========================
# EXTRACT
# ===========================
def extract_to_minio(conn, fs: s3fs.S3FileSystem, spec: TableSpec, base_prefix: str, bucket: str, ingestion_dt: datetime) -> dict:
    sql = build_select_sql(spec)
    stats = {"chunks_created": 0, "files_written": 0, "rows_written": 0}

    logger.info("🚀 Extraindo %s.%s | chunk_rows=%s | itersize=%s | prefix=%s",
                spec.schema, spec.name, spec.chunk_rows, spec.itersize, base_prefix)

    cur_name = f"cur_{spec.schema}_{spec.name}_{int(ingestion_dt.timestamp())}"
    cur = conn.cursor(name=cur_name)
    cur.itersize = spec.itersize
    cur.execute(sql)

    cols = [d[0] for d in cur.description] if cur.description else []
    if not cols:
        logger.warning("⚠️ Named cursor sem description; buscando colunas no information_schema...")
        with conn.cursor() as cur_meta:
            cur_meta.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = %s
                  AND table_name = %s
                ORDER BY ordinal_position
                """,
                (spec.schema, spec.name),
            )
            cols = [r[0] for r in cur_meta.fetchall()]

    if not cols:
        raise RuntimeError(
            f"Não foi possível identificar colunas de {spec.schema}.{spec.name}. "
            "Abortando para evitar parquet com colunas numéricas (0..N)."
        )

    logger.info("🔍 Detectadas %s colunas: %s...", len(cols), cols[:5])
    part = 0

    try:
        while True:
            rows = cur.fetchmany(spec.chunk_rows)
            if not rows:
                break

            # Pandas intermediário para inferência de tipos, depois Polars
            df_pd = pd.DataFrame(rows, columns=cols) if cols else pd.DataFrame(rows)
            df_pd = df_pd.infer_objects(copy=False)
            df = pl.from_pandas(df_pd)

            s3_path = make_s3_path(bucket=bucket, base_prefix=base_prefix, part=part)
            logger.info("📦 Chunk %s | linhas=%s | %s", part + 1, df.height, s3_path)

            write_parquet_to_minio(df, fs, s3_path, ingestion_dt)

            stats["chunks_created"] += 1
            stats["files_written"] += 1
            stats["rows_written"] += df.height
            part += 1

            del df, df_pd, rows
            gc.collect()
    finally:
        cur.close()

    logger.info("✅ Concluído %s.%s | arquivos=%s | linhas=%s",
                spec.schema, spec.name, stats["files_written"], stats["rows_written"])
    return stats

# ===========================
# MAIN
# ===========================
def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(BASE_DIR, ".env")
    logger.info("📄 Carregando .env de: %s", env_path)

    pg_cfg = load_postgres_config(env_path)
    minio_cfg = load_minio_config(env_path)
    fs = minio_fs(minio_cfg)

    table_prefix = "bronze/005_bronze_pcnfsaid"
    clear_minio_prefix(fs, minio_cfg.bucket, table_prefix)

    ingestion_dt = datetime.now(timezone.utc)
    base_prefix = table_prefix

    spec = TableSpec(schema="public", name="pcnfsaid", chunk_rows=25_000, itersize=5_000)

    with pg_connection(pg_cfg) as conn:
        stats = extract_to_minio(
            conn=conn,
            fs=fs,
            spec=spec,
            base_prefix=base_prefix,
            bucket=minio_cfg.bucket,
            ingestion_dt=ingestion_dt,
        )

    # ✅ Copia o ÚLTIMO parquet gerado (opcional)
    files = list_parquets(fs, minio_cfg.bucket, base_prefix)
    if files:
        last_file = files[-1]
        s3_path = f"s3://{last_file}"
        local_folder = "/home/Projetos/Docker/Orquestradores/airflow/data/Sost/Datalake/Bronze/005_bronze_pcnfsaid"
        local_file = os.path.join(local_folder, os.path.basename(last_file))
        copy_parquet_to_local(fs, s3_path, local_file)

    logger.info("📈 Estatísticas finais: %s", stats)
    return stats


if __name__ == "__main__":
    main()