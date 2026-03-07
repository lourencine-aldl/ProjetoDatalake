#!/usr/bin/env python3
"""
Bronze Layer ETL: Extração TAB_FATURAMENTO_FULL PostgreSQL → MinIO
"""

import os
import math
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from contextlib import contextmanager

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
logger = logging.getLogger("bronze_006_tab_faturamento_full")


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
    endpoint: str          # ex: http://209.50.228.232:9000
    bucket: str            # ex: datalake-sost
    access_key: str
    secret_key: str
    region: str = "us-east-1"


@dataclass(frozen=True)
class TableSpec:
    schema: str
    name: str
    chunk_rows: int = 50_000


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

    return MinioConfig(
        endpoint=endpoint,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key,
        region=region,
    )


# ===========================
# CONNECTIONS
# ===========================
def minio_fs(cfg: MinioConfig) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=cfg.access_key,
        secret=cfg.secret_key,
        client_kwargs={
            "endpoint_url": cfg.endpoint,
            "region_name": cfg.region,
        },
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
        # IMPORTANTE: cursor server-side precisa de transação ativa
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


def count_rows(conn, spec: TableSpec) -> int:
    sql = f'SELECT COUNT(*) FROM "{spec.schema}"."{spec.name}"'
    with conn.cursor() as cur:
        cur.execute(sql)
        return int(cur.fetchone()[0])


# ===========================
# WRITE TO MINIO
# ===========================
def make_s3_path(bucket: str, schema: str, table: str, ingestion_dt: datetime, part: int) -> str:
    return f"s3://{bucket}/bronze/006_bronze_tab_faturamento_full/part-{part:05d}.parquet"

def write_parquet_to_minio(df: pd.DataFrame, fs: s3fs.S3FileSystem, s3_path: str, ingestion_dt: datetime) -> int:
    df["_ingestion_ts_utc"] = ingestion_dt.isoformat()
    df["_ingestion_date"] = ingestion_dt.date().isoformat()

    table_pa = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(s3_path, "wb") as f:
        pq.write_table(table_pa, f, compression="snappy")

    # não dá pra saber bytes reais sem head_object; retornamos 0 pra estatística simples
    return 0

# ===========================
# EXTRACT
# ===========================
def extract_to_minio(conn, fs: s3fs.S3FileSystem, spec: TableSpec, minio_cfg: MinioConfig, ingestion_dt: datetime) -> dict:
    total_rows = count_rows(conn, spec)
    chunks = max(1, math.ceil(total_rows / spec.chunk_rows))

    logger.info("🚀 Extraindo %s.%s | total=%s | chunk=%s | chunks=%s",
                spec.schema, spec.name, total_rows, spec.chunk_rows, chunks)

    sql = build_select_sql(spec)
    stats = {"total_rows": total_rows, "chunks_created": 0, "files_written": 0, "bytes_written": 0}

    # ✅ SERVER-SIDE CURSOR DE VERDADE (psycopg2)
    cur = conn.cursor()
    cur.execute(sql)

    # Pega nomes das colunas após execute
    cols = [desc[0] for desc in cur.description] if cur.description else []
    part = 0

    try:
        while True:
            rows = cur.fetchmany(spec.chunk_rows)
            if not rows:
                break

            df = pd.DataFrame(rows, columns=cols)

            s3_path = make_s3_path(minio_cfg.bucket, spec.schema, spec.name, ingestion_dt, part)
            logger.info("📦 Chunk %s | linhas=%s | gravando %s", part, len(df), s3_path)

            stats["bytes_written"] += write_parquet_to_minio(df, fs, s3_path, ingestion_dt)
            stats["chunks_created"] += 1
            stats["files_written"] += 1
            part += 1
    finally:
        cur.close()

    logger.info("✅ Concluído %s.%s | arquivos=%s", spec.schema, spec.name, stats["files_written"])
    return stats


# ===========================
# COPY TO LOCAL FOLDER
# ===========================
def copy_parquet_to_local(fs: s3fs.S3FileSystem, s3_prefix: str, local_folder: str) -> list:
    """Copia todos os parquets do MinIO para pasta local"""
    os.makedirs(local_folder, exist_ok=True)
    
    try:
        # Lista todos os arquivos com padrão part-*.parquet
        s3_files = fs.glob(f"{s3_prefix}/part-*.parquet")
        
        if not s3_files:
            logger.warning(f"⚠️ Nenhum arquivo encontrado em {s3_prefix}")
            return []
        
        logger.info(f"📥 Copiando {len(s3_files)} arquivos para {local_folder}")
        copied_files = []
        
        for s3_file in s3_files:
            filename = s3_file.split("/")[-1]
            local_file = os.path.join(local_folder, filename)
            
            try:
                with fs.open(s3_file, "rb") as src:
                    with open(local_file, "wb") as dst:
                        chunk_size = 1024 * 1024  # 1MB
                        while True:
                            chunk = src.read(chunk_size)
                            if not chunk:
                                break
                            dst.write(chunk)
                copied_files.append(local_file)
                logger.info(f"  ✅ {filename}")
            except Exception as e:
                logger.warning(f"  ⚠️ Falha ao copiar {filename}: {e}")
        
        logger.info(f"✅ {len(copied_files)} arquivo(s) copiado(s)")
        return copied_files
    except Exception as e:
        logger.warning(f"⚠️ Falha ao copiar arquivos locais (continuando): {e}")
        return []

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

    spec = TableSpec(schema="public", name="tab_faturamento_full", chunk_rows=50_000)
    ingestion_dt = datetime.now(timezone.utc)

    with pg_connection(pg_cfg) as conn:
        stats = extract_to_minio(
            conn=conn,
            fs=fs,
            spec=spec,
            minio_cfg=minio_cfg,
            ingestion_dt=ingestion_dt
        )

    # 📥 Copiar todos os parquets para pasta local
    s3_prefix = f"s3://{minio_cfg.bucket}/bronze/006_bronze_tab_faturamento_full"
    local_folder = "/home/Projetos/Docker/Orquestradores/airflow/data/Sost/Datalake/Bronze/006_bronze_tab_faturamento_full"
    copy_parquet_to_local(fs, s3_prefix, local_folder)

    logger.info("📈 Estatísticas finais: %s", stats)
    return stats


if __name__ == "__main__":
    main()
