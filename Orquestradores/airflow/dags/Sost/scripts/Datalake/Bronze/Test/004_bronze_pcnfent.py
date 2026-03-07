#!/usr/bin/env python3
"""
Bronze Layer ETL: Extração PCNFSAID PostgreSQL → MinIO
=====================================
Script para extrair dados da tabela pcnfsaid do PostgreSQL
e salvar em formato Parquet no MinIO (S3-compatible).

Características:
- Cursor servidor-side (sem carregar tudo em memória)
- Chunks de 50k registros
- Compressão Snappy
- Logging estruturado
"""

import os
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
# SETUP DE LOGGING
# ===========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ===========================
# DATACLASSES PARA CONFIGURAÇÃO
# ===========================

@dataclass(frozen=True)
class PostgresConfig:
    """Configuração PostgreSQL"""
    host: str
    port: int
    database: str
    user: str
    password: str


@dataclass(frozen=True)
class MinioConfig:
    """Configuração MinIO"""
    endpoint: str
    bucket: str
    access_key: str
    secret_key: str


@dataclass(frozen=True)
class TableSpec:
    """Especificação de tabela"""
    schema: str
    name: str
    chunk_rows: int = 50_000  # Reduzido para evitar picos de memória


# ===========================
# CARREGAMENTO DE CONFIGURAÇÃO
# ===========================

def load_postgres_config() -> PostgresConfig:
    """Carrega configuração PostgreSQL do .env"""
    load_dotenv(".env")

    host = os.getenv("POSTGRES_DW_HOST", "").strip()
    port = int(os.getenv("POSTGRES_DW_PORT", "5432").strip()) if os.getenv("POSTGRES_DW_PORT") else 5432
    database = os.getenv("POSTGRES_DW_DB", "").strip()
    user = os.getenv("POSTGRES_DW_USER", "").strip()
    password = os.getenv("POSTGRES_DW_PASSWORD", "").strip()

    if not (host and database and user and password):
        raise ValueError(
            f"❌ Configuração PostgreSQL incompleta:\n"
            f"  HOST: {host if host else 'VAZIO'} | PORT: {port}\n"
            f"  DATABASE: {database if database else 'VAZIO'} | USER: {user if user else 'VAZIO'}"
        )

    return PostgresConfig(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )


def load_minio_config() -> MinioConfig:
    """Carrega configuração MinIO do .env"""
    load_dotenv(".env")

    endpoint = os.getenv("MINIO_ENDPOINT", "").strip()
    bucket = os.getenv("MINIO_BUCKET_SOST", "").strip()
    access_key = os.getenv("MINIO_ROOT_USER", "").strip()
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "").strip()

    if not (endpoint and bucket and access_key and secret_key):
        raise ValueError(
            f"❌ Configuração MinIO incompleta:\n"
            f"  ENDPOINT: {endpoint[:20]}... | BUCKET: {bucket}\n"
            f"  ACCESS_KEY: {access_key[:5]}... | SECRET_KEY: {secret_key[:5] if secret_key else 'VAZIO'}..."
        )

    return MinioConfig(
        endpoint=endpoint,
        bucket=bucket,
        access_key=access_key,
        secret_key=secret_key
    )


# ===========================
# CONEXÕES
# ===========================

def minio_fs(cfg: MinioConfig) -> s3fs.S3FileSystem:
    """Cria conexão S3FileSystem com MinIO"""
    return s3fs.S3FileSystem(
        key=cfg.access_key,
        secret=cfg.secret_key,
        client_kwargs={"endpoint_url": cfg.endpoint},
        config_kwargs={"signature_version": "s3v4"},
        use_ssl=False,
    )


@contextmanager
def pg_connection(cfg: PostgresConfig):
    """Context manager para conexão PostgreSQL"""
    conn = psycopg2.connect(
        host=cfg.host,
        port=cfg.port,
        database=cfg.database,
        user=cfg.user,
        password=cfg.password
    )
    try:
        yield conn
    finally:
        conn.close()


# ===========================
# FUNÇÕES DE UTILIDADE
# ===========================

def build_select_sql(table_spec: TableSpec) -> str:
    """Constrói SQL de seleção"""
    return f"SELECT * FROM {table_spec.schema}.{table_spec.name};"


def count_rows(conn, table_spec: TableSpec) -> int:
    """Conta linhas na tabela"""
    sql = f"SELECT COUNT(*) as cnt FROM {table_spec.schema}.{table_spec.name};"
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchone()[0]


def write_parquet_to_minio(
    df: pd.DataFrame,
    fs: s3fs.S3FileSystem,
    bucket: str,
    schema: str,
    table: str,
    chunk_idx: int,
    ingestion_dt: datetime,
    filename: str = None
) -> str:
    """Escreve parquet direto no MinIO sem passar pelo disco local"""
    df["_ingestion_ts_utc"] = ingestion_dt.isoformat()
    df["_ingestion_date"] = ingestion_dt.date().isoformat()

    # ✅ Usa nome customizado ou gera com parte numerada
    if filename:
        file_part = filename
    else:
        file_part = f"part-{chunk_idx:05d}.parquet"

    s3_path = f"s3://{bucket}/bronze/{file_part}"

    table_pa = pa.Table.from_pandas(df, preserve_index=False)

    # ✅ Escreve direto no S3/MinIO (sem cache local)
    with fs.open(s3_path, "wb") as f:
        pq.write_table(table_pa, f, compression="snappy")

    logger.info(f"✅ Arquivo escrito: {s3_path}")
    return s3_path


# ===========================
# FUNÇÃO PRINCIPAL DE EXTRAÇÃO
# ===========================

def extract_to_minio(
    conn,
    fs: s3fs.S3FileSystem,
    table_spec: TableSpec,
    minio_cfg: MinioConfig,
    ingestion_dt: datetime
) -> dict:
    """Extrai tabela PostgreSQL → MinIO usando cursor servidor-side"""

    logger.info(f"🚀 Iniciando extração: {table_spec.schema}.{table_spec.name}")

    # Conta linhas totais
    total_rows = count_rows(conn, table_spec)
    logger.info(f"📊 Total de linhas: {total_rows}")

    sql = build_select_sql(table_spec)
    stats = {
        "total_rows": total_rows,
        "chunks_created": 0,
        "files_written": 0,
        "bytes_written": 0
    }

    # ✅ CURSOR SERVIDOR-SIDE: Não carrega tudo em memória
    cur = conn.cursor()
    cur.execute(sql)

    chunk_idx = 0
    while True:
        # Fetch de 50k linhas por vez
        rows = cur.fetchmany(table_spec.chunk_rows)
        if not rows:
            break

        chunk_idx += 1
        logger.info(f"📦 Processando chunk {chunk_idx} ({len(rows)} linhas)...")

        # Converte para DataFrame
        cols = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=cols)

        # Escreve direto para MinIO
        s3_path = write_parquet_to_minio(
            df=df,
            fs=fs,
            bucket=minio_cfg.bucket,
            schema=table_spec.schema,
            table=table_spec.name,
            chunk_idx=chunk_idx,
            ingestion_dt=ingestion_dt,
            filename="005_bronze_pcnfsaid.parquet"
        )

        stats["chunks_created"] += 1
        stats["files_written"] += 1

    cur.close()
    logger.info(f"✅ Extração completa!")
    logger.info(f"📈 Estatísticas: {stats}")
    return stats


# ===========================
# EXECUÇÃO PRINCIPAL
# ===========================

def main():
    """Orquestra a extração completa"""
    try:
        logger.info("="*60)
        logger.info("🎯 INICIANDO EXTRAÇÃO BRONZE: pcnfsaid")
        logger.info("="*60)

        # Carrega configurações
        pg_cfg = load_postgres_config()
        minio_cfg = load_minio_config()
        logger.info(f"✅ Configurações carregadas")
        logger.info(f"   PostgreSQL: {pg_cfg.host}:{pg_cfg.port}/{pg_cfg.database}")
        logger.info(f"   MinIO: {minio_cfg.endpoint} / {minio_cfg.bucket}")

        # Cria conexão MinIO
        fs = minio_fs(minio_cfg)
        logger.info(f"✅ Conexão MinIO estabelecida")

        # Define tabela a extrair
        table_spec = TableSpec(schema="public", name="pcnfsaid", chunk_rows=50_000)

        # Conecta ao PostgreSQL e extrai
        with pg_connection(pg_cfg) as conn:
            stats = extract_to_minio(
                conn=conn,
                fs=fs,
                table_spec=table_spec,
                minio_cfg=minio_cfg,
                ingestion_dt=datetime.now(timezone.utc)
            )

        logger.info("="*60)
        logger.info("✅ SUCESSO!")
        logger.info("="*60)
        return stats

    except Exception as e:
        logger.error(f"❌ ERRO: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    result = main()
    print("\n" + "="*60)
    print("RESULTADO FINAL:")
    print("="*60)
    for key, value in result.items():
        print(f"{key}: {value}")
    print("="*60)
