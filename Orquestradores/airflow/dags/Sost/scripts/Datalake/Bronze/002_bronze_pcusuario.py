#!/usr/bin/env python3
"""
Bronze Layer ETL: Extração PCUSUARIO PostgreSQL → MinIO

Ajustes (igual fizemos no PCSUPERVISOR):
- Cursor server-side (named cursor) para stream
- Colunas obtidas via SELECT ... LIMIT 0 (evita erro "0 columns passed")
- Normalização Dremio-friendly:
  - datetime -> datetime64[ms] (evita TIMESTAMP(NANOS) no Parquet)
  - numeric/decimal/object numérico -> float (bronze) ou Int64 nullable
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
logger = logging.getLogger("bronze_pcusuario")

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
    chunk_rows: int = 200_000

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

def get_columns(conn, sql: str) -> list[str]:
    """Obtém colunas com SELECT ... LIMIT 0 (robusto para named cursor)."""
    with conn.cursor() as cur:
        cur.execute(sql + " LIMIT 0")
        return [d[0] for d in (cur.description or [])]

# ===========================
# PARQUET NORMALIZATION (DREMIO-FRIENDLY)
# ===========================
def normalize_for_dremio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ajusta tipos para evitar TIMESTAMP(NANOS) e variações 'object/empty' em Parquet.
    Como PCUSUARIO pode variar, a estratégia aqui é genérica e segura.
    """
    out = df.copy()

    # 1) Qualquer coluna datetime -> ms
    # (detecta datetime64 e também object que pareça datetime)
    for c in out.columns:
        s = out[c]
        if pd.api.types.is_datetime64_any_dtype(s):
            out[c] = s.astype("datetime64[ms]")
        elif s.dtype == "object":
            # tenta converter somente se houver indícios (não converte textos normais)
            sample = s.dropna().head(20)
            if not sample.empty:
                # heurística: se tem datetime/date vindo do psycopg2, converte
                if any(hasattr(v, "year") and hasattr(v, "month") and hasattr(v, "day") for v in sample):
                    out[c] = pd.to_datetime(out[c], errors="coerce").astype("datetime64[ms]")

    # 2) DECIMAL / NUMERIC que chegam como object (Decimal) -> float64
    # Heurística: se a coluna object tem Decimal, converte pra número
    for c in out.columns:
        s = out[c]
        if s.dtype == "object":
            sample = s.dropna().head(50)
            if sample.empty:
                continue
            # se parece número (Decimal/int/float) converte; se não, ignora
            if all(isinstance(v, (int, float)) or str(type(v)).endswith("Decimal'>") for v in sample):
                out[c] = pd.to_numeric(out[c], errors="coerce").astype("float64")

    # 3) Inteiros “vazios” (tudo null) que viram object -> Int64 nullable
    # (aplica só quando a coluna é object e parece inteiro)
    for c in out.columns:
        s = out[c]
        if s.dtype == "object":
            sample = s.dropna().head(50)
            if sample.empty:
                continue
            # se todos valores são int (ou Decimal sem parte fracionária), converte
            def _is_intish(v):
                if isinstance(v, bool):
                    return False
                if isinstance(v, int):
                    return True
                if isinstance(v, float):
                    return v.is_integer()
                if str(type(v)).endswith("Decimal'>"):
                    try:
                        return float(v).is_integer()
                    except Exception:
                        return False
                return False

            if all(_is_intish(v) for v in sample):
                out[c] = pd.to_numeric(out[c], errors="coerce").astype("Int64")

    return out

# ===========================
# WRITE TO MINIO
# ===========================
def make_s3_path(bucket: str, part: int) -> str:
    return f"s3://{bucket}/bronze/002_bronze_pcusuario/part-{part + 1:05d}.parquet"

def clear_minio_prefix(fs: s3fs.S3FileSystem, bucket: str, prefix: str) -> int:
    target_prefix = f"{bucket}/{prefix.strip('/')}"
    old_files = fs.find(target_prefix)

    if old_files:
        fs.rm(old_files)
        logger.info("🧹 MinIO limpo: prefixo=%s | arquivos_removidos=%s", f"{target_prefix}/", len(old_files))
    else:
        logger.info("🧹 MinIO sem arquivos antigos em: %s", f"{target_prefix}/")

    return len(old_files)

def write_parquet_to_minio(df: pd.DataFrame, fs: s3fs.S3FileSystem, s3_path: str, ingestion_dt: datetime) -> int:
    df = normalize_for_dremio(df)

    df["_ingestion_ts_utc"] = ingestion_dt.isoformat()
    df["_ingestion_date"] = ingestion_dt.date().isoformat()

    table_pa = pa.Table.from_pandas(df, preserve_index=False)
    with fs.open(s3_path, "wb") as f:
        pq.write_table(
            table_pa,
            f,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True
        )

    return 0

# ===========================
# COPY TO LOCAL FOLDER
# ===========================
def copy_parquet_to_local(fs: s3fs.S3FileSystem, s3_path: str, local_folder: str) -> str:
    """Copia parquet do MinIO para pasta local (com fallback)"""
    local_file = os.path.join(local_folder, "part-00001.parquet")

    try:
        os.makedirs(local_folder, exist_ok=True)
        logger.info("📥 Copiando %s → %s", s3_path, local_file)
        with fs.open(s3_path, "rb") as src, open(local_file, "wb") as dst:
            chunk_size = 1024 * 1024  # 1MB
            while True:
                chunk = src.read(chunk_size)
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
def extract_to_minio(conn, fs: s3fs.S3FileSystem, spec: TableSpec, minio_cfg: MinioConfig, ingestion_dt: datetime) -> dict:
    total_rows = count_rows(conn, spec)
    chunks = max(1, math.ceil(total_rows / spec.chunk_rows))

    logger.info(
        "🚀 Extraindo %s.%s | total=%s | chunk=%s | chunks=%s",
        spec.schema, spec.name, total_rows, spec.chunk_rows, chunks
    )

    sql = build_select_sql(spec)
    stats = {"total_rows": total_rows, "chunks_created": 0, "files_written": 0, "bytes_written": 0}

    # ✅ pega colunas com LIMIT 0 (robusto)
    cols = get_columns(conn, sql)
    if not cols:
        raise RuntimeError("Não foi possível obter colunas do SELECT (cols vazio). Verifique schema/tabela/SQL.")

    # ✅ cursor server-side (named) para stream
    cur = conn.cursor(name="pcusuario_cursor")
    cur.itersize = spec.chunk_rows
    cur.execute(sql)

    part = 0
    try:
        while True:
            rows = cur.fetchmany(spec.chunk_rows)
            if not rows:
                break

            df = pd.DataFrame.from_records(rows, columns=cols)

            s3_path = make_s3_path(minio_cfg.bucket, part)
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
# MAIN
# ===========================
def main():
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    env_path = os.path.join(BASE_DIR, ".env")

    logger.info("📄 Carregando .env de: %s", env_path)

    pg_cfg = load_postgres_config(env_path)
    minio_cfg = load_minio_config(env_path)
    fs = minio_fs(minio_cfg)
    bronze_prefix = "bronze/002_bronze_pcusuario"

    clear_minio_prefix(fs, minio_cfg.bucket, bronze_prefix)

    spec = TableSpec(schema="public", name="pcusuari", chunk_rows=50_000)
    ingestion_dt = datetime.now(timezone.utc)

    with pg_connection(pg_cfg) as conn:
        stats = extract_to_minio(
            conn=conn,
            fs=fs,
            spec=spec,
            minio_cfg=minio_cfg,
            ingestion_dt=ingestion_dt
        )

    # 📥 Copiar o primeiro parquet para pasta local (opcional)
    s3_path = f"s3://{minio_cfg.bucket}/bronze/002_bronze_pcusuario/part-00001.parquet"
    local_folder = "/home/Projetos/Docker/Orquestradores/airflow/data/Sost/Datalake/Bronze/002_bronze_pcusuario"
    copy_parquet_to_local(fs, s3_path, local_folder)

    logger.info("📈 Estatísticas finais: %s", stats)
    return stats

if __name__ == "__main__":
    main()