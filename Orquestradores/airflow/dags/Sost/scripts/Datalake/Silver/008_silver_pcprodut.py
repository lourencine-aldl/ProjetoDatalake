#!/usr/bin/env python3
"""
Silver Layer ETL: PCPRODUT MinIO (Bronze) -> PostgreSQL DW (public.silver_pcprodut)

Modelo de campos baseado na amostra enviada (produto):
- codprod (PK)
- descricao, embalagem, unidade
- pesos/volume/dimensões
- códigos e flags de negócio
- datas (dtcadastro, dtexclusao, dtultaltcom)

✅ Garante CREATE SCHEMA + CREATE TABLE antes do upsert.
✅ Lê todos os parquets em: s3://<bucket>/bronze/008_bronze_pcprodut/*.parquet
✅ Upsert (ON CONFLICT) em batches.
✅ Conversão robusta de número pt-BR e notação científica.
"""

from __future__ import annotations

import io
import os
import re
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Tuple

import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv


# ===========================
# SETTINGS EDITÁVEIS
# ===========================
LOGGER_NAME = "silver_pcprodut"

BRONZE_PREFIX = "bronze/008_bronze_pcprodut"
TARGET_TABLE = "public.silver_pcprodut"
CHUNK_SIZE = 5000


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
    schema, _table = TARGET_TABLE.split(".", 1)

    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        codprod            INTEGER PRIMARY KEY,
        descricao          TEXT,
        embalagem          VARCHAR(40),
        unidade            VARCHAR(10),

        pesoliq            NUMERIC(18,6),
        pesobruto          NUMERIC(18,6),
        codepto            INTEGER,
        codsec             INTEGER,

        pcomint1           NUMERIC(18,6),
        temrepos           SMALLINT,
        qtunit             NUMERIC(18,6),
        obs                TEXT,

        pcomrep1           NUMERIC(18,6),
        pcomext1           NUMERIC(18,6),
        codfornec          BIGINT,

        dtcadastro         DATE,
        volume             NUMERIC(18,6),
        codauxiliar        VARCHAR(30),
        classe             VARCHAR(80),

        lastropal          NUMERIC(18,6),
        alturapal          NUMERIC(18,6),
        qttotpal           NUMERIC(18,6),
        prazoval           SMALLINT,
        qtunitcx           NUMERIC(18,6),

        revenda            CHAR(1),
        importado          CHAR(1),
        folharosto         TEXT,
        dtexclusao         DATE,

        modulo             SMALLINT,
        rua                SMALLINT,
        apto               SMALLINT,
        dtultaltcom        DATE,
        campanha           CHAR(1),

        codprodprinc       INTEGER,
        obs2               TEXT,
        percipi            NUMERIC(18,6),
        unidademaster      VARCHAR(10),
        corredor           VARCHAR(80),

        larguram3          NUMERIC(18,6),
        alturam3           NUMERIC(18,6),
        comprimentom3      NUMERIC(18,6),

        _ingestion_ts_utc  TIMESTAMP,
        _ingestion_date    DATE,
        _silver_ts_utc     TIMESTAMP,
        created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    CREATE INDEX IF NOT EXISTS idx_silver_pcprodut_ingestion_date
      ON {TARGET_TABLE}(_ingestion_date);
    """

    with conn.cursor() as cur:
        cur.execute(sql)

        # compatibilidade: versões anteriores criaram 'coddepto'; renomeia para 'codepto'
        cur.execute(
            f"""
            DO $$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'silver_pcprodut' AND column_name = 'coddepto'
                ) AND NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'silver_pcprodut' AND column_name = 'codepto'
                ) THEN
                    ALTER TABLE {TARGET_TABLE} RENAME COLUMN coddepto TO codepto;
                END IF;
            END
            $$;
            """
        )
    conn.commit()

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
_RE_SCI = re.compile(r"^[+-]?\d+([.,]\d+)?[eE][+-]?\d+$")


def _clean_str(x) -> Optional[str]:
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None
    return s


def _to_decimal_maybe(x) -> Optional[Decimal]:
    if x is None:
        return None
    if isinstance(x, float) and pd.isna(x):
        return None
    if isinstance(x, (int, Decimal)):
        return Decimal(str(x))
    if isinstance(x, float):
        return Decimal(str(x))

    s = str(x).strip()
    if s == "" or s.lower() in ("nan", "none"):
        return None

    s = s.replace(" ", "")

    # pt-BR + científico (ex: 7,89606E+12)
    if _RE_SCI.match(s):
        s = s.replace(",", ".")
    else:
        if "," in s:
            s = s.replace(".", "").replace(",", ".")

    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _to_int_series(series: pd.Series) -> pd.Series:
    vals = series.apply(_to_decimal_maybe)
    out = []
    for v in vals:
        if v is None:
            out.append(pd.NA)
            continue
        if v == v.to_integral_value():
            out.append(int(v))
        else:
            out.append(pd.NA)
    return pd.Series(out, index=series.index, dtype="Int64")


def _to_numeric_series(series: pd.Series) -> pd.Series:
    vals = series.apply(_to_decimal_maybe)
    return pd.Series(vals, index=series.index, dtype="object")


def _to_date_series(series: pd.Series) -> pd.Series:
    # cobre 01/12/12, 11/18/13, yyyy-mm-dd e já-date
    dt = pd.to_datetime(series, errors="coerce", dayfirst=True)
    # fallback para alguns formatos ambíguos em MM/DD/YY
    mask_na = dt.isna()
    if mask_na.any():
        dt2 = pd.to_datetime(series[mask_na], errors="coerce", dayfirst=False)
        dt.loc[mask_na] = dt2
    return dt.dt.date


def _to_code_text(x) -> Optional[str]:
    """
    Preserva códigos como texto e converte notação científica para inteiro em string.
    Ex.: 7,89606E+12 -> "7896060000000"
    """
    s = _clean_str(x)
    if s is None:
        return None

    dec = _to_decimal_maybe(s)
    if dec is None:
        return s

    if dec == dec.to_integral_value():
        return str(int(dec))
    return format(dec, "f").rstrip("0").rstrip(".")


# ===========================
# PREP DF
# ===========================
def prepare_dataframe(df: pd.DataFrame, table_schema: List[Tuple[str, str]]) -> pd.DataFrame:
    out = df.copy()
    out.columns = [c.strip().lower() for c in out.columns]

    # compatibilidade de nomenclatura observada em amostras: coddepto -> codepto
    if "coddepto" in out.columns and "codepto" not in out.columns:
        out = out.rename(columns={"coddepto": "codepto"})

    if "codprod" not in out.columns:
        raise ValueError("Coluna codprod não encontrada no parquet Bronze")

    out["_silver_ts_utc"] = datetime.now(timezone.utc)

    # strings
    for c in [
        "descricao", "embalagem", "unidade", "obs", "classe", "revenda", "importado",
        "folharosto", "rua", "apto", "campanha", "obs2", "unidademaster", "corredor"
    ]:
        if c in out.columns:
            out[c] = out[c].apply(_clean_str)

    # ints
    int_cols = [
        "codprod", "codepto", "codsec", "temrepos", "prazoval", "modulo", "rua", "apto", "codprodprinc"
    ]
    for c in int_cols:
        if c in out.columns:
            out[c] = _to_int_series(out[c])

    # bigint/ints grandes
    if "codfornec" in out.columns:
        out["codfornec"] = _to_int_series(out["codfornec"])

    # numéricos
    numeric_cols = [
        "pesoliq", "pesobruto", "pcomint1", "pcomrep1", "pcomext1", "volume",
        "qtunit", "lastropal", "alturapal", "qttotpal", "qtunitcx",
        "percipi", "larguram3", "alturam3", "comprimentom3"
    ]
    for c in numeric_cols:
        if c in out.columns:
            out[c] = _to_numeric_series(out[c])

    # códigos texto
    if "codauxiliar" in out.columns:
        out["codauxiliar"] = out["codauxiliar"].apply(_to_code_text)

    # datas
    for c in ["dtcadastro", "dtexclusao", "dtultaltcom", "_ingestion_date"]:
        if c in out.columns:
            out[c] = _to_date_series(out[c])

    if "_ingestion_ts_utc" in out.columns:
        out["_ingestion_ts_utc"] = pd.to_datetime(out["_ingestion_ts_utc"], errors="coerce")

    # coerção final pelo schema
    integer_types = {"smallint", "integer", "bigint"}
    datetime_types = {"date", "timestamp without time zone", "timestamp with time zone"}
    numeric_types = {"numeric", "double precision", "real", "decimal"}

    for col, data_type in table_schema:
        c = col.lower()
        if c not in out.columns or c == "created_at":
            continue

        if data_type in integer_types:
            out[c] = _to_int_series(out[c])
        elif data_type in datetime_types:
            if data_type == "date":
                out[c] = _to_date_series(out[c])
            else:
                out[c] = pd.to_datetime(out[c], errors="coerce")
        elif data_type in numeric_types:
            out[c] = _to_numeric_series(out[c])
        else:
            out[c] = out[c].apply(_clean_str)

    # mantém só colunas do destino (sem created_at)
    target_cols = [c.lower() for c, _ in table_schema if c.lower() in out.columns and c.lower() != "created_at"]
    out = out[target_cols]

    # PK válida
    out = out.dropna(subset=["codprod"])
    out["codprod"] = out["codprod"].astype("int64")

    # dedup por PK
    out = out.drop_duplicates(subset=["codprod"], keep="last")

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
    update_cols = [c for c in columns if c != "codprod"]

    insert_sql = f"""
        INSERT INTO {TARGET_TABLE} ({', '.join(columns)})
        VALUES %s
        ON CONFLICT (codprod) DO UPDATE SET
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
        ensure_schema_and_table(conn)
        table_schema = get_table_columns(conn)

        silver_df = prepare_dataframe(bronze_df, table_schema)
        loaded = upsert_dataframe(conn, silver_df, chunk_size=CHUNK_SIZE)

        logger.info("✅ Carga concluída em %s (%s linhas upsert)", TARGET_TABLE, loaded)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
