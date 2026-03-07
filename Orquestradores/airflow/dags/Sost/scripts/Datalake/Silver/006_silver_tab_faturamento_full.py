#!/usr/bin/env python3
"""
Silver Layer ETL: TAB_FATURAMENTO_FULL MinIO (Bronze) -> PostgreSQL DW (public.silver_tab_faturamento_full)

✅ Tipos definidos pela amostra (pt-BR, notação científica).
✅ SNAPSHOT: TRUNCATE + INSERT em batches.
✅ Lê: s3://<bucket>/bronze/006_bronze_tab_faturamento_full/*.parquet
✅ Garante CREATE SCHEMA + TABLE antes de carregar (modelo PCUSUARIO).
✅ NORMALIZA nomes de colunas (snake_case) + aliases para bater com o DDL.
✅ FILTRA linhas inválidas (PK nula / linha quase toda nula) antes do insert.
"""

import io
import os
import re
import logging
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from typing import List, Optional, Tuple, Dict

import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv


# ===========================
# SETTINGS EDITÁVEIS
# ===========================
LOGGER_NAME = "silver_tab_faturamento_full"

BRONZE_PREFIX = "bronze/006_bronze_tab_faturamento_full"
TARGET_TABLE = "public.silver_tab_faturamento_full"
CHUNK_SIZE = 5000


# ===========================
# LOGGING
# ===========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
# HELPERS
# ===========================
def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def normalize_colname(name: str) -> str:
    """
    Normaliza colunas para snake_case ascii:
    - remove acentos
    - troca espaços e separadores por _
    - remove chars estranhos
    - lower
    """
    s = str(name).strip()
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    s = s.lower()
    s = re.sub(r"[^\w]+", "_", s)      # qualquer coisa não alfanum vira _
    s = re.sub(r"_+", "_", s).strip("_")
    return s


# aliases comuns (ajuste se aparecerem variações)
ALIASES: Dict[str, str] = {
    "datafaturamento": "data_faturamento",
    "data_faturamento_": "data_faturamento",
    "data_faturamento__": "data_faturamento",
    "data_faturamento": "data_faturamento",
    "data_faturamento_0": "data_faturamento",
    "data_faturamento_00": "data_faturamento",
    "data_de_faturamento": "data_faturamento",
    "datafat": "data_faturamento",
    "dt_faturamento": "data_faturamento",
    "cod_filial": "codfilial",
    "cod_filial_": "codfilial",
    "codfilial": "codfilial",
    "num_nota": "numnota",
    "numnota": "numnota",
    "cod_cli": "codcli",
    "cod_cliente": "codcli",
    "codcli": "codcli",
    "valor_liquido": "valor_liquido",
}


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


def read_parquet(fs: s3fs.S3FileSystem, path_no_scheme: str) -> pd.DataFrame:
    s3_uri = f"s3://{path_no_scheme}"
    logger.info("📥 Lendo %s", s3_uri)
    with fs.open(s3_uri, "rb") as f:
        table = pq.read_table(io.BytesIO(f.read()))
        df = table.to_pandas()
    logger.info("📊 Linhas lidas do arquivo: %s", len(df))
    logger.info("📋 Colunas do arquivo: %s", list(df.columns)[:20])
    return df


# ===========================
# CONVERSÕES ROBUSTAS (pt-BR)
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

    if _RE_SCI.match(s) and "," in s:
        s = s.replace(".", "").replace(",", ".")
    else:
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        else:
            if s.count(".") >= 2:
                s = s.replace(".", "")

    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def _to_int_series(series: pd.Series) -> pd.Series:
    vals = series.apply(_to_decimal_maybe)
    out: List[object] = []
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


def _to_ts_series(series: pd.Series) -> pd.Series:
    return pd.to_datetime(series, errors="coerce")


# ===========================
# DDL DESTINO (MODELO PCUSUARIO)
# ===========================
def ensure_schema_and_table(conn) -> None:
    schema, _ = TARGET_TABLE.split(".", 1)

    sql = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};

    CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
        codfilial          INTEGER NOT NULL,
        data_faturamento   TIMESTAMP NOT NULL,
        mes                SMALLINT,
        ano                INTEGER,
        codcli             INTEGER,
        cliente            TEXT,
        codativprinc       NUMERIC(18,4),
        ramo               TEXT,
        ramoprincipal      TEXT,
        tiporamo           TEXT,
        tipovenda          TEXT,
        codigo             BIGINT,
        produto            TEXT,
        embalagem          TEXT,
        litragem           TEXT,

        qtd                NUMERIC(18,6),
        qtdevol            NUMERIC(18,6),

        peso               NUMERIC(18,6),
        pesoliq            NUMERIC(18,6),

        valortotal         NUMERIC(18,6),
        ptabela            NUMERIC(18,6),
        valor_liquido      NUMERIC(18,6),

        codfornec          BIGINT,
        fornecedor         TEXT,

        codsupervisor      INTEGER,
        supervisor         TEXT,

        codusur            INTEGER,
        nomerca            TEXT,

        codsec             INTEGER,
        secao              TEXT,

        contrato           TEXT,
        codicmtab          NUMERIC(18,6),

        custofinest        NUMERIC(18,6),
        custoreal          NUMERIC(18,6),

        perc_lucro         NUMERIC(18,6),
        valorbonificado    NUMERIC(18,6),
        vldevolucao        NUMERIC(18,6),

        codcategoria       BIGINT,
        categoria          TEXT,
        codsubcategoria    BIGINT,
        subcategoria       TEXT,

        percdescdsv        NUMERIC(18,6),
        valor_dsv          NUMERIC(18,6),
        valor_dsv_2        NUMERIC(18,6),

        codativ            NUMERIC(18,4),

        numnota            BIGINT,
        numped             NUMERIC(38,0),
        numnotadev         BIGINT,

        percdescfin        NUMERIC(18,6),
        horafat            INTEGER,
        minutofat          INTEGER,
        codplpag           NUMERIC(38,0),

        _ingestion_ts_utc  TIMESTAMP,
        _ingestion_date    DATE,
        _silver_ts_utc     TIMESTAMP,
        created_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

        PRIMARY KEY (codfilial, data_faturamento, numnota, codigo, codcli)
    );

    CREATE INDEX IF NOT EXISTS idx_silver_tab_faturamento_full_ingestion_date
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


def get_table_schema(conn) -> List[Tuple[str, str]]:
    schema, table = TARGET_TABLE.split(".", 1)
    sql = """
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = %s AND table_name = %s
    ORDER BY ordinal_position
    """
    with conn.cursor() as cur:
        cur.execute(sql, (schema, table))
        return [(r[0], r[1]) for r in cur.fetchall()]


# ===========================
# PREP DF
# ===========================
REQUIRED_PK = ["codfilial", "data_faturamento", "numnota", "codigo", "codcli"]


def normalize_and_alias_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    mapping: Dict[str, str] = {}
    for c in out.columns:
        nc = normalize_colname(c)
        nc = ALIASES.get(nc, nc)
        mapping[c] = nc
    out = out.rename(columns=mapping)

    # se houver duplicadas após normalize, mantém a primeira não-nula (combina)
    if out.columns.duplicated().any():
        cols = list(out.columns)
        dup = [c for c in set(cols) if cols.count(c) > 1]
        for c in dup:
            sub = out.loc[:, [col for col in out.columns if col == c]]
            combined = sub.bfill(axis=1).iloc[:, 0]
            out = out.drop(columns=[col for col in out.columns if col == c])
            out[c] = combined

    return out


def _ensure_ingestion_cols(out: pd.DataFrame) -> pd.DataFrame:
    out = out.copy()
    ts = datetime.now(timezone.utc)
    if "_ingestion_ts_utc" not in out.columns:
        out["_ingestion_ts_utc"] = ts
    if "_ingestion_date" not in out.columns:
        out["_ingestion_date"] = ts.date()
    out["_silver_ts_utc"] = ts
    return out


def drop_invalid_rows(out: pd.DataFrame) -> pd.DataFrame:
    """
    Remove:
    - linhas sem PK
    - linhas "quase vazias" (ex.: tudo None exceto ingestion)
    """
    before = len(out)

    # garante que PK existe como coluna; se não existir, falha com debug
    missing_pk_cols = [c for c in REQUIRED_PK if c not in out.columns]
    if missing_pk_cols:
        raise ValueError(
            "Colunas PK esperadas não encontradas após normalização.\n"
            f"Faltando: {missing_pk_cols}\n"
            f"Disponíveis (amostra): {sorted(list(out.columns))[:80]}"
        )

    out = out.dropna(subset=REQUIRED_PK)
    after_pk_drop = len(out)
    pk_dropped = before - after_pk_drop
    if pk_dropped > 0:
        logger.warning("🧽 Linhas descartadas por PK nula: %s (%.1f%%)", pk_dropped, 100*pk_dropped/before if before > 0 else 0)

    # remove linhas onde todos os campos "de dados" estão nulos
    meta_cols = {"_ingestion_ts_utc", "_ingestion_date", "_silver_ts_utc", "created_at"}
    data_cols = [c for c in out.columns if c not in meta_cols]
    if data_cols:
        all_null_data = out[data_cols].isna().all(axis=1)
        out = out.loc[~all_null_data].copy()

    after = len(out)
    empty_dropped = after_pk_drop - after
    if empty_dropped > 0:
        logger.warning("🧽 Linhas descartadas por estar vazias: %s", empty_dropped)
    
    if after == 0:
        logger.error("❌ ERRO CRÍTICO: Nenhuma linha restou após filtros! antes=%s, pk_nula=%s, vazias=%s", before, pk_dropped, empty_dropped)

    return out


def prepare_dataframe(df: pd.DataFrame, table_schema: List[Tuple[str, str]]) -> pd.DataFrame:
    out = normalize_and_alias_columns(df)
    logger.info("📊 Após normalizar colunas: %s linhas", len(out))
    out = _ensure_ingestion_cols(out)

    # timestamps
    if "data_faturamento" in out.columns:
        out["data_faturamento"] = _to_ts_series(out["data_faturamento"])
    if "_ingestion_ts_utc" in out.columns:
        out["_ingestion_ts_utc"] = _to_ts_series(out["_ingestion_ts_utc"])
    if "_ingestion_date" in out.columns:
        out["_ingestion_date"] = pd.to_datetime(out["_ingestion_date"], errors="coerce").dt.date

    # strings (limpa)
    for c in [
        "cliente", "ramo", "ramoprincipal", "tiporamo", "tipovenda", "produto", "embalagem", "litragem",
        "fornecedor", "supervisor", "nomerca", "secao", "contrato", "categoria", "subcategoria"
    ]:
        if c in out.columns:
            out[c] = out[c].apply(_clean_str)

    # inteiros
    for c in [
        "codfilial", "mes", "ano", "codcli", "codigo", "codfornec", "codsupervisor", "codusur", "codsec",
        "codcategoria", "codsubcategoria", "numnota", "numnotadev", "horafat", "minutofat"
    ]:
        if c in out.columns:
            out[c] = _to_int_series(out[c])

    # NUMERIC(38,0)
    for c in ["numped", "codplpag"]:
        if c in out.columns:
            out[c] = out[c].apply(_to_decimal_maybe)

    # numéricos
    for c in [
        "codativprinc", "qtd", "qtdevol", "peso", "pesoliq", "valortotal", "ptabela", "codicmtab",
        "custofinest", "custoreal", "perc_lucro", "valorbonificado", "vldevolucao", "valor_liquido",
        "percdescdsv", "valor_dsv", "valor_dsv_2", "codativ", "percdescfin"
    ]:
        if c in out.columns:
            out[c] = _to_numeric_series(out[c])

    # coerção final pelo schema
    int_types = {"smallint", "integer", "bigint"}
    ts_types = {"timestamp without time zone", "timestamp with time zone", "date"}
    num_types = {"numeric", "real", "double precision", "decimal"}

    for col, data_type in table_schema:
        col_l = col.lower()
        if col_l not in out.columns or col_l == "created_at":
            continue

        if data_type in int_types:
            out[col_l] = _to_int_series(out[col_l])
        elif data_type in num_types:
            out[col_l] = _to_numeric_series(out[col_l]) if out[col_l].dtype != object else out[col_l].apply(_to_decimal_maybe)
        elif data_type in ts_types:
            if data_type == "date":
                out[col_l] = pd.to_datetime(out[col_l], errors="coerce").dt.date
            else:
                out[col_l] = _to_ts_series(out[col_l])
        else:
            out[col_l] = out[col_l].apply(_clean_str)

    # mantém só colunas do destino (sem created_at)
    target_cols = [c.lower() for c, _ in table_schema if c.lower() in out.columns and c.lower() != "created_at"]
    out = out[target_cols]

    # filtra linhas inválidas (PK nula / linha vazia)
    logger.info("📊 Antes de filtros: %s linhas, PK disponíveis: %s", len(out), [c for c in REQUIRED_PK if c in out.columns])
    out = drop_invalid_rows(out)
    logger.info("📊 Após filtros: %s linhas", len(out))

    # NaN/NaT -> None
    out = out.astype(object).where(pd.notnull(out), None)
    return out


# ===========================
# LOAD (TRUNCATE + INSERT)
# ===========================
def truncate_and_insert(conn, df: pd.DataFrame, chunk_size: int = CHUNK_SIZE) -> int:
    if df.empty:
        logger.info("⚠️ DataFrame vazio, nada para carregar.")
        logger.error("❌ ERRO: DataFrame vazio! Nenhuma linha será inserida após truncate.")
        return 0

    columns = list(df.columns)
    quoted_cols = ", ".join(qident(c) for c in columns)
    insert_sql = f"INSERT INTO {TARGET_TABLE} ({quoted_cols}) VALUES %s"

    rows = [tuple(row) for row in df.itertuples(index=False, name=None)]
    total = len(rows)

    with conn.cursor() as cur:
        logger.info("🧹 TRUNCATE %s", TARGET_TABLE)
        cur.execute(f"TRUNCATE TABLE {TARGET_TABLE}")

        for i in range(0, total, chunk_size):
            batch = rows[i:i + chunk_size]
            psycopg2.extras.execute_values(cur, insert_sql, batch, page_size=chunk_size)

    conn.commit()
    return total


def insert_append(conn, df: pd.DataFrame, chunk_size: int = CHUNK_SIZE) -> int:
    if df.empty:
        return 0

    columns = list(df.columns)
    quoted_cols = ", ".join(qident(c) for c in columns)
    # ON CONFLICT DO NOTHING para ignorar duplicatas
    insert_sql = f"INSERT INTO {TARGET_TABLE} ({quoted_cols}) VALUES %s ON CONFLICT DO NOTHING"

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

    conn = pg_connection(pg_cfg)
    try:
        ensure_schema_and_table(conn)
        table_schema = get_table_schema(conn)

        total_loaded = 0
        first = True

        for path_no_scheme in files:
            bronze_df = read_parquet(fs, path_no_scheme)

            # debug opcional de colunas (1a vez)
            if first:
                cols_norm = [normalize_colname(c) for c in bronze_df.columns]
                logger.info("🧾 Colunas (amostra normalizada): %s", cols_norm[:60])

            silver_df = prepare_dataframe(bronze_df, table_schema)

            if first:
                loaded = truncate_and_insert(conn, silver_df, chunk_size=CHUNK_SIZE)
                first = False
            else:
                loaded = insert_append(conn, silver_df, chunk_size=CHUNK_SIZE)

            total_loaded += loaded
            if loaded > 0:
                logger.info("✅ Arquivo carregado: %s | linhas inseridas=%s", path_no_scheme, loaded)
            else:
                logger.warning("⚠️ Arquivo processado mas 0 linhas inseridas: %s", path_no_scheme)

        logger.info("✅ SNAPSHOT concluído em %s | total linhas inseridas=%s", TARGET_TABLE, total_loaded)
        if total_loaded == 0:
            logger.error("❌ ERRO: Nenhuma linha foi inserida no banco! Verifique e corija o problema acima.", )

    finally:
        conn.close()


if __name__ == "__main__":
    main()