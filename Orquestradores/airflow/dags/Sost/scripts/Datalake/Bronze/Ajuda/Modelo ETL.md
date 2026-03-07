from __future__ import annotations

import os
import math
import logging
from dataclasses import dataclass
from datetime import datetime, date, timezone
from pathlib import Path
from typing import Iterable, Optional, Sequence, Tuple

import psycopg
from psycopg.rows import dict_row
from dotenv import load_dotenv

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


# =========================
# LOGGING (produção)
# =========================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("etl_bronze")


# =========================
# CONFIG
# =========================
@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    dbname: str
    user: str
    password: str
    connect_timeout: int = 10


@dataclass(frozen=True)
class BronzeConfig:
    base_path: Path
    chunk_rows: int = 200_000  # ajuste conforme RAM/volume


@dataclass(frozen=True)
class TableSpec:
    schema: str
    table: str
    # Se quiser filtrar colunas, defina listagem (evita SELECT *)
    columns: Optional[Sequence[str]] = None
    where: Optional[str] = None  # ex: "ativo = true"


def load_configs() -> tuple[PostgresConfig, BronzeConfig]:
    load_dotenv()

    host = os.getenv("POSTGRES_DW_HOST")
    port = int(os.getenv("POSTGRES_DW_PORT", "5432"))
    dbname = os.getenv("POSTGRES_DW_DB")
    user = os.getenv("POSTGRES_DW_USER")
    password = os.getenv("POSTGRES_DW_PASSWORD")

    bronze_base = os.getenv("BRONZE_BASE_PATH", "./datalake/bronze")

    if not all([host, dbname, user, password]):
        raise RuntimeError("Variáveis Postgres incompletas no .env")

    pg = PostgresConfig(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    bronze = BronzeConfig(base_path=Path(bronze_base))
    return pg, bronze


def pg_conn_str(cfg: PostgresConfig) -> str:
    return (
        f"host={cfg.host} port={cfg.port} dbname={cfg.dbname} "
        f"user={cfg.user} password={cfg.password} connect_timeout={cfg.connect_timeout}"
    )


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def build_select_sql(spec: TableSpec) -> str:
    cols = ", ".join([f'"{c}"' for c in spec.columns]) if spec.columns else "*"
    sql = f'SELECT {cols} FROM "{spec.schema}"."{spec.table}"'
    if spec.where:
        sql += f" WHERE {spec.where}"
    return sql


def count_rows(conn: psycopg.Connection, spec: TableSpec) -> int:
    sql = f'SELECT COUNT(*) AS n FROM "{spec.schema}"."{spec.table}"'
    if spec.where:
        sql += f" WHERE {spec.where}"
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(sql)
        return int(cur.fetchone()["n"])


def write_parquet_chunk(
    df: pd.DataFrame,
    out_dir: Path,
    chunk_idx: int,
    ingestion_dt: datetime,
) -> Path:
    # Metadados Bronze (padrão)
    df["_ingestion_ts"] = ingestion_dt.isoformat()
    df["_ingestion_date"] = ingestion_dt.date().isoformat()

    ensure_dir(out_dir)
    file_path = out_dir / f"part-{chunk_idx:05d}.parquet"

    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_table(table, file_path, compression="snappy")

    return file_path


def extract_table_to_bronze(
    conn: psycopg.Connection,
    bronze_cfg: BronzeConfig,
    spec: TableSpec,
    ingestion_dt: datetime,
) -> list[Path]:
    total = count_rows(conn, spec)
    if total == 0:
        logger.warning("Tabela vazia: %s.%s (where=%s)", spec.schema, spec.table, spec.where)
        return []

    chunks = max(1, math.ceil(total / bronze_cfg.chunk_rows))
    logger.info(
        "Extraindo %s.%s | linhas=%s | chunk_rows=%s | chunks=%s",
        spec.schema, spec.table, total, bronze_cfg.chunk_rows, chunks
    )

    ingest_date = ingestion_dt.date().isoformat()
    out_dir = (
        bronze_cfg.base_path
        / spec.schema
        / spec.table
        / f"ingest_date={ingest_date}"
    )

    sql = build_select_sql(spec)
    written: list[Path] = []

    # Server-side cursor para stream (não explode memória)
    with conn.cursor(name=f"cur_{spec.schema}_{spec.table}", row_factory=dict_row) as cur:
        cur.itersize = bronze_cfg.chunk_rows
        cur.execute(sql)

        chunk_idx = 0
        while True:
            rows = cur.fetchmany(bronze_cfg.chunk_rows)
            if not rows:
                break

            df = pd.DataFrame(rows)
            path = write_parquet_chunk(df, out_dir, chunk_idx, ingestion_dt)
            written.append(path)

            logger.info("Gravado: %s | linhas=%s", path, len(df))
            chunk_idx += 1

    logger.info("Concluído %s.%s | arquivos=%s", spec.schema, spec.table, len(written))
    return written


def run_bronze_etl(tables: Iterable[TableSpec]) -> None:
    pg_cfg, bronze_cfg = load_configs()
    ingestion_dt = datetime.now(timezone.utc)

    logger.info("Bronze base path: %s", bronze_cfg.base_path)
    logger.info("Conectando ao Postgres %s:%s/%s", pg_cfg.host, pg_cfg.port, pg_cfg.dbname)

    conn_str = pg_conn_str(pg_cfg)

    with psycopg.connect(conn_str) as conn:
        # dica: só leitura no Bronze; mantém transação “limpa”
        conn.autocommit = True

        for spec in tables:
            extract_table_to_bronze(
                conn=conn,
                bronze_cfg=bronze_cfg,
                spec=spec,
                ingestion_dt=ingestion_dt,
            )


if __name__ == "__main__":
    # ✅ Defina aqui as tabelas que vão para o Bronze
    TABLES = [
        TableSpec(schema="public", table="sua_tabela"),
        # Exemplo com colunas selecionadas (evita SELECT *)
        # TableSpec(schema="public", table="clientes", columns=["id", "nome", "email"]),
        # Exemplo com filtro
        # TableSpec(schema="public", table="pagamentos", where="data >= CURRENT_DATE - INTERVAL '30 days'"),
    ]

    run_bronze_etl(TABLES)
