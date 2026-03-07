#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv


# =========================
# CONFIG EDITÁVEL
# =========================
API_KEY = "VEMV5N0MQK4GS2B79HFKR8QXDIUN3QZTIOCGTR5WIFRD964KWQE5D3SD0AAT0LNGTOBW7VZ32FYPQKBBDV3Q3C7QFP43C1DHASNR8PIGFUB7YGR28RF3LXA0OYGOA2N4"
BASE_URL = "https://app.zuq.com.br"
ENDPOINT = "/api/report/vehicle/daily"  # Power Query: "api/report/vehicle/daily?"
PAGE_SIZE = 500
MAX_PAGES = 1000

# destino no MinIO
MINIO_S3_PATH = "s3://datalake-sost/api/zuq/zuq_report_vehicle_daily/zuq_report_vehicle_daily.parquet"

# ExpandRecordColumn(Fonte, "row", {...})
ROW_FIELDS = [
    "date", "vehicle", "headDuration", "pointCount", "powerFailCount",
    "ignitionOn", "ignitionOff", "movingTime", "stoppedTime", "stoppedAndIgnition",
    "maxSpeed", "meanSpeed", "startOdometer", "endOdometer",
    "startOperation", "endOperation",
    "speedrange1", "speedrange2", "speedrange3", "speedrange4", "speedrange5",
    "input1", "input2",
]

# Tipos (Power Query: type number / type date)
NUMERIC_COLS = [
    "pointCount", "powerFailCount", "ignitionOn", "ignitionOff",
    "movingTime", "stoppedTime", "stoppedAndIgnition",
    "maxSpeed", "meanSpeed",
    "startOdometer", "endOdometer",
    "startOperation", "endOperation",
    "speedrange1", "speedrange2", "speedrange3", "speedrange4", "speedrange5",
    "input1", "input2",
    "headDuration",
]
DATE_COLS = ["date"]

# RenameColumns
RENAME_MAP = {
    "id": "ID_VEICULO",
    "licensePlate": "PLACA",
    "code": "VEICULO",
}


# =========================
# LOGGING
# =========================
logger = logging.getLogger("zuq_report_vehicle_daily")
if not logger.handlers:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )


# =========================
# MINIO (via s3fs)
# =========================
def load_minio_config(env_path: str | None = None) -> Dict[str, str]:
    if env_path is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(base_dir, "../../.env")

    load_dotenv(env_path)

    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "bucket": os.getenv("MINIO_BUCKET_RAW", "datalake-sost"),
        "access_key": os.getenv("MINIO_ROOT_USER", ""),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD", ""),
        "region": os.getenv("MINIO_REGION", "us-east-1"),
    }


def minio_fs(cfg: Dict[str, str]) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=cfg["access_key"],
        secret=cfg["secret_key"],
        client_kwargs={
            "endpoint_url": cfg["endpoint"],
            "region_name": cfg["region"],
        },
    )


def upload_parquet_to_minio(df: pd.DataFrame, fs: s3fs.S3FileSystem, s3_path: str) -> None:
    ingestion_dt = datetime.now(timezone.utc)
    df_out = df.copy()
    df_out["_ingestion_ts_utc"] = ingestion_dt.isoformat()
    df_out["_ingestion_date"] = ingestion_dt.date().isoformat()

    table = pa.Table.from_pandas(df_out, preserve_index=False)

    with fs.open(s3_path, "wb") as f:
        pq.write_table(
            table,
            f,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True,
        )

    logger.info("✅ Parquet salvo no MinIO: %s (linhas=%s)", s3_path, len(df_out))


# =========================
# API + PAGINAÇÃO (fnZuqGetPaged equivalente)
# =========================
def _extract_records(payload: Any) -> List[Dict[str, Any]]:
    if payload is None:
        return []
    if isinstance(payload, list):
        return [x for x in payload if isinstance(x, dict)]
    if not isinstance(payload, dict):
        return []

    for key in ("content", "data", "results", "rows", "items"):
        v = payload.get(key)
        if isinstance(v, list):
            return [x for x in v if isinstance(x, dict)]

    for _, v in payload.items():
        if isinstance(v, list) and all(isinstance(x, dict) for x in v):
            return v

    return []


def zuq_get_paged(
    base_url: str,
    endpoint: str,
    api_key: str,
    page_size: int = 500,
    max_pages: int = 1000,
    extra_params: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    url = f"{base_url.rstrip('/')}/{endpoint.lstrip('/')}"
    headers = {"Authorization": api_key}

    # mesmo padrão (page/pageSize). Se esse endpoint não paginar, ele ainda funciona:
    params = {"page": 1, "pageSize": page_size}
    if extra_params:
        params.update(extra_params)

    all_rows: List[Dict[str, Any]] = []
    page = 1

    while True:
        params["page"] = page
        params["pageSize"] = page_size

        r = requests.get(url, headers=headers, params=params, timeout=60)
        if r.status_code != 200:
            raise RuntimeError(f"Erro HTTP {r.status_code} na página {page}: {r.text}")

        data = r.json()
        records = _extract_records(data)

        if not records:
            logger.info("✅ API retornou 0 registros — fim (page=%s).", page)
            break

        all_rows.extend(records)
        logger.info("📄 Página %s carregada (%s registros).", page, len(records))

        if len(records) < page_size:
            logger.info("✅ Fim. Última página com %s registros.", len(records))
            break

        page += 1
        if page > max_pages:
            logger.warning("🚨 Limite de %s páginas atingido. Encerrando.", max_pages)
            break

    return all_rows


# =========================
# TRANSFORMAÇÕES (Power Query -> Python)
# =========================
def transform_vehicle_daily_report(raw_records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Power Query:
    - Expand row (ROW_FIELDS)
    - Expand vehicle (id, code, licensePlate)
    - Tipar números + date
    - Renomear colunas: id->ID_VEICULO, code->VEICULO, licensePlate->PLACA
    """
    if not raw_records:
        return pd.DataFrame()

    # normaliza "row"
    base_rows: List[Dict[str, Any]] = []
    for rec in raw_records:
        if isinstance(rec, dict) and isinstance(rec.get("row"), dict):
            base_rows.append(rec["row"])
        else:
            base_rows.append(rec if isinstance(rec, dict) else {})

    # seleciona campos do row
    normalized: List[Dict[str, Any]] = []
    for row in base_rows:
        item = {k: row.get(k) for k in ROW_FIELDS}
        for k in ROW_FIELDS:
            item.setdefault(k, None)
        normalized.append(item)

    df = pd.DataFrame(normalized)

    # expand vehicle (id, code, licensePlate) com nomes originais como no PQ
    if "vehicle" in df.columns:
        df["id"] = df["vehicle"].apply(lambda d: d.get("id") if isinstance(d, dict) else None)
        df["code"] = df["vehicle"].apply(lambda d: d.get("code") if isinstance(d, dict) else None)
        df["licensePlate"] = df["vehicle"].apply(lambda d: d.get("licensePlate") if isinstance(d, dict) else None)
        df.drop(columns=["vehicle"], inplace=True)

    # tipos: numbers
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # tipos: date
    for c in DATE_COLS:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date

    # garantir texto nos campos vehicle (opcional)
    for c in ("id", "code", "licensePlate"):
        if c in df.columns:
            df[c] = df[c].astype("string")

    # renomear como no PQ
    df.rename(columns=RENAME_MAP, inplace=True)

    return df


def main() -> None:
    logger.info("Iniciando extração ZUQ Vehicle Daily Report: %s%s", BASE_URL, ENDPOINT)

    raw = zuq_get_paged(
        base_url=BASE_URL,
        endpoint=ENDPOINT,
        api_key=API_KEY,
        page_size=PAGE_SIZE,
        max_pages=MAX_PAGES,
        extra_params=None,
    )

    if not raw:
        logger.warning("⚠️ Nenhum dado retornado pela API.")
        return

    df = transform_vehicle_daily_report(raw)
    logger.info("📊 DataFrame final: linhas=%s colunas=%s", len(df), len(df.columns))

    minio_cfg = load_minio_config()
    if not minio_cfg["access_key"] or not minio_cfg["secret_key"]:
        raise RuntimeError("Credenciais do MinIO não configuradas no .env (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD).")

    fs = minio_fs(minio_cfg)

    # Se quiser forçar bucket do .env:
    # s3_path = f"s3://{minio_cfg['bucket']}/api/zuq/zuq_report_vehicle_daily/zuq_report_vehicle_daily.parquet"
    upload_parquet_to_minio(df, fs, MINIO_S3_PATH)

    logger.info("✅ Processo concluído.")


if __name__ == "__main__":
    main()