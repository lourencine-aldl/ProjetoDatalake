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
ENDPOINT = "/api/maintenance/reminder/list"  # Power Query: "api/maintenance/reminder/list?"
PAGE_SIZE = 500
MAX_PAGES = 1000

# destino no MinIO
MINIO_S3_PATH = "s3://datalake-sost/api/zuq/zuq_maintenance_reminder_list/zuq_maintenance_reminder_list.parquet"

# ExpandRecordColumn(Fonte, "row", {...})
ROW_FIELDS = ["id", "name", "emailCSV", "creationInstant", "closed", "vehicles", "reminderItems"]

# vehicles (list -> record)
VEHICLE_FIELDS = [
    "id", "vehicleType", "make", "model", "licensePlate", "code",
    "assembleYear", "subsidiary", "vehicleStatus", "tankSize",
    "kmPerLiter", "vehicleTypeOrdinal",
]

# reminderItems (list -> record)
REMINDER_ITEM_FIELDS = [
    "id", "payable", "reminderid", "odometerGap", "hourmeterGap",
    "intervalGap", "intervalType", "odometerThreshold",
    "hourmeterThreshold", "timeThreshold", "thresholdIntervalType",
]

# payable (record)
PAYABLE_FIELDS = ["id", "code", "name", "unit", "manufacturer", "payableType", "responsible", "documentNumber"]

# Tipos numéricos (Power Query: type number)
NUMERIC_COLS = [
    "odometerThreshold", "intervalType", "hourmeterThreshold", "timeThreshold",
    "thresholdIntervalType", "intervalGap", "odometerGap", "hourmeterGap",
]

# Renomes finais (equivalente aos dois RenameColumns do PQ)
RENAME_MAP = {
    # rename 1
    "name": "NOME_VEICULO",
    "id": "ID",
    "code": "PLACA",                 # do veículo
    "model": "MARCA/MODELO",
    "make": "MODELO",
    "name.1": "TIPO_MANUTENCAO",     # do payable
    "unit": "UNIT",

    # rename 2
    "intervalGap": "INTERVALO_GAP",
    "odometerGap": "ODEMETRO_GAP",
    "id.1": "ID_VEICULO",
    "vehicleStatus": "STATUS_VEICULO",
}

# ReorderColumns (se alguma não existir, será ignorada)
REORDER_COLUMNS = [
    "ID", "NOME_VEICULO", "emailCSV", "creationInstant", "closed",
    "ID_VEICULO", "vehicleType", "MODELO", "MARCA/MODELO", "licensePlate", "PLACA",
    "assembleYear", "subsidiary", "STATUS_VEICULO", "tankSize", "kmPerLiter", "vehicleTypeOrdinal",
    "id.2", "id.3", "code.1", "TIPO_MANUTENCAO", "UNIT", "manufacturer", "payableType",
    "responsible", "documentNumber", "reminderid", "INTERVALO_GAP", "ODEMETRO_GAP", "hourmeterGap",
    "intervalType", "odometerThreshold", "hourmeterThreshold", "timeThreshold", "thresholdIntervalType",
]


# =========================
# LOGGING
# =========================
logger = logging.getLogger("zuq_maintenance_reminder_list")
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
        "bucket": os.getenv("MINIO_BUCKET_RAW", "raw-zone"),
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
            logger.info("✅ API retornou 0 registros — fim da paginação (page=%s).", page)
            break

        all_rows.extend(records)
        logger.info("📄 Página %s carregada (%s registros).", page, len(records))

        if len(records) < page_size:
            logger.info("✅ Fim da paginação. Última página com %s registros.", len(records))
            break

        page += 1
        if page > max_pages:
            logger.warning("🚨 Limite de %s páginas atingido. Encerrando.", max_pages)
            break

    return all_rows


# =========================
# TRANSFORMAÇÕES (Power Query -> Python)
# =========================
def transform_maintenance_reminder(raw_records: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Replica:
    - Expand row (id, name, emailCSV, creationInstant, closed, vehicles, reminderItems)
    - ExpandList vehicles; ExpandRecord vehicles (id->id.1, ...)
    - ExpandList reminderItems; ExpandRecord reminderItems (id->id.2, ...)
    - ExpandRecord payable (id->id.3, code->code.1, name->name.1, ...)
    - Types numbers
    - Rename columns
    - Reorder columns
    """
    if not raw_records:
        return pd.DataFrame()

    # normaliza "row"
    rows: List[Dict[str, Any]] = []
    for rec in raw_records:
        if isinstance(rec, dict) and isinstance(rec.get("row"), dict):
            rows.append(rec["row"])
        else:
            rows.append(rec if isinstance(rec, dict) else {})

    # base df com os campos do row
    base_list: List[Dict[str, Any]] = []
    for row in rows:
        item = {k: row.get(k) for k in ROW_FIELDS}
        for k in ROW_FIELDS:
            item.setdefault(k, None)
        base_list.append(item)

    df = pd.DataFrame(base_list)

    # ExpandList vehicles (explode)
    if "vehicles" not in df.columns:
        df["vehicles"] = None
    df = df.explode("vehicles", ignore_index=True)

    # ExpandRecord vehicles -> cria colunas do veículo (id -> id.1 para bater com PQ)
    if "vehicles" in df.columns:
        df["id.1"] = df["vehicles"].apply(lambda d: d.get("id") if isinstance(d, dict) else None)
        for k in VEHICLE_FIELDS:
            if k == "id":
                continue
            df[k] = df["vehicles"].apply(lambda d, kk=k: d.get(kk) if isinstance(d, dict) else None)
        df.drop(columns=["vehicles"], inplace=True)

    # ExpandList reminderItems (explode)
    if "reminderItems" not in df.columns:
        df["reminderItems"] = None
    df = df.explode("reminderItems", ignore_index=True)

    # ExpandRecord reminderItems -> cria colunas (id -> id.2)
    if "reminderItems" in df.columns:
        df["id.2"] = df["reminderItems"].apply(lambda d: d.get("id") if isinstance(d, dict) else None)
        for k in REMINDER_ITEM_FIELDS:
            if k == "id":
                continue
            df[k] = df["reminderItems"].apply(lambda d, kk=k: d.get(kk) if isinstance(d, dict) else None)
        df.drop(columns=["reminderItems"], inplace=True)

    # ExpandRecord payable -> id.3, code.1, name.1, ...
    if "payable" in df.columns:
        df["id.3"] = df["payable"].apply(lambda d: d.get("id") if isinstance(d, dict) else None)
        df["code.1"] = df["payable"].apply(lambda d: d.get("code") if isinstance(d, dict) else None)
        df["name.1"] = df["payable"].apply(lambda d: d.get("name") if isinstance(d, dict) else None)
        for k in PAYABLE_FIELDS:
            if k in ("id", "code", "name"):
                continue
            df[k] = df["payable"].apply(lambda d, kk=k: d.get(kk) if isinstance(d, dict) else None)
        df.drop(columns=["payable"], inplace=True)

    # tipos numéricos
    for c in NUMERIC_COLS:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # renomear (equivalente aos 2 RenameColumns)
    df.rename(columns=RENAME_MAP, inplace=True)

    # reorder (ignora faltantes)
    existing = [c for c in REORDER_COLUMNS if c in df.columns]
    rest = [c for c in df.columns if c not in existing]
    df = df[existing + rest]

    return df


def main() -> None:
    logger.info("Iniciando extração ZUQ Maintenance Reminder List: %s%s", BASE_URL, ENDPOINT)

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

    df = transform_maintenance_reminder(raw)
    logger.info("📊 DataFrame final: linhas=%s colunas=%s", len(df), len(df.columns))

    minio_cfg = load_minio_config()
    if not minio_cfg["access_key"] or not minio_cfg["secret_key"]:
        raise RuntimeError("Credenciais do MinIO não configuradas no .env (MINIO_ROOT_USER / MINIO_ROOT_PASSWORD).")

    fs = minio_fs(minio_cfg)

    # Se quiser forçar bucket do .env:
    # s3_path = f"s3://{minio_cfg['bucket']}/api/zuq/zuq_maintenance_reminder_list/zuq_maintenance_reminder_list.parquet"
    upload_parquet_to_minio(df, fs, MINIO_S3_PATH)

    logger.info("✅ Processo concluído.")


if __name__ == "__main__":
    main()