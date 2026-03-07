import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime, timezone
import os
import glob
from dotenv import load_dotenv
import s3fs

# =========================
# Config MinIO (via s3fs)
# =========================
def load_minio_config(env_path: str = None):
    # Buscar .env na pasta do projeto
    if env_path is None:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        env_path = os.path.join(base_dir, "../../.env")
    
    load_dotenv(env_path)
    
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        "bucket": os.getenv("MINIO_BUCKET_RAW", "raw-zone"),
        "access_key": os.getenv("MINIO_ROOT_USER"),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD"),
        "region": os.getenv("MINIO_REGION", "us-east-1"),
    }

def minio_fs(cfg: dict) -> s3fs.S3FileSystem:
    return s3fs.S3FileSystem(
        key=cfg["access_key"],
        secret=cfg["secret_key"],
        client_kwargs={
            "endpoint_url": cfg["endpoint"],
            "region_name": cfg["region"],
        },
    )

def upload_parquet_to_minio(df: pd.DataFrame, fs: s3fs.S3FileSystem, s3_path: str) -> None:
    """Salva parquet diretamente no MinIO com metadata de ingestão"""
    # Adicionar metadata de ingestão
    ingestion_dt = datetime.now(timezone.utc)
    df_copy = df.copy()
    df_copy["_ingestion_ts_utc"] = ingestion_dt.isoformat()
    df_copy["_ingestion_date"] = ingestion_dt.date().isoformat()
    
    # Converter para PyArrow Table
    table = pa.Table.from_pandas(df_copy, preserve_index=False)
    
    # Upload para MinIO
    with fs.open(s3_path, "wb") as f:
        pq.write_table(
            table,
            f,
            compression="snappy",
            use_dictionary=True,
            write_statistics=True
        )
    
    print(f"✅ Parquet salvo no MinIO: {s3_path}")


# --- Configurações API ---
api_key = "VEMV5N0MQK4GS2B79HFKR8QXDIUN3QZTIOCGTR5WIFRD964KWQE5D3SD0AAT0LNGTOBW7VZ32FYPQKBBDV3Q3C7QFP43C1DHASNR8PIGFUB7YGR28RF3LXA0OYGOA2N4"
url = "https://app.zuq.com.br/api/driver/workjourney/v2/search"
headers = {"Authorization": api_key}

payload = {"page": 1, "pageSize": 500}
todos = []
pagina = 1
pageSize = 500

# --- Paginação ---
while True:
    payload["page"] = pagina
    payload["pageSize"] = pageSize

    r = requests.get(url, headers=headers, params=payload)
    if r.status_code != 200:
        print(f"❌ Erro na página {pagina}: {r.status_code} -> {r.text}")
        break

    dados = r.json()
    registros = dados.get("content") or dados.get("data") or dados.get("results") or dados

    if isinstance(registros, dict):
        registros = list(registros.values())

    if not registros:
        print("✅ API retornou 0 registros — fim da paginação.")
        break

    todos.extend(registros)
    print(f"📄 Página {pagina} carregada ({len(registros)} registros).")

    if len(registros) < pageSize:
        print(f"✅ Fim da paginação. Última página com {len(registros)} registros.")
        break

    pagina += 1

    if pagina > 1000:
        print("🚨 Limite de 1000 páginas atingido. Encerrando execução.")
        break

# --- Final da coleta ---
if not todos:
    print("⚠️ Nenhum dado retornado pela API.")
    raise SystemExit(0)

print(f"📊 Total de registros coletados: {len(todos):,}")

# --- DataFrame ---
df = pd.DataFrame(todos)

# --- Expansão de campos aninhados ---
if "driver" in df.columns:
    df["motorista_id"] = df["driver"].apply(lambda d: d.get("id") if isinstance(d, dict) else None)
    df["motorista_nome"] = df["driver"].apply(lambda d: d.get("name") if isinstance(d, dict) else None)
    df.drop(columns=["driver"], inplace=True)

if "vehicle" in df.columns:
    df["veiculo_id"] = df["vehicle"].apply(lambda v: v.get("id") if isinstance(v, dict) else None)
    df["veiculo_codigo"] = df["vehicle"].apply(lambda v: v.get("code") if isinstance(v, dict) else None)
    df.drop(columns=["vehicle"], inplace=True)

# --- Conversões e cálculos adicionais ---
df["entrada"] = pd.to_datetime(df.get("enterTime"), errors="coerce")
df["saida"] = pd.to_datetime(df.get("leaveTime"), errors="coerce")
df["duracao_minutos"] = df.get("duration", 0).astype(float) / 60
df["distancia_km"] = df.get("distance", 0).astype(float) / 1000

# --- Upload direto para MinIO (sem salvamento local) ---
try:
    print(f"📊 Total de registros coletados: {len(df):,}")

    # Upload para MinIO via s3fs
    minio_cfg = load_minio_config()
    if minio_cfg["access_key"] and minio_cfg["secret_key"]:
        try:
            fs = minio_fs(minio_cfg)
            
            # Upload only latest (sobrescreve sempre)
            s3_path = f"s3://{minio_cfg['bucket']}/api/zuq/zuq_driver_workjourney/zuq_driver_workjourney.parquet"
            upload_parquet_to_minio(df, fs, s3_path)
            
            print("✅ Upload para MinIO concluído (Parquet + Metadata)")
        except Exception as e:
            print(f"❌ Erro ao conectar ao MinIO: {e}")
    else:
        print("⚠️ Credenciais MinIO não configuradas no .env.")

except Exception as e:
    print(f"❌ Erro ao salvar: {e}")
