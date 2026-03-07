#!/usr/bin/env python3
"""
Verifica estado atual das pastas Bronze no MinIO
"""
import os
import s3fs
from dotenv import load_dotenv

# Carrega .env
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(BASE_DIR, ".env")
load_dotenv(env_path)

# Configuração MinIO
endpoint = os.getenv("MINIO_ENDPOINT", "").strip()
bucket = os.getenv("MINIO_BUCKET_SOST", "").strip()
access_key = os.getenv("MINIO_ROOT_USER", "").strip()
secret_key = os.getenv("MINIO_ROOT_PASSWORD", "").strip()

print(f"📦 MinIO: {endpoint}")
print(f"📂 Bucket: {bucket}\n")

# Cria filesystem
fs = s3fs.S3FileSystem(
    key=access_key,
    secret=secret_key,
    client_kwargs={"endpoint_url": endpoint},
    use_ssl=False,
    default_fill_cache=False,
)

print("📋 Pastas atuais em bronze/:")
print("=" * 60)

try:
    folders = fs.ls(f"{bucket}/bronze/")
    for folder in sorted(folders):
        folder_name = folder.split("/")[-1]
        try:
            count = len(fs.find(folder))
            print(f"📂 {folder_name:40} ({count:3} arquivos)")
        except:
            print(f"📂 {folder_name:40} (erro ao contar)")
except Exception as e:
    print(f"❌ Erro: {e}")

print("\n" + "=" * 60)
print("✅ Esperado (nova ordem):")
print("=" * 60)
expected = [
    "001_bronze_pcsupervisor",
    "002_bronze_pcusuario", 
    "003_bronze_tab_cliente",
    "004_bronze_pcnfent",
    "005_bronze_pcnfsaid",
    "006_bronze_tab_faturamento_full",
    "007_bronze_tab_movimentacao",
]
for exp in expected:
    print(f"   📂 {exp}")
