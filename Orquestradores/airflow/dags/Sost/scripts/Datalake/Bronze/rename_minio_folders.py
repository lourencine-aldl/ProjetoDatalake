#!/usr/bin/env python3
"""
Script para renomear pastas Bronze no MinIO para a nova ordem
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

print(f"📦 Conectando ao MinIO: {endpoint}")
print(f"📂 Bucket: {bucket}")

# Cria filesystem
fs = s3fs.S3FileSystem(
    key=access_key,
    secret=secret_key,
    client_kwargs={"endpoint_url": endpoint},
    use_ssl=False,
    default_fill_cache=False,
)

# Mapeamento: pasta_antiga -> pasta_nova
renomeacoes = {
    "001_bronze_pcnfsaid": "005_bronze_pcnfsaid",
    "002_bronze_pcnfent": "004_bronze_pcnfent",
    "003_bronze_pcsupervisor": "001_bronze_pcsupervisor",
    "004_bronze_pcusuario": "002_bronze_pcusuario",
    "005_bronze_tab_faturamento_full": "006_bronze_tab_faturamento_full",
    "006_bronze_tab_movimentacao": "007_bronze_tab_movimentacao",
    "007_bronze_tab_cliente": "003_bronze_tab_cliente",
}

print("\n🔄 Iniciando renomeação em 2 fases (evitar conflito)...\n")

# Fase 1: Renomear para nomes temporários
print("=" * 60)
print("FASE 1: Renomeando para nomes temporários")
print("=" * 60)

for old, new in renomeacoes.items():
    old_path = f"{bucket}/bronze/{old}"
    tmp_path = f"{bucket}/bronze/tmp_{old}"
    
    if fs.exists(old_path):
        print(f"📁 {old} → tmp_{old}")
        try:
            # Lista todos os arquivos da pasta antiga
            files = fs.find(old_path)
            if files:
                # Cria pasta temporária e move arquivos
                for file in files:
                    new_file = file.replace(f"bronze/{old}/", f"bronze/tmp_{old}/")
                    fs.copy(file, new_file)
                    fs.rm(file)
                print(f"   ✅ {len(files)} arquivos movidos")
            else:
                print(f"   ⚠️  Pasta vazia")
        except Exception as e:
            print(f"   ❌ Erro: {e}")
    else:
        print(f"⏭️  {old} não existe, pulando...")

# Fase 2: Renomear temporários para nomes finais
print("\n" + "=" * 60)
print("FASE 2: Renomeando para novos nomes finais")
print("=" * 60)

for old, new in renomeacoes.items():
    tmp_path = f"{bucket}/bronze/tmp_{old}"
    new_path = f"{bucket}/bronze/{new}"
    
    if fs.exists(tmp_path):
        print(f"📁 tmp_{old} → {new}")
        try:
            files = fs.find(tmp_path)
            if files:
                for file in files:
                    new_file = file.replace(f"bronze/tmp_{old}/", f"bronze/{new}/")
                    fs.copy(file, new_file)
                    fs.rm(file)
                print(f"   ✅ {len(files)} arquivos movidos")
            else:
                print(f"   ⚠️  Pasta vazia")
        except Exception as e:
            print(f"   ❌ Erro: {e}")

print("\n" + "=" * 60)
print("✅ RENOMEAÇÃO CONCLUÍDA!")
print("=" * 60)

# Lista pastas finais
print("\n📋 Pastas Bronze no MinIO:")
try:
    bronze_folders = fs.ls(f"{bucket}/bronze/")
    for folder in sorted(bronze_folders):
        folder_name = folder.split("/")[-1]
        if folder_name.startswith(("001_", "002_", "003_", "004_", "005_", "006_", "007_")):
            count = len(fs.find(folder))
            print(f"   📂 {folder_name} ({count} arquivos)")
except Exception as e:
    print(f"   ❌ Erro ao listar: {e}")
