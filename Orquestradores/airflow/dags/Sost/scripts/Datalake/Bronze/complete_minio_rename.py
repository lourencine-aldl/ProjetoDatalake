#!/usr/bin/env python3
"""
Completa a renomeação das pastas Bronze no MinIO
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
print(f"📂 Bucket: {bucket}\n")

# Cria filesystem
fs = s3fs.S3FileSystem(
    key=access_key,
    secret=secret_key,
    client_kwargs={"endpoint_url": endpoint},
    use_ssl=False,
    default_fill_cache=False,
)

# Pendências a completar
pendencias = [
    ("tmp_002_bronze_pcnfent", "004_bronze_pcnfent"),  # completar merge
    ("tmp_003_bronze_pcsupervisor", "001_bronze_pcsupervisor"),
    ("tmp_004_bronze_pcusuario", "002_bronze_pcusuario"),
    ("tmp_005_bronze_tab_faturamento_full", "006_bronze_tab_faturamento_full"),
    ("tmp_006_bronze_tab_movimentacao", "007_bronze_tab_movimentacao"),
    ("tmp_007_bronze_tab_cliente", "003_bronze_tab_cliente"),
]

print("=" * 70)
print("COMPLETANDO RENOMEAÇÃO")
print("=" * 70)

for tmp_name, final_name in pendencias:
    tmp_path = f"{bucket}/bronze/{tmp_name}"
    final_path = f"{bucket}/bronze/{final_name}"
    
    if fs.exists(tmp_path):
        print(f"\n📁 {tmp_name} → {final_name}")
        try:
            files = fs.find(tmp_path)
            if files:
                print(f"   📦 Movendo {len(files)} arquivos...")
                for i, file in enumerate(files, 1):
                    new_file = file.replace(f"bronze/{tmp_name}/", f"bronze/{final_name}/")
                    fs.copy(file, new_file)
                    fs.rm(file)
                    if i % 10 == 0:
                        print(f"   ... {i}/{len(files)}")
                print(f"   ✅ {len(files)} arquivos movidos com sucesso")
            else:
                print(f"   ⚠️  Pasta vazia")
        except Exception as e:
            print(f"   ❌ Erro: {e}")
    else:
        print(f"\n⏭️  {tmp_name} não existe, pulando...")

print("\n" + "=" * 70)
print("VERIFICAÇÃO FINAL")
print("=" * 70 + "\n")

# Lista pastas finais
try:
    folders = fs.ls(f"{bucket}/bronze/")
    print("📋 Pastas Bronze no MinIO:\n")
    for folder in sorted(folders):
        folder_name = folder.split("/")[-1]
        if not folder_name.startswith("tmp_"):
            count = len(fs.find(folder))
            status = "✅" if folder_name.startswith(("001_", "002_", "003_", "004_", "005_", "006_", "007_")) else "📂"
            print(f"   {status} {folder_name:40} ({count:3} arquivos)")
    
    # Verifica se ainda há temporários
    temp_folders = [f for f in folders if f.split("/")[-1].startswith("tmp_")]
    if temp_folders:
        print(f"\n⚠️  Ainda restam {len(temp_folders)} pasta(s) temporária(s):")
        for tf in temp_folders:
            print(f"   📂 {tf.split('/')[-1]}")
    else:
        print("\n✅ Nenhuma pasta temporária restante!")
        
except Exception as e:
    print(f"❌ Erro ao listar: {e}")

print("\n" + "=" * 70)
print("✅ PROCESSO CONCLUÍDO!")
print("=" * 70)
