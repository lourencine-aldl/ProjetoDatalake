#!/usr/bin/env python3
"""
Script para criar buckets no MinIO se não existirem
"""
import os
from minio import Minio
from dotenv import load_dotenv

def main():
    # Carregar variáveis de ambiente
    env_path = os.path.join(os.path.dirname(__file__), "../../.env")
    load_dotenv(env_path)
    
    # Configuração do MinIO
    endpoint = os.getenv("MINIO_ENDPOINT", "http://localhost:9000").replace("http://", "")
    access_key = os.getenv("MINIO_ROOT_USER", "minioadmin")
    secret_key = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
    bucket_name = os.getenv("MINIO_BUCKET_RAW", "raw-zone")
    
    print(f"Conectando ao MinIO: {endpoint}")
    print(f"Bucket: {bucket_name}")
    
    try:
        # Criar cliente MinIO
        client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=False  # HTTP, não HTTPS
        )
        
        # Verificar se o bucket existe
        if client.bucket_exists(bucket_name):
            print(f"✅ Bucket '{bucket_name}' já existe")
        else:
            # Criar bucket
            client.make_bucket(bucket_name)
            print(f"✅ Bucket '{bucket_name}' criado com sucesso!")
            
    except Exception as e:
        print(f"❌ Erro: {e}")
        return 1
    
    return 0

if __name__ == "__main__":
    exit(main())
