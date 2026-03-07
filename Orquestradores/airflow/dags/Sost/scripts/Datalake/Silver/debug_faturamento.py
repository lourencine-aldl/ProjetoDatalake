#!/usr/bin/env python3
"""
🔍 Script de diagnóstico: Rastreia onde os dados estão sendo perdidos
Uso: python debug_faturamento.py
"""

import io
import os
import logging
from pathlib import Path
import pandas as pd
import pyarrow.parquet as pq
import s3fs
from dotenv import load_dotenv


logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("debug_faturamento")


def load_config():
    script_dir = Path(__file__).parent
    env_path = script_dir / ".." / ".env"
    
    load_dotenv(env_path)
    
    minio_cfg = {
        "endpoint": os.getenv("MINIO_ENDPOINT", "").strip(),
        "bucket": os.getenv("MINIO_BUCKET_SOST", "").strip(),
        "access_key": os.getenv("MINIO_ROOT_USER", "").strip(),
        "secret_key": os.getenv("MINIO_ROOT_PASSWORD", "").strip(),
    }
    
    pg_cfg = {
        "host": os.getenv("POSTGRES_DW_HOST", "").strip(),
        "port": int(os.getenv("POSTGRES_DW_PORT", "5432") or "5432"),
        "database": os.getenv("POSTGRES_DW_DB", "").strip(),
        "user": os.getenv("POSTGRES_DW_USER", "").strip(),
        "password": os.getenv("POSTGRES_DW_PASSWORD", "").strip(),
    }
    
    return minio_cfg, pg_cfg


def get_minio_fs(minio_cfg):
    return s3fs.S3FileSystem(
        key=minio_cfg["access_key"],
        secret=minio_cfg["secret_key"],
        client_kwargs={"endpoint_url": minio_cfg["endpoint"]},
        config_kwargs={"signature_version": "s3v4"},
        use_ssl=False,
        default_fill_cache=False,
    )


def analyze_bronze():
    logger.info("=" * 80)
    logger.info("📊 ANALISANDO DADOS NO BRONZE")
    logger.info("=" * 80)
    
    minio_cfg, _ = load_config()
    fs = get_minio_fs(minio_cfg)
    
    # Listar arquivos Bronze
    pattern = f"{minio_cfg['bucket']}/bronze/006_bronze_tab_faturamento_full/*.parquet"
    logger.info(f"Procurando arquivos em: s3://{pattern}")
    
    files = sorted(fs.glob(pattern))
    if not files:
        logger.error(f"❌ Nenhum arquivo encontrado em s3://{pattern}")
        return
    
    logger.info(f"✅ {len(files)} arquivo(s) encontrado(s)")
    
    for file_path in files[:1]:  # Analisar apenas o primeiro
        logger.info(f"\n📄 Analisando: {file_path}")
        s3_uri = f"s3://{file_path}"
        
        try:
            with fs.open(s3_uri, "rb") as f:
                table = pq.read_table(io.BytesIO(f.read()))
                df = table.to_pandas()
            
            logger.info(f"   Shape: {df.shape}")
            logger.info(f"   Colunas ({len(df.columns)}): {list(df.columns)}")
            logger.info(f"   Tipos:\n{df.dtypes}")
            logger.info(f"   Null counts:\n{df.isnull().sum()}")
            logger.info(f"   Primeiras 3 linhas:\n{df.head(3)}")
            
            # Verificar PK
            pk_cols = ["codfilial", "data_faturamento", "numnota", "codigo", "codcli"]
            pk_available = [c for c in pk_cols if c.lower() in [col.lower() for col in df.columns]]
            logger.info(f"   Colunas PK encontradas: {pk_available}")
            
            if pk_available:
                logger.info(f"   PK nulls:\n{df[pk_available].isnull().sum()}")
            
        except Exception as e:
            logger.error(f"   ❌ Erro lendo arquivo: {e}")


def analyze_database():
    logger.info("\n" + "=" * 80)
    logger.info("🗄️ ANALISANDO BANCO DE DADOS")
    logger.info("=" * 80)
    
    _, pg_cfg = load_config()
    
    try:
        import psycopg2
        
        conn = psycopg2.connect(
            host=pg_cfg["host"],
            port=pg_cfg["port"],
            database=pg_cfg["database"],
            user=pg_cfg["user"],
            password=pg_cfg["password"],
        )
        
        with conn.cursor() as cur:
            # Checar se tabela existe
            cur.execute(
                "SELECT to_regclass(%s)",
                ("public.silver_tab_faturamento_full",)
            )
            exists = cur.fetchone()[0]
            
            if exists:
                logger.info("✅ Tabela public.silver_tab_faturamento_full existe")
                
                # Contar linhas
                cur.execute("SELECT COUNT(*) FROM public.silver_tab_faturamento_full")
                count = cur.fetchone()[0]
                logger.info(f"   Linhas na tabela: {count}")
                
                if count == 0:
                    logger.warning("   ⚠️ TABELA VAZIA - dados não estão sendo inseridos!")
                
                # Listar colunas
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema = 'public' AND table_name = 'silver_tab_faturamento_full'
                    ORDER BY ordinal_position
                """)
                cols = cur.fetchall()
                logger.info(f"   Colunas ({len(cols)}):")
                for col_name, col_type in cols[:10]:
                    logger.info(f"      - {col_name}: {col_type}")
                if len(cols) > 10:
                    logger.info(f"      ... (+{len(cols)-10} mais)")
            else:
                logger.error("❌ Tabela public.silver_tab_faturamento_full NÃO existe")
        
        conn.close()
        
    except Exception as e:
        logger.error(f"❌ Erro conectando ao banco: {e}")


if __name__ == "__main__":
    try:
        analyze_bronze()
        analyze_database()
        
        logger.info("\n" + "=" * 80)
        logger.info("🔍 DIAGNÓSTICO COMPLETO")
        logger.info("=" * 80)
        logger.info("""
Próximos passos se encontrou problema:
1. Se Bronze tem dados mas tabela está vazia:
   - Execute: python 005_silver_tab_faturamento_full.py
   - Verificar logs para ver onde dados são filtrados

2. Se Bronze está vazio:
   - Verificar se dag_bronze_sost.py rodou com sucesso
   - Conferir MinIO com s3fs ou UI Dremio

3. Possíveis causas de perda de dados em Silver:
   - PK nula (codfilial, data_faturamento, numnota, codigo, codcli)
   - Conversão de tipos falhando silenciosamente
   - Filtro 'linhas vazias' removendo tudo
        """)
        
    except Exception as e:
        logger.error(f"❌ Erro fatal: {e}", exc_info=True)
