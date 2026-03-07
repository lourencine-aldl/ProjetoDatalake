import pyodbc
import pandas as pd
from datetime import datetime
import os
import glob
import subprocess
import shutil
from contextlib import contextmanager
from dotenv import load_dotenv

# Carrega variáveis de ambiente do arquivo .env no diretório do script
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(SCRIPT_DIR, '.env'))

# =========================
# Configurações MinIO (via mc)
# =========================
MINIO_ALIAS = os.getenv('MINIO_ALIAS', 'datalake')
BUCKET = os.getenv('MINIO_BUCKET_RAW', 'raw-zone')
PREFIX = os.getenv('MINIO_PREFIX', 'ahreas/cndcondo')

# =========================
# Configurações SQL Server
# =========================
SQL_SERVER = os.getenv('SQL_SERVER', '192.168.254.10,1433')
SQL_DATABASE = os.getenv('SQL_DATABASE', 'SIGADM')
SQL_USERNAME = os.getenv('SQL_USERNAME', 'app')
SQL_PASSWORD = os.getenv('SQL_PASSWORD')

# =========================
# Diretórios de saída
# =========================
BASE_DIR = os.getenv('BASE_DIR', '/opt/airflow/data/Gscia/ahreas/cndcondo')
LATEST_DIR = os.path.join(BASE_DIR, "latest")
HISTORY_DIR = os.path.join(BASE_DIR, "history")

# =========================
# Configurações gerais
# =========================
MAX_HISTORY_FILES = int(os.getenv('MAX_HISTORY_FILES', '5'))
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


@contextmanager
def sql_server_connection(database=SQL_DATABASE):
    """Context manager para gerenciar conexão com SQL Server"""
    connection_string = (
        f'DRIVER={{ODBC Driver 18 for SQL Server}};'
        f'SERVER={SQL_SERVER};'
        f'DATABASE={database};'
        f'UID={SQL_USERNAME};'
        f'PWD={SQL_PASSWORD};'
        f'TrustServerCertificate=yes'
    )
    
    conn = None
    try:
        conn = pyodbc.connect(connection_string, timeout=30)
        print(f"✓ Conectado ao SQL Server - Banco: {database}")
        yield conn
    except pyodbc.Error as e:
        print(f"✗ Erro na conexão: {e}")
        raise
    finally:
        if conn:
            conn.close()
            print("✓ Conexão fechada")


def mc_available() -> bool:
    """Verifica se o MinIO Client está disponível"""
    return shutil.which("mc") is not None


def upload_to_minio(latest_parquet: str, history_parquet: str, timestamp: str) -> None:
    """Faz upload dos arquivos para o MinIO"""
    if not mc_available():
        print("⚠️ 'mc' não encontrado no ambiente. Pulando upload pro MinIO.")
        print("   Dica: instale/configure o MinIO Client (mc) ou rode isso dentro de um container que tenha mc.")
        return

    try:
        # Upload da última versão
        latest_path = f"{MINIO_ALIAS}/{BUCKET}/{PREFIX}/latest/cndcondo.parquet"
        subprocess.run(
            ["mc", "cp", latest_parquet, latest_path],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"✅ Upload latest para MinIO: {latest_path}")

        # Upload do histórico
        history_path = f"{MINIO_ALIAS}/{BUCKET}/{PREFIX}/history/cndcondo_{timestamp}.parquet"
        subprocess.run(
            ["mc", "cp", history_parquet, history_path],
            check=True,
            capture_output=True,
            text=True
        )
        print(f"✅ Upload history para MinIO: {history_path}")

        print("✅ Upload para MinIO concluído com sucesso.")
    except subprocess.CalledProcessError as e:
        print(f"❌ Erro ao fazer upload para MinIO: {e}")
        print(f"   Stderr: {e.stderr}")
        raise


def extract_cndcondo_data():
    """Extrai dados da tabela cndcondo do SQL Server"""
    print("=" * 80)
    print("🚀 Iniciando extração da camada RAW - Tabela: cndcondo")
    print("=" * 80)
    print(f"📅 Data/Hora: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"🗄️ Servidor: {SQL_SERVER}")
    print(f"💾 Banco de Dados: {SQL_DATABASE}")
    print("=" * 80)
    
    # Query para extrair todos os dados da tabela
    query = "SELECT * FROM [dbo].[cndcondo]"
    
    with sql_server_connection() as conn:
        print(f"📊 Executando query: {query}")
        df = pd.read_sql(query, conn)
        
        # Informações sobre os dados extraídos
        print(f"✅ Dados extraídos: {len(df):,} registros")
        print(f"✅ Colunas encontradas: {len(df.columns)}")
        
        if LOG_LEVEL == 'DEBUG':
            print(f"   Colunas: {', '.join(df.columns.tolist())}")
            print(f"   Tipos de dados:")
            for col, dtype in df.dtypes.items():
                print(f"      {col}: {dtype}")
    
    return df


def get_data_summary(df: pd.DataFrame) -> dict:
    """Retorna um resumo dos dados extraídos"""
    return {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'columns': df.columns.tolist(),
        'memory_usage': df.memory_usage(deep=True).sum() / 1024 / 1024,  # MB
        'null_counts': df.isnull().sum().to_dict()
    }


def save_and_upload_data(df: pd.DataFrame):
    """Salva os dados localmente e faz upload para o MinIO"""
    # Cria diretórios
    os.makedirs(LATEST_DIR, exist_ok=True)
    os.makedirs(HISTORY_DIR, exist_ok=True)
    
    # Define nomes dos arquivos
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    latest_parquet = os.path.join(LATEST_DIR, "cndcondo.parquet")
    history_parquet = os.path.join(HISTORY_DIR, f"cndcondo_{timestamp}.parquet")
    
    # Resumo dos dados
    summary = get_data_summary(df)
    
    try:
        # Salva os arquivos Parquet
        print("\n💾 Salvando arquivos Parquet...")
        df.to_parquet(latest_parquet, index=False, compression='snappy')
        print(f"   ✅ Latest: {latest_parquet}")
        
        df.to_parquet(history_parquet, index=False, compression='snappy')
        print(f"   ✅ History: {history_parquet}")
        
        # Tamanhos dos arquivos
        latest_size = os.path.getsize(latest_parquet) / 1024 / 1024  # MB
        history_size = os.path.getsize(history_parquet) / 1024 / 1024  # MB
        
        print(f"\n📊 Estatísticas:")
        print(f"   • Total de registros: {summary['total_rows']:,}")
        print(f"   • Total de colunas: {summary['total_columns']}")
        print(f"   • Uso de memória: {summary['memory_usage']:.2f} MB")
        print(f"   • Tamanho do arquivo: {latest_size:.2f} MB")
        
        # Upload para o MinIO
        print("\n☁️ Iniciando upload para MinIO...")
        upload_to_minio(latest_parquet, history_parquet, timestamp)
        
    except Exception as e:
        print(f"❌ Erro ao salvar/enviar CSV: {e}")
        raise


def cleanup_old_files(max_files: int = MAX_HISTORY_FILES):
    """Remove arquivos antigos do histórico local, mantendo apenas os mais recentes"""
    files = sorted(
        glob.glob(os.path.join(HISTORY_DIR, "cndcondo_*.parquet")),
        key=os.path.getmtime,
        reverse=True,
    )
    
    if len(files) > max_files:
        print(f"\n🧹 Limpando arquivos antigos (mantendo apenas {max_files} mais recentes)...")
        removed_count = 0
        for f in files[max_files:]:
            try:
                os.remove(f)
                print(f"   🗑️ Removido: {os.path.basename(f)}")
                removed_count += 1
            except Exception as e:
                print(f"   ⚠️ Erro ao remover {os.path.basename(f)}: {e}")
        print(f"✅ Retenção aplicada ({removed_count} arquivo(s) removido(s))\n")
    else:
        print(f"\nℹ️ Total de {len(files)} arquivo(s) no histórico (limite: {max_files})")


def validate_environment():
    """Valida as configurações do ambiente"""
    issues = []
    
    if not SQL_PASSWORD:
        issues.append("SQL_PASSWORD não configurada")
    
    if not os.path.exists(os.path.dirname(BASE_DIR)) and BASE_DIR.startswith('/opt'):
        issues.append(f"Diretório base não acessível: {BASE_DIR}")
    
    if issues:
        print("⚠️ Problemas na configuração do ambiente:")
        for issue in issues:
            print(f"   • {issue}")
        return False
    
    return True


def main():
    """Função principal de execução"""
    print("🚀 Iniciando script raw_layer_cndcondo_env.py")
    print(f"📁 Diretório de trabalho: {os.getcwd()}")
    start_time = datetime.now()
    
    try:
        # Valida ambiente
        print("🔍 Validando ambiente...")
        if not validate_environment():
            print("❌ Corrija os problemas de configuração antes de continuar.")
            return
        
        print("✅ Ambiente validado com sucesso!")
        
        # 1. Extrair dados do SQL Server
        df = extract_cndcondo_data()
        
        if df.empty:
            print("⚠️ Nenhum dado retornado pela query.")
            return
        
        # 2. Salvar localmente e fazer upload para MinIO
        save_and_upload_data(df)
        
        # 3. Limpar arquivos antigos
        cleanup_old_files(max_files=MAX_HISTORY_FILES)
        
        # Tempo de execução
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print("=" * 80)
        print("✅ Processamento da camada RAW concluído com sucesso!")
        print(f"⏱️ Tempo de execução: {duration:.2f} segundos")
        print("=" * 80)
        
    except Exception as e:
        print("=" * 80)
        print(f"❌ Erro durante o processamento: {e}")
        print("=" * 80)
        raise


if __name__ == "__main__":
    main()
