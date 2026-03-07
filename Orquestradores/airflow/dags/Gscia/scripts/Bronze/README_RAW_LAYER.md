# Camada RAW - Extração de Dados SQL Server para MinIO

## 📋 Descrição

Este projeto implementa a camada RAW do Data Lake, extraindo dados da tabela `cndcondo` do SQL Server SIGADM e armazenando no MinIO (object storage compatível com S3).

## 🏗️ Arquitetura

```
SQL Server (SIGADM)
    └── Tabela: cndcondo
         │
         ▼
    [Extração Python]
         │
         ├── 💾 Armazenamento Local
         │    ├── /latest/cndcondo.csv (versão atual)
         │    └── /history/cndcondo_YYYYMMDD_HHMMSS.csv (histórico)
         │
         └── ☁️ Upload para MinIO
              └── Bucket: raw-zone
                   └── sqlserver/cndcondo/
                        ├── latest/cndcondo.csv
                        └── history/cndcondo_YYYYMMDD_HHMMSS.csv
```

## 📦 Dependências

```bash
pip install pandas pyodbc
```

### Driver ODBC SQL Server

**Windows:**
- Baixe e instale: [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

**Linux:**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### MinIO Client (mc)

**Windows:**
```powershell
# Usando Chocolatey
choco install minio-client

# Ou baixar diretamente
wget https://dl.min.io/client/mc/release/windows-amd64/mc.exe
```

**Linux:**
```bash
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/
```

## ⚙️ Configuração

### 1. Configurar MinIO Client

```bash
# Configurar alias do MinIO
mc alias set datalake http://seu-minio-server:9000 ACCESS_KEY SECRET_KEY

# Criar bucket se não existir
mc mb datalake/raw-zone --ignore-existing

# Verificar
mc ls datalake
```

Ou use o script auxiliar:
```bash
chmod +x setup_minio.sh
./setup_minio.sh
```

### 2. Configurar variáveis no script

Edite o arquivo `raw_layer_cndcondo.py`:

```python
# Configurações MinIO
MINIO_ALIAS = "datalake"  # Nome do alias configurado no mc
BUCKET = "raw-zone"        # Nome do bucket
PREFIX = "sqlserver/cndcondo"  # Caminho dentro do bucket

# Configurações SQL Server
SQL_SERVER = '192.168.254.10,1433'
SQL_DATABASE = 'SIGADM'
SQL_USERNAME = 'app'
SQL_PASSWORD = 'sua-senha'

# Diretórios locais
BASE_DIR = "/opt/airflow/data/sqlserver/cndcondo"
```

## 🚀 Execução

### Execução Manual

```bash
python raw_layer_cndcondo.py
```

### Execução via Airflow

Crie uma DAG para agendar a execução:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
sys.path.append('/opt/airflow/dags/scripts')
from raw_layer_cndcondo import main

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'raw_layer_cndcondo',
    default_args=default_args,
    description='Extração RAW da tabela cndcondo para MinIO',
    schedule_interval='0 2 * * *',  # Diariamente às 2h
    catchup=False,
) as dag:
    
    extract_task = PythonOperator(
        task_id='extract_cndcondo',
        python_callable=main,
    )
```

## 📊 Estrutura dos Dados

O script extrai todos os campos da tabela `cndcondo` e salva em formato CSV UTF-8 com BOM.

### Formato de Saída

- **Encoding:** UTF-8-SIG (com BOM)
- **Formato:** CSV
- **Separador:** vírgula (,)
- **Índice:** não incluído

## 🔄 Gestão de Arquivos

### Retenção Local
- Mantém apenas os 5 arquivos mais recentes no histórico local
- Arquivos mais antigos são automaticamente removidos

### Armazenamento MinIO
- **latest/**: sempre contém a versão mais recente
- **history/**: contém todas as versões históricas com timestamp

## 📝 Logs de Execução

O script fornece logs detalhados:

```
================================================================================
🚀 Iniciando extração da camada RAW - Tabela: cndcondo
================================================================================
✓ Conectado ao SQL Server - Banco: SIGADM
📊 Executando query: SELECT * FROM [dbo].[cndcondo]
✅ Dados extraídos: 1,234 registros
✅ Colunas encontradas: 15
   Colunas: id, nome, endereco, ...
✓ Conexão fechada
✅ Última versão salva: /opt/airflow/data/sqlserver/cndcondo/latest/cndcondo.csv
📦 Histórico salvo: /opt/airflow/data/sqlserver/cndcondo/history/cndcondo_20240201_143000.csv
📊 Total de registros salvos: 1,234
✅ Upload latest para MinIO: datalake/raw-zone/sqlserver/cndcondo/latest/cndcondo.csv
✅ Upload history para MinIO: datalake/raw-zone/sqlserver/cndcondo/history/cndcondo_20240201_143000.csv
✅ Upload para MinIO concluído com sucesso.
================================================================================
✅ Processamento da camada RAW concluído com sucesso!
================================================================================
```

## 🛠️ Troubleshooting

### Erro: "mc não encontrado"
```bash
# Verifique se o mc está instalado
mc --version

# Verifique se está no PATH
which mc
```

### Erro de conexão SQL Server
```bash
# Teste a conexão
sqlcmd -S 192.168.254.10,1433 -U app -P 'senha' -d SIGADM -Q "SELECT @@VERSION"
```

### Erro de permissão MinIO
```bash
# Verifique o alias
mc alias list

# Teste o acesso
mc ls datalake/raw-zone
```

## 📈 Próximos Passos

1. **Camada Bronze:** Transformação e limpeza dos dados
2. **Camada Silver:** Agregações e modelagem dimensional
3. **Camada Gold:** Dados prontos para consumo e dashboards
4. **Monitoramento:** Integração com Prometheus/Grafana
5. **Alertas:** Notificações em caso de falha

## 📄 Licença

Projeto interno - Gscia

---
**Criado em:** 04/02/2026  
**Versão:** 1.0.0
