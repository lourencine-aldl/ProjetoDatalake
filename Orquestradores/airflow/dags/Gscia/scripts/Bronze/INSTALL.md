# 🚀 Guia de Instalação Rápida - Camada RAW

## 📋 Pré-requisitos

- Python 3.8+
- SQL Server com acesso à tabela `cndcondo`
- MinIO Server configurado
- Windows ou Linux

## ⚡ Instalação Rápida (5 minutos)

### 1. Instalar Dependências Python

```bash
cd c:\Projetos-Gscia\2.Python\ConexaoSqlServer
pip install -r requirements.txt
```

### 2. Instalar Driver ODBC SQL Server

**Windows:**
```powershell
# Baixe e instale o driver:
# https://go.microsoft.com/fwlink/?linkid=2249004
```

**Linux:**
```bash
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
curl https://packages.microsoft.com/config/ubuntu/20.04/prod.list > /etc/apt/sources.list.d/mssql-release.list
apt-get update
ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### 3. Instalar MinIO Client

**Windows:**
```powershell
# Via Chocolatey
choco install minio-client

# Ou download direto
Invoke-WebRequest -Uri "https://dl.min.io/client/mc/release/windows-amd64/mc.exe" -OutFile "mc.exe"
# Mova mc.exe para uma pasta no PATH
```

**Linux:**
```bash
wget https://dl.min.io/client/mc/release/linux-amd64/mc
chmod +x mc
sudo mv mc /usr/local/bin/
```

### 4. Configurar MinIO Client

```bash
# Configure o alias
mc alias set datalake http://SEU_MINIO_SERVER:9000 ACCESS_KEY SECRET_KEY

# Crie o bucket
mc mb datalake/raw-zone --ignore-existing

# Teste
mc ls datalake
```

### 5. Configurar Variáveis de Ambiente

```bash
# Copie o arquivo de exemplo
cp .env.example .env

# Edite o arquivo .env com suas credenciais
notepad .env  # Windows
nano .env     # Linux
```

Exemplo de `.env`:
```ini
# MinIO
MINIO_ALIAS=datalake
MINIO_ENDPOINT=http://localhost:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET_RAW=raw-zone

# SQL Server
SQL_SERVER=192.168.254.10,1433
SQL_DATABASE=SIGADM
SQL_USERNAME=app
SQL_PASSWORD=sua_senha_aqui

# Diretórios (Windows)
BASE_DIR=C:/Temp/data/sqlserver/cndcondo

# Diretórios (Linux)
# BASE_DIR=/opt/airflow/data/sqlserver/cndcondo
```

### 6. Testar Configuração

```bash
python test_environment.py
```

Saída esperada:
```
🧪 TESTE DE CONFIGURAÇÃO DO AMBIENTE
============================================================
✅ TODOS OS TESTES PASSARAM!
   O ambiente está pronto para executar o script.
```

### 7. Executar Extração

```bash
# Versão simples (sem .env)
python raw_layer_cndcondo.py

# Versão com .env (recomendado)
python raw_layer_cndcondo_env.py
```

## 🎯 Verificar Resultados

### Arquivos Locais

```bash
# Windows
dir C:\Temp\data\sqlserver\cndcondo\latest\
dir C:\Temp\data\sqlserver\cndcondo\history\

# Linux
ls -lh /opt/airflow/data/sqlserver/cndcondo/latest/
ls -lh /opt/airflow/data/sqlserver/cndcondo/history/
```

### Arquivos no MinIO

```bash
# Listar bucket
mc ls datalake/raw-zone/

# Listar arquivos cndcondo
mc ls datalake/raw-zone/sqlserver/cndcondo/latest/
mc ls datalake/raw-zone/sqlserver/cndcondo/history/

# Ver detalhes de um arquivo
mc stat datalake/raw-zone/sqlserver/cndcondo/latest/cndcondo.csv

# Baixar arquivo para verificar
mc cp datalake/raw-zone/sqlserver/cndcondo/latest/cndcondo.csv ./
```

## 🔧 Troubleshooting

### Erro: "mc não encontrado"

```bash
# Verificar instalação
mc --version

# Adicionar ao PATH (Windows)
$env:Path += ";C:\caminho\para\mc"

# Adicionar ao PATH (Linux)
export PATH=$PATH:/caminho/para/mc
```

### Erro: "ODBC Driver 18 not found"

```bash
# Windows: Reinstale o driver
# https://go.microsoft.com/fwlink/?linkid=2249004

# Linux: Instale o driver
sudo ACCEPT_EULA=Y apt-get install -y msodbcsql18
```

### Erro: "Cannot connect to MinIO"

```bash
# Verificar se o MinIO está rodando
mc admin info datalake

# Reconfigurar alias
mc alias remove datalake
mc alias set datalake http://SEU_SERVER:9000 ACCESS_KEY SECRET_KEY
```

### Erro: "SQL Server connection failed"

```bash
# Testar conexão com sqlcmd (Windows/Linux)
sqlcmd -S 192.168.254.10,1433 -U app -P 'senha' -d SIGADM -Q "SELECT @@VERSION"

# Verificar firewall
Test-NetConnection -ComputerName 192.168.254.10 -Port 1433  # Windows
nc -zv 192.168.254.10 1433  # Linux
```

## 📅 Agendar Execução (Opcional)

### Windows Task Scheduler

```powershell
# Criar tarefa agendada (executa diariamente às 2h)
$action = New-ScheduledTaskAction -Execute "python" -Argument "C:\Projetos-Gscia\2.Python\ConexaoSqlServer\raw_layer_cndcondo_env.py"
$trigger = New-ScheduledTaskTrigger -Daily -At 2am
Register-ScheduledTask -Action $action -Trigger $trigger -TaskName "RAW_Layer_cndcondo"
```

### Linux Cron

```bash
# Editar crontab
crontab -e

# Adicionar linha (executa diariamente às 2h)
0 2 * * * cd /caminho/para/projeto && /usr/bin/python3 raw_layer_cndcondo_env.py >> /var/log/raw_layer_cndcondo.log 2>&1
```

### Airflow

```bash
# Copiar DAG para o diretório do Airflow
cp airflow_dag_raw_cndcondo.py /opt/airflow/dags/

# Copiar script para o diretório de scripts
cp raw_layer_cndcondo_env.py /opt/airflow/dags/scripts/

# Reiniciar o Airflow (se necessário)
airflow dags trigger raw_layer_cndcondo
```

## 🐳 Docker (Opcional)

Se preferir rodar em container:

```dockerfile
# Dockerfile
FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && apt-get clean

RUN wget https://dl.min.io/client/mc/release/linux-amd64/mc \
    && chmod +x mc \
    && mv mc /usr/local/bin/

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "raw_layer_cndcondo_env.py"]
```

Build e executar:
```bash
docker build -t raw-layer-cndcondo .
docker run --env-file .env raw-layer-cndcondo
```

## ✅ Checklist Final

- [ ] Python 3.8+ instalado
- [ ] Driver ODBC SQL Server instalado
- [ ] MinIO Client instalado e configurado
- [ ] Dependências Python instaladas (`pip install -r requirements.txt`)
- [ ] Arquivo `.env` configurado
- [ ] Teste de ambiente passou (`python test_environment.py`)
- [ ] Primeira execução bem-sucedida
- [ ] Arquivos visíveis no MinIO
- [ ] Agendamento configurado (se aplicável)

## 📞 Suporte

- **Documentação completa:** [README_RAW_LAYER.md](README_RAW_LAYER.md)
- **Problemas:** Verifique os logs de execução
- **Contato:** Time de Data Engineering

---
**Última atualização:** 04/02/2026
