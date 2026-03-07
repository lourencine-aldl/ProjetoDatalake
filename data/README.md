# Análise de Dados com DuckDB

## 📁 Estrutura

- `.venv/` - Ambiente virtual Python 3.12.3 com DuckDB instalado
- `analysis_duckdb.py` - Script principal de análise
- `run_analysis.sh` - Atalho para executar o script
- `requirements.txt` - Dependências do projeto

## 🚀 Como usar

### Opção 1: Executar via script bash (mais fácil)
```bash
/home/Projetos/ProjetosDocker/data/run_analysis.sh
```

### Opção 2: Usar venv diretamente
```bash
# Ativar o venv
source /home/Projetos/ProjetosDocker/data/.venv/bin/activate

# Executar script
python analysis_duckdb.py

# Desativar venv
deactivate
```

### Opção 3: Python direto sem ativar venv
```bash
/home/Projetos/ProjetosDocker/data/.venv/bin/python analysis_duckdb.py
```

## 📊 O que o script faz

1. Conecta ao DuckDB (cria banco em `analysis.duckdb`)
2. Lê o arquivo CSV `data-1772046577622.csv`
3. Executa análises SQL:
   - Contagem total de registros
   - Distribuição por status
   - Valor total por status
   - Top 10 suppliers

## 💾 Banco de dados

O banco DuckDB é criado em:
- `analysis.duckdb` (local)

Você pode fazer mais queries usando:
```python
import duckdb
conn = duckdb.connect('/home/Projetos/ProjetosDocker/data/analysis.duckdb')
result = conn.execute("SELECT * FROM conciliacoes LIMIT 5").fetchall()
```

## 📦 Reinstalar dependências

Se precisar reinstalar as dependências:
```bash
/home/Projetos/ProjetosDocker/data/.venv/bin/pip install -r requirements.txt
```
