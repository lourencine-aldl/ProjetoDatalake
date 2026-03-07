#!/usr/bin/env python3
"""Análise de dados com DuckDB"""

import duckdb

# Criando banco DuckDB
conn = duckdb.connect('/home/Projetos/ProjetosDocker/data/analysis.duckdb')

# Ler arquivo principal
csv_path = '/home/Projetos/ProjetosDocker/data/data-1772046577622.csv'
print(f'Lendo: {csv_path}\n')

# Criar tabela a partir do CSV (usando CREATE OR REPLACE)
conn.execute(f"CREATE OR REPLACE TABLE conciliacoes AS SELECT * FROM read_csv_auto('{csv_path}')")

# Consultas básicas
total = conn.execute("SELECT COUNT(*) as total FROM conciliacoes").fetchall()[0][0]
print(f'✓ Total de registros: {total}')

# Estrutura da tabela
print('\nColunas da tabela:')
schema = conn.execute("DESCRIBE conciliacoes").fetchall()
for col in schema:
    print(f'  - {col[0]}: {col[1]}')

# Distribuição de status
print('\nDistribuição de STATUS:')
status = conn.execute("""
    SELECT status, COUNT(*) as count 
    FROM conciliacoes 
    GROUP BY status 
    ORDER BY count DESC
""").fetchall()
for row in status:
    print(f'  {row[0]}: {row[1]} registros')

# Valor total por status
print('\nValor total por STATUS:')
value_status = conn.execute("""
    SELECT status, SUM(amount) as total, AVG(amount) as media
    FROM conciliacoes 
    GROUP BY status
    ORDER BY total DESC
""").fetchall()
for row in value_status:
    print(f'  {row[0]}: R$ {row[1]:,.2f} (média: R$ {row[2]:,.2f})')

# Top 10 suppliers
print('\nTop 10 Suppliers:')
suppliers = conn.execute("""
    SELECT supplier, COUNT(*) as count, SUM(amount) as total
    FROM conciliacoes 
    WHERE supplier IS NOT NULL
    GROUP BY supplier 
    ORDER BY count DESC 
    LIMIT 10
""").fetchall()
for row in suppliers:
    print(f'  {row[0]}: {row[1]} registros (R$ {row[2]:,.2f})')

print('\n✓ Banco DuckDB pronto em: /home/Projetos/ProjetosDocker/data/analysis.duckdb')
