#!/usr/bin/env python3
"""Visualização de dados DuckDB com gráficos"""

import duckdb
import pandas as pd
import subprocess
import sys

# Instalar plotly se necessário
try:
    import plotly.express as px
    import plotly.graph_objects as go
except ImportError:
    print('Instalando Plotly...')
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'plotly', '-q'])
    import plotly.express as px
    import plotly.graph_objects as go

# Conectar ao DuckDB
conn = duckdb.connect('/home/Projetos/ProjetosDocker/data/analysis.duckdb')

print('📊 Gerando visualizações...\n')

# 1. Gráfico de STATUS
print('1️⃣  Criando gráfico de distribuição de STATUS...')
status_df = pd.DataFrame(
    conn.execute("""
        SELECT status, COUNT(*) as count 
        FROM conciliacoes 
        GROUP BY status 
        ORDER BY count DESC
    """).fetchall(),
    columns=['Status', 'Quantidade']
)

fig1 = px.pie(
    status_df, 
    values='Quantidade', 
    names='Status',
    title='Distribuição por Status',
    template='plotly_dark'
)
fig1.write_html('/home/Projetos/ProjetosDocker/data/grafico_status.html')
print('   ✓ Salvo em: grafico_status.html')

# 2. Gráfico de VALOR por STATUS
print('2️⃣  Criando gráfico de valor por STATUS...')
value_df = pd.DataFrame(
    conn.execute("""
        SELECT status, SUM(amount) as total 
        FROM conciliacoes 
        GROUP BY status
        ORDER BY total DESC
    """).fetchall(),
    columns=['Status', 'Valor']
)

fig2 = px.bar(
    value_df, 
    x='Status', 
    y='Valor',
    title='Valor Total por Status (R$)',
    template='plotly_dark',
    text='Valor',
    color='Valor'
)
fig2.update_traces(texttemplate='R$ %.0f', textposition='auto')
fig2.write_html('/home/Projetos/ProjetosDocker/data/grafico_valor_status.html')
print('   ✓ Salvo em: grafico_valor_status.html')

# 3. Top 10 Suppliers
print('3️⃣  Criando gráfico de Top 10 Suppliers...')
suppliers_df = pd.DataFrame(
    conn.execute("""
        SELECT supplier, COUNT(*) as count, SUM(amount) as total
        FROM conciliacoes 
        WHERE supplier IS NOT NULL
        GROUP BY supplier 
        ORDER BY total DESC 
        LIMIT 10
    """).fetchall(),
    columns=['Supplier', 'Quantidade', 'Valor']
)

fig3 = px.bar(
    suppliers_df, 
    x='Supplier', 
    y='Valor',
    title='Top 10 Suppliers por Valor',
    template='plotly_dark',
    hover_data=['Quantidade'],
    color='Valor'
)
fig3.update_xaxes(tickangle=-45)
fig3.write_html('/home/Projetos/ProjetosDocker/data/grafico_top_suppliers.html')
print('   ✓ Salvo em: grafico_top_suppliers.html')

# 4. Série temporal (se houver dados de data)
print('4️⃣  Criando gráfico temporal...')
temporal_df = pd.DataFrame(
    conn.execute("""
        SELECT DATE(created_at) as data, COUNT(*) as quantidade, SUM(amount) as valor
        FROM conciliacoes 
        GROUP BY DATE(created_at)
        ORDER BY data
    """).fetchall(),
    columns=['Data', 'Quantidade', 'Valor']
)

fig4 = px.line(
    temporal_df, 
    x='Data', 
    y='Valor',
    title='Evolução de Valor ao Longo do Tempo',
    template='plotly_dark',
    markers=True
)
fig4.write_html('/home/Projetos/ProjetosDocker/data/grafico_temporal.html')
print('   ✓ Salvo em: grafico_temporal.html')

# 5. Resumo geral
print('5️⃣  Criando dashboard resumido...')

total_registros = conn.execute("SELECT COUNT(*) FROM conciliacoes").fetchone()[0]
total_valor = conn.execute("SELECT SUM(amount) FROM conciliacoes").fetchone()[0]
matched = conn.execute("SELECT COUNT(*) FROM conciliacoes WHERE status = 'MATCHED'").fetchone()[0]
taxa_match = (matched / total_registros) * 100

resumo_html = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="UTF-8">
    <title>Dashboard - Análise de Conciliações</title>
    <style>
        body {{
            font-family: Arial, sans-serif;
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            margin: 0;
            padding: 20px;
        }}
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        h1 {{
            text-align: center;
            margin-bottom: 30px;
        }}
        .grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .card {{
            background: rgba(255, 255, 255, 0.1);
            border: 1px solid rgba(255, 255, 255, 0.2);
            border-radius: 10px;
            padding: 20px;
            text-align: center;
        }}
        .card h3 {{
            margin: 0;
            font-size: 14px;
            opacity: 0.8;
            text-transform: uppercase;
        }}
        .card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 10px 0 0 0;
        }}
        .links {{
            text-align: center;
            margin-top: 30px;
        }}
        .links a {{
            display: inline-block;
            background: #0066cc;
            color: white;
            padding: 12px 20px;
            margin: 10px;
            text-decoration: none;
            border-radius: 5px;
            transition: background 0.3s;
        }}
        .links a:hover {{
            background: #0052a3;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>📊 Dashboard - Análise de Conciliações</h1>
        
        <div class="grid">
            <div class="card">
                <h3>Total de Registros</h3>
                <div class="value">{total_registros:,.0f}</div>
            </div>
            <div class="card">
                <h3>Valor Total</h3>
                <div class="value">R$ {total_valor:,.2f}</div>
            </div>
            <div class="card">
                <h3>Registros Matched</h3>
                <div class="value">{matched:,.0f}</div>
            </div>
            <div class="card">
                <h3>Taxa de Match</h3>
                <div class="value">{taxa_match:.1f}%</div>
            </div>
        </div>

        <div class="links">
            <h2>📈 Gráficos Disponíveis</h2>
            <a href="grafico_status.html">📊 Distribuição por Status</a>
            <a href="grafico_valor_status.html">💰 Valor por Status</a>
            <a href="grafico_top_suppliers.html">🏆 Top 10 Suppliers</a>
            <a href="grafico_temporal.html">📅 Série Temporal</a>
        </div>
    </div>
</body>
</html>
"""

with open('/home/Projetos/ProjetosDocker/data/dashboard.html', 'w') as f:
    f.write(resumo_html)

print('   ✓ Salvo em: dashboard.html')

print('\n✅ Todas as visualizações foram criadas!')
print('\n📂 Arquivos HTML gerados:')
print('   1. dashboard.html - Resumo geral com links para todos os gráficos')
print('   2. grafico_status.html - Gráfico de pizza por status')
print('   3. grafico_valor_status.html - Gráfico de barras de valor')
print('   4. grafico_top_suppliers.html - Top 10 suppliers')
print('   5. grafico_temporal.html - Série temporal')
print('\n🌐 Abra no navegador para visualizar os gráficos!')
