#!/bin/bash

# Script para executar visualizações DuckDB

VENV_PATH="/home/Projetos/ProjetosDocker/data/.venv"
SCRIPT_PATH="/home/Projetos/ProjetosDocker/data/visualize_duckdb.py"

echo "📊 Gerando visualizações com Plotly..."
echo ""

$VENV_PATH/bin/python $SCRIPT_PATH

echo ""
echo "🎉 Visualizações prontas!"
echo "   Abra: file:///home/Projetos/ProjetosDocker/data/dashboard.html"
