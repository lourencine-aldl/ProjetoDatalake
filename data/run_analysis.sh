#!/bin/bash

# Script para executar análise DuckDB
# Usa o .venv local do projeto

VENV_PATH="/home/Projetos/ProjetosDocker/data/.venv"
SCRIPT_PATH="/home/Projetos/ProjetosDocker/data/analysis_duckdb.py"

echo "🚀 Executando análise DuckDB..."
echo "  Usando venv: $VENV_PATH"
echo "  Script: $SCRIPT_PATH"
echo ""

$VENV_PATH/bin/python $SCRIPT_PATH

echo ""
echo "✓ Análise concluída!"
