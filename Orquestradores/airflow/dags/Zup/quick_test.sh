#!/bin/bash
echo "🧪 Teste Rápido das DAGs"
echo "========================"
echo ""

cd /opt/airflow/dags/Zup

# Teste 1: Sintaxe
echo "1️⃣ Sintaxe:"
python3 -c "import ast; ast.parse(open('dag_zup.py').read())" && echo "  ✅ dag_zup.py" || echo "  ❌ dag_zup.py"
python3 -c "import ast; ast.parse(open('dag_bronze_zup.py').read())" && echo "  ✅ dag_bronze_zup.py" || echo "  ❌ dag_bronze_zup.py"

echo ""
echo "2️⃣ DAG IDs registrados:"
airflow dags list 2>/dev/null | grep -E "dag_zuq|dag_bronze" || echo "  ⚠️ DAGs não listadas (pode estar carregando)"

echo ""
echo "✅ Teste concluído!"
