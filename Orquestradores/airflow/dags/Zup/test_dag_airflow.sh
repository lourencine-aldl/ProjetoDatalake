#!/bin/bash
# ============================================================================
# Script de teste das DAGs Zuq no Airflow
# ============================================================================

set -e

echo "=========================================="
echo "🧪 TESTE DAS DAGs NO AIRFLOW - Zuq"
echo "=========================================="
echo ""

CONTAINER="airflow_airflow-scheduler_1"

# 1. Verificar se container está rodando
echo "1️⃣ Verificando Container do Airflow..."
if docker ps | grep -q "$CONTAINER"; then
    echo "  ✅ Container $CONTAINER está rodando"
else
    echo "  ❌ Container $CONTAINER não encontrado"
    exit 1
fi

echo ""

# 2. Verificar arquivos DAG
echo "2️⃣ Verificando Arquivos das DAGs..."
docker exec $CONTAINER bash -c "ls -lh /opt/airflow/dags/Zup/*.py" 2>&1 | grep -E "dag_zup|dag_bronze" || echo "  ⚠️ Arquivos não encontrados"

echo ""

# 3. Testar sintaxe Python
echo "3️⃣ Validando Sintaxe Python..."
docker exec $CONTAINER bash -c "
cd /opt/airflow/dags/Zup
python3 -m py_compile dag_zup.py 2>&1 && echo '  ✅ dag_zup.py' || echo '  ❌ dag_zup.py'
python3 -m py_compile dag_bronze_zup.py 2>&1 && echo '  ✅ dag_bronze_zup.py' || echo '  ❌ dag_bronze_zup.py'
"

echo ""

# 4. Verificar DAGs no Airflow (timeout curto)
echo "4️⃣ Verificando DAGs Registradas (pode demorar)..."
timeout 20 docker exec $CONTAINER bash -c "
airflow dags list 2>/dev/null | grep -E 'dag_zuq|dag_bronze_zuq' 
" 2>&1 || echo "  ⚠️ Timeout ou DAGs não listadas ainda"

echo ""

# 5. Verificar estado das DAGs
echo "5️⃣ Estado das DAGs..."
timeout 15 docker exec $CONTAINER bash -c "
airflow dags state dag_zuq 2>/dev/null || echo 'DAG dag_zuq: Não disponível ainda'
airflow dags state dag_bronze_zuq 2>/dev/null || echo 'DAG dag_bronze_zuq: Não disponível ainda'  
" 2>&1 || echo "  ⚠️ Timeout ao verificar estado"

echo ""

# 6. Testar parse das DAGs
echo "6️⃣ Testando Parse das DAGs..."
timeout 20 docker exec $CONTAINER bash -c "
airflow dags show dag_zuq 2>&1 | head -5
" 2>&1 || echo "  ⚠️ Timeout ou erro ao mostrar DAG"

echo ""
echo "=========================================="
echo "✅ Testes Concluídos!"
echo "=========================================="
echo ""
echo "📌 Próximos passos:"
echo "  - Acesse o Airflow UI: http://localhost:8085"
echo "  - Login: admin / airflow123"
echo "  - Procure por: dag_zuq ou dag_bronze_zuq"
echo ""
