#!/bin/bash
# ============================================================================
# Script para executar todos os pipelines Bronze do Zuq
# ============================================================================

set -e  # Para em caso de erro

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# Python do container Airflow
PYTHON_BIN="python3"

echo "=========================================="
echo "🚀 Iniciando Pipelines Bronze - Zuq"
echo "=========================================="
echo "Diretório: $SCRIPT_DIR"
echo "Python: $PYTHON_BIN"
echo "Data/Hora: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

# Cores para output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Contador
TOTAL=0
SUCCESS=0
FAILED=0

# Array com os scripts na ordem de execução
SCRIPTS=(
    "bronze_zuq_vehicles.py"
    "bronze_zuq_contact_drivers.py"
    "bronze_zuq_vehicle_daily.py"
    "bronze_zuq_driver_workjourney.py"
    "bronze_zuq_notification.py"
    "bronze_zuq_maintenance_reminder.py"
    "bronze_zuq_maintenance_results.py"
)

# Função para executar script
run_script() {
    local script=$1
    TOTAL=$((TOTAL + 1))
    
    echo ""
    echo "=========================================="
    echo "📦 [$TOTAL/${#SCRIPTS[@]}] Executando: $script"
    echo "=========================================="
    echo "Início: $(date '+%H:%M:%S')"
    
    if $PYTHON_BIN "$script"; then
        SUCCESS=$((SUCCESS + 1))
        echo -e "${GREEN}✅ SUCESSO: $script${NC}"
    else
        FAILED=$((FAILED + 1))
        echo -e "${RED}❌ ERRO: $script${NC}"
        echo -e "${YELLOW}⚠️  Continuando com próximo script...${NC}"
    fi
    
    echo "Fim: $(date '+%H:%M:%S')"
}

# Executar cada script
for script in "${SCRIPTS[@]}"; do
    if [ -f "$script" ]; then
        run_script "$script"
    else
        echo -e "${RED}❌ Script não encontrado: $script${NC}"
        FAILED=$((FAILED + 1))
    fi
done

# Upload para MinIO (opcional)
echo ""
echo "=========================================="
echo "📤 Upload para MinIO"
echo "=========================================="
if [ -f "zuq_upload_minio.py" ]; then
    if $PYTHON_BIN zuq_upload_minio.py; then
        echo -e "${GREEN}✅ Upload concluído${NC}"
    else
        echo -e "${YELLOW}⚠️  Erro no upload (pode ser que os arquivos não existam)${NC}"
    fi
else
    echo -e "${YELLOW}⚠️  Script zuq_upload_minio.py não encontrado${NC}"
fi

# Resumo final
echo ""
echo "=========================================="
echo "📊 RESUMO DA EXECUÇÃO"
echo "=========================================="
echo "Total de scripts: $TOTAL"
echo -e "${GREEN}Sucessos: $SUCCESS${NC}"
echo -e "${RED}Falhas: $FAILED${NC}"
echo "Data/Hora fim: $(date '+%Y-%m-%d %H:%M:%S')"
echo ""

if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}🎉 Todos os pipelines foram executados com sucesso!${NC}"
    exit 0
else
    echo -e "${YELLOW}⚠️  Alguns pipelines falharam. Verifique os logs acima.${NC}"
    exit 1
fi
