#!/bin/bash
# Script para aplicar Lifecycle Management Policy no Azure Storage Account
# 
# Este script configura políticas de retenção e otimização de custos
# movendo dados entre tiers conforme idade:
#   - Hot Tier (0-30 dias): Acesso frequente, custo mais alto
#   - Cool Tier (30-90 dias): Acesso ocasional, custo médio
#   - Archive Tier (90-730 dias): Acesso raro, custo mínimo
#   - Delete (> 730 dias / 2 anos): Remoção automática
#
# Requisitos:
#   - Azure CLI instalado e autenticado (az login)
#   - Permissão de Owner/Contributor no Storage Account
#
# Uso:
#   ./apply_lifecycle_policy.sh
#

set -e  # Parar em caso de erro

# ==============================================================================
# Configurações (ajuste conforme seu ambiente)
# ==============================================================================

STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT_NAME:-stticketsdatalake}"
RESOURCE_GROUP="${AZURE_RESOURCE_GROUP:-rg-airflow-datalake}"
POLICY_FILE="$(dirname "$0")/lifecycle-policy.json"

echo "================================================================================"
echo "Aplicação de Lifecycle Management Policy no Azure Storage Account"
echo "================================================================================"
echo "Storage Account: $STORAGE_ACCOUNT"
echo "Resource Group: $RESOURCE_GROUP"
echo "Policy File: $POLICY_FILE"
echo "================================================================================"
echo ""

# ==============================================================================
# Validações
# ==============================================================================

# Verificar se está logado no Azure
if ! az account show &> /dev/null; then
    echo "❌ Azure CLI não está autenticado."
    echo "   Execute: az login"
    exit 1
fi

echo "✅ Azure CLI autenticado"
echo ""

# Verificar se o arquivo de policy existe
if [ ! -f "$POLICY_FILE" ]; then
    echo "❌ Arquivo de policy não encontrado: $POLICY_FILE"
    exit 1
fi

echo "✅ Arquivo de policy encontrado"
echo ""

# Verificar se o Storage Account existe
if ! az storage account show \
    --name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --output none 2>/dev/null; then
    echo "❌ Storage Account '$STORAGE_ACCOUNT' não encontrado no Resource Group '$RESOURCE_GROUP'"
    exit 1
fi

echo "✅ Storage Account verificado"
echo ""

# ==============================================================================
# Mostrar política atual (se existir)
# ==============================================================================

echo "📋 Verificando política atual..."
if az storage account management-policy show \
    --account-name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --output table 2>/dev/null; then
    echo ""
    echo "⚠️ Política existente será SUBSTITUÍDA"
    echo ""
    read -p "Deseja continuar? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        echo "❌ Operação cancelada"
        exit 0
    fi
else
    echo "ℹ️ Nenhuma política existente (primeira configuração)"
fi

echo ""

# ==============================================================================
# Mostrar resumo da política a ser aplicada
# ==============================================================================

echo "📄 Resumo da Política a ser aplicada:"
echo "───────────────────────────────────────────────────────────────────────────────"
echo "Regra 1: MoveToArchiveAfter90Days"
echo "  - Hot → Cool: após 30 dias de modificação"
echo "  - Cool → Archive: após 90 dias de modificação"
echo "  - Delete: após 730 dias (2 anos) de modificação"
echo "  - Aplica-se a: tickets-data/ (todos os arquivos)"
echo ""
echo "Regra 2: DeleteOldCompactionFiles (opcional - requer blob index tags)"
echo "  - Delete: arquivos marcados como 'compaction_status=superseded' após 7 dias"
echo "───────────────────────────────────────────────────────────────────────────────"
echo ""

# ==============================================================================
# Aplicar política
# ==============================================================================

echo "🔄 Aplicando Lifecycle Management Policy..."
echo ""

if az storage account management-policy create \
    --account-name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --policy "@$POLICY_FILE"; then
    
    echo ""
    echo "✅ Lifecycle Management Policy aplicada com sucesso!"
    echo ""
else
    echo ""
    echo "❌ Erro ao aplicar Lifecycle Management Policy"
    exit 1
fi

# ==============================================================================
# Verificar política aplicada
# ==============================================================================

echo "📋 Política aplicada (verificação):"
echo "───────────────────────────────────────────────────────────────────────────────"
az storage account management-policy show \
    --account-name "$STORAGE_ACCOUNT" \
    --resource-group "$RESOURCE_GROUP" \
    --output json | jq '.policy.rules[] | {name: .name, enabled: .enabled, actions: .definition.actions}'

echo ""
echo "================================================================================"
echo "✅ CONFIGURAÇÃO CONCLUÍDA"
echo "================================================================================"
echo ""
echo "PRÓXIMOS PASSOS:"
echo "1. Monitorar custos de storage no Azure Portal (pode levar 24-48h para aplicar)"
echo "2. Verificar métricas de tier distribution no Storage Account"
echo "3. Ajustar política se necessário (editar lifecycle-policy.json e re-executar)"
echo ""
echo "OBSERVAÇÕES:"
echo "  - Políticas são avaliadas 1x por dia (horário variável)"
echo "  - Mudanças de tier podem levar até 24h para ocorrer"
echo "  - Archive Tier requer 'rehydration' (horas) para acesso"
echo "  - Delete após 2 anos é irreversível (faça backup se necessário)"
echo ""
echo "================================================================================"
