#!/bin/bash
# Script para configurar ACLs POSIX do Azure Data Lake Storage Gen2 por tenant
# ISOLAMENTO: Apenas ACLs, SEM RBAC no Storage Account
#
# Este script:
# 1. Lê a ACL atual de cada pasta de tenant
# 2. Adiciona entrada ACL para o Service Principal do cliente (preservando outras entradas)
# 3. Aplica recursivamente em arquivos/subpastas existentes
# 4. Define ACL padrão para novos arquivos
# 5. Configura permissão --x no diretório raiz (necessária para autenticação)
#
# IMPORTANTE: Service Principals NÃO devem ter RBAC no Storage Account
# - RBAC ignoraria ACLs e quebraria o isolamento
# - Isolamento funciona apenas com ACLs POSIX
#
# Uso:
#   ./scripts/setup_azure_acls.sh
#
# Requisitos:
#   - Azure CLI instalado e autenticado (az login)
#   - Variáveis de ambiente configuradas
#   - Permissões: Storage Blob Data Owner (para configurar ACLs)
#
# Autor: Airflow Team
# Data: 2026-01-16

set -e  # Parar em caso de erro

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Carregar variáveis de ambiente
if [ -f .env ]; then
    set -a
    source <(grep -v '^#' .env | grep -v '^$')
    set +a
fi

# Validar variáveis obrigatórias
REQUIRED_VARS=("AZURE_STORAGE_ACCOUNT_NAME" "AZURE_CONTAINER_NAME")
for var in "${REQUIRED_VARS[@]}"; do
    if [ -z "${!var}" ]; then
        echo -e "${RED}❌ Variável $var não definida no .env${NC}"
        exit 1
    fi
done

STORAGE_ACCOUNT="${AZURE_STORAGE_ACCOUNT_NAME}"
CONTAINER="${AZURE_CONTAINER_NAME}"

echo -e "${BLUE}=================================================="
echo "Azure Data Lake ACL Setup - Multi-tenant"
echo "MODELO: ACLs POSIX apenas (SEM RBAC)"
echo "=================================================="
echo "Storage Account: $STORAGE_ACCOUNT"
echo "Container: $CONTAINER"
echo -e "==================================================${NC}\n"

# Verificar se está autenticado
if ! az account show &> /dev/null; then
    echo -e "${RED}❌ Azure CLI não está autenticado. Execute: az login${NC}"
    exit 1
fi

echo -e "${GREEN}✅ Azure CLI autenticado${NC}"
CURRENT_USER=$(az account show --query user.name -o tsv)
echo -e "Usuário: ${BLUE}$CURRENT_USER${NC}\n"

# Verificar se HNS está habilitado (obrigatório para ACLs)
echo -e "${YELLOW}🔍 Verificando se Hierarchical Namespace está habilitado...${NC}"
HNS_ENABLED=$(az storage account show \
    --name "$STORAGE_ACCOUNT" \
    --query "isHnsEnabled" -o tsv)

if [ "$HNS_ENABLED" != "true" ]; then
    echo -e "${RED}❌ Hierarchical Namespace (HNS) NÃO está habilitado neste Storage Account!${NC}"
    echo -e "${RED}   ACLs POSIX requerem ADLS Gen2 (HNS habilitado).${NC}"
    echo -e "${YELLOW}   Para habilitar, recrie o Storage Account com --hierarchical-namespace true${NC}"
    exit 1
fi

echo -e "${GREEN}✅ HNS habilitado${NC}\n"

# Função para adicionar ACL a uma pasta de tenant
setup_tenant_acl() {
    local TENANT_ID=$1
    local OBJECT_ID=$2
    local TENANT_NAME=$3
    
    TENANT_FOLDER="tenant_${TENANT_ID}"
    
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}📁 Tenant: ${YELLOW}$TENANT_NAME${NC}"
    echo -e "${BLUE}   Tenant ID: $TENANT_ID${NC}"
    echo -e "${BLUE}   Pasta: $TENANT_FOLDER${NC}"
    echo -e "${BLUE}   Service Principal Object ID: $OBJECT_ID${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"
    
    # Verificar se a pasta existe
    FOLDER_EXISTS=false
    EXISTS_OUTPUT=$(az storage fs directory exists \
        --account-name "$STORAGE_ACCOUNT" \
        --file-system "$CONTAINER" \
        --name "$TENANT_FOLDER" \
        --auth-mode login 2>/dev/null)
    
    if echo "$EXISTS_OUTPUT" | grep -q '"exists": true'; then
        FOLDER_EXISTS=true
        echo -e "${GREEN}✅ Pasta $TENANT_FOLDER já existe${NC}\n"
    else
        echo -e "${YELLOW}⚠️  Pasta $TENANT_FOLDER não existe. Criando...${NC}"
        az storage fs directory create \
            --account-name "$STORAGE_ACCOUNT" \
            --file-system "$CONTAINER" \
            --name "$TENANT_FOLDER" \
            --auth-mode login
        
        if [ $? -eq 0 ]; then
            echo -e "${GREEN}✅ Pasta criada${NC}\n"
            FOLDER_EXISTS=true
        else
            echo -e "${RED}❌ Erro ao criar pasta${NC}"
            return 1
        fi
    fi
    
    # Ler ACL atual (se pasta existia antes, pode já ter ACLs)
    echo -e "${YELLOW}📋 Lendo ACL atual da pasta...${NC}"
    CURRENT_ACL=$(az storage fs access show \
        --account-name "$STORAGE_ACCOUNT" \
        --file-system "$CONTAINER" \
        --path "$TENANT_FOLDER" \
        --auth-mode login \
        --query "acl" -o tsv 2>&1)
    
    # Verificar se comando foi bem-sucedido
    if [ $? -ne 0 ] || [ -z "$CURRENT_ACL" ] || [[ "$CURRENT_ACL" == *"ERROR"* ]]; then
        echo -e "${YELLOW}⚠️  Não foi possível ler ACL (pasta nova ou sem ACL). Usando ACL padrão...${NC}"
        CURRENT_ACL="user::rwx,group::r-x,other::---"
        echo "   ACL padrão: $CURRENT_ACL"
    else
        echo "   ACL atual: $CURRENT_ACL"
    fi
    
    # Verificar se a entrada já existe
    if echo "$CURRENT_ACL" | grep -q "user:${OBJECT_ID}"; then
        echo -e "${YELLOW}⚠️  Service Principal já tem ACL configurada. Pulando...${NC}\n"
        return 0
    fi
    
    # Adicionar entrada ACL (preserva entradas existentes)
    echo -e "\n${YELLOW}🔧 Adicionando ACL para Service Principal...${NC}"
    az storage fs access set \
        --account-name "$STORAGE_ACCOUNT" \
        --file-system "$CONTAINER" \
        --path "$TENANT_FOLDER" \
        --acl "${CURRENT_ACL},user:${OBJECT_ID}:r-x" \
        --auth-mode login
    
    echo -e "${GREEN}✅ ACL adicionada na pasta raiz${NC}"
    
    # Aplicar recursivamente em arquivos/subpastas existentes
    echo -e "\n${YELLOW}🔄 Aplicando ACL recursivamente (arquivos existentes)...${NC}"
    az storage fs access update-recursive \
        --account-name "$STORAGE_ACCOUNT" \
        --file-system "$CONTAINER" \
        --path "$TENANT_FOLDER" \
        --acl "user:${OBJECT_ID}:r-x" \
        --auth-mode login \
        --continue-on-failure true 2>&1 | grep -E '(Successfully|Failed|directories|files)' || true
    
    echo -e "${GREEN}✅ ACL aplicada recursivamente${NC}"
    
    # Definir ACL padrão para novos arquivos/pastas
    echo -e "\n${YELLOW}🔄 Configurando ACL padrão (novos arquivos)...${NC}"
    az storage fs access set-recursive \
        --account-name "$STORAGE_ACCOUNT" \
        --file-system "$CONTAINER" \
        --path "$TENANT_FOLDER" \
        --acl "default:user::rwx,default:user:${OBJECT_ID}:r-x,default:group::---,default:other::---" \
        --auth-mode login \
        --continue-on-failure true 2>&1 | grep -E '(Successfully|Failed|directories|files)' || true
    
    echo -e "${GREEN}✅ ACL padrão configurada${NC}"
    
    # Mostrar ACL final
    echo -e "\n${YELLOW}📋 ACL final da pasta:${NC}"
    FINAL_ACL=$(az storage fs access show \
        --account-name "$STORAGE_ACCOUNT" \
        --file-system "$CONTAINER" \
        --path "$TENANT_FOLDER" \
        --auth-mode login \
        --query "acl" -o tsv)
    echo "   $FINAL_ACL"
    
    # Configurar permissão --x no diretório raiz (necessária para autenticação)
    echo -e "\n${YELLOW}🔑 Configurando permissão no diretório raiz (--x)...${NC}"
    echo "   Necessário para Service Principal atravessar até a pasta do tenant"
    
    # Ler ACL atual do raiz
    ROOT_ACL=$(az storage fs access show \
        --account-name "$STORAGE_ACCOUNT" \
        --file-system "$CONTAINER" \
        --path "/" \
        --auth-mode login \
        --query "acl" -o tsv 2>/dev/null)
    
    # Verificar se já tem permissão --x no raiz
    if echo "$ROOT_ACL" | grep -q "user:${OBJECT_ID}"; then
        echo -e "${YELLOW}⚠️  Service Principal já tem permissão no diretório raiz. Pulando...${NC}"
    else
        # Adicionar --x ao diretório raiz
        NEW_ROOT_ACL="${ROOT_ACL},user:${OBJECT_ID}:--x"
        az storage fs access set \
            --account-name "$STORAGE_ACCOUNT" \
            --file-system "$CONTAINER" \
            --path "/" \
            --acl "$NEW_ROOT_ACL" \
            --auth-mode login
        
        echo -e "${GREEN}✅ Permissão --x adicionada no diretório raiz${NC}"
    fi
    
    echo -e "\n${GREEN}✅ ACLs configuradas para $TENANT_NAME${NC}\n"
}

# Modo de uso: interativo ou via argumentos
if [ $# -eq 0 ]; then
    # Modo interativo
    echo -e "${YELLOW}📝 Modo Interativo${NC}\n"
    
    read -p "Tenant ID (UUID): " TENANT_ID
    read -p "Service Principal Object ID: " OBJECT_ID
    read -p "Nome da Tenant (opcional): " TENANT_NAME
    
    if [ -z "$TENANT_NAME" ]; then
        TENANT_NAME="Tenant $TENANT_ID"
    fi
    
    setup_tenant_acl "$TENANT_ID" "$OBJECT_ID" "$TENANT_NAME"
    
elif [ "$1" == "--batch" ]; then
    # Modo batch - ler de arquivo CSV
    # Formato CSV: tenant_id,object_id,tenant_name
    BATCH_FILE="${2:-tenants_acls.csv}"
    
    if [ ! -f "$BATCH_FILE" ]; then
        echo -e "${RED}❌ Arquivo $BATCH_FILE não encontrado${NC}"
        echo -e "${YELLOW}Formato esperado (CSV):${NC}"
        echo "tenant_id,object_id,tenant_name"
        echo "123e4567-e89b-12d3-a456-426614174000,xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx,Tenant X"
        exit 1
    fi
    
    echo -e "${YELLOW}📦 Modo Batch - processando $BATCH_FILE${NC}\n"
    
    # Pular header
    tail -n +2 "$BATCH_FILE" | while IFS=, read -r TENANT_ID OBJECT_ID TENANT_NAME; do
        # Remover espaços e aspas
        TENANT_ID=$(echo "$TENANT_ID" | tr -d ' "')
        OBJECT_ID=$(echo "$OBJECT_ID" | tr -d ' "')
        TENANT_NAME=$(echo "$TENANT_NAME" | tr -d '"')
        
        if [ -z "$TENANT_ID" ] || [ -z "$OBJECT_ID" ]; then
            echo -e "${YELLOW}⚠️  Linha inválida, pulando...${NC}\n"
            continue
        fi
        
        setup_tenant_acl "$TENANT_ID" "$OBJECT_ID" "$TENANT_NAME"
        sleep 2  # Evitar throttling
    done
    
else
    # Modo argumentos diretos
    TENANT_ID=$1
    OBJECT_ID=$2
    TENANT_NAME=${3:-"Tenant $TENANT_ID"}
    
    setup_tenant_acl "$TENANT_ID" "$OBJECT_ID" "$TENANT_NAME"
fi

echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
echo -e "${GREEN}✅ Setup de ACLs concluído!${NC}"
echo -e "${GREEN}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}\n"

echo -e "${YELLOW}💡 Dicas:${NC}"
echo "   • Teste o isolamento tentando acessar pasta de outra tenant"
echo "   • Configure alertas no Azure Monitor para acessos negados (403)"
echo "   • Documente as credenciais (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET) em local seguro"
echo "   • Forneça ao cliente o arquivo .pbids para Power BI Desktop"
echo ""
echo -e "${YELLOW}📚 Próximos passos:${NC}"
echo "   1. Testar exportação: docker-compose exec airflow-scheduler airflow dags test export_tickets_to_azure_datalake"
echo "   2. Validar isolamento: az storage fs file list --account-name \$STORAGE_ACCOUNT --file-system \$CONTAINER --path tenant_XXX"
echo "   3. Gerar documentação para cliente: scripts/generate_user_configs.py"
echo ""
