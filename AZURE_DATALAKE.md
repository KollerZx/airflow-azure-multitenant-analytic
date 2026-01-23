# Azure Data Lake Storage Gen2 - Integração Multi-Tenant

## 📋 Visão Geral

Esta documentação descreve como exportar dados do PostgreSQL para Azure Data Lake Storage Gen2 (ADLS Gen2) com **isolamento físico por tenant** usando `tenant_id`. Cada cliente terá acesso apenas aos seus próprios dados através de **Service Principals dedicados** com permissões ACL no nível de diretório.

**Arquitetura:**

```
PostgreSQL (Materialized Views)
    ↓ Airflow DAG (a cada 15 minutos)
Azure Data Lake Storage Gen2
    ├── tenant_<uuid_1>/
    │   ├── tickets/format=parquet/year=2026/month=01/day=14/*.parquet
    │   └── tickets/format=csv/year=2026/month=01/day=14/*.csv
    ├── tenant_<uuid_2>/
    │   └── ...
    └── tenant_<uuid_N>/
        └── ...
```

**Benefícios:**

- ✅ **Isolamento físico**: Cada tenant tem sua própria pasta isolada
- ✅ **Segurança**: ACLs do Azure garantem que tenant X não veja dados da tenant Y
- ✅ **Self-service**: Clientes consomem dados com suas próprias ferramentas (Power BI Service, Python, Excel, Tableau, Synapse)
- ✅ **Múltiplos formatos**: Parquet (otimizado para analytics) + CSV (compatível Excel)
- ✅ **Performance**: Formato Parquet com compressão Snappy otimizado para analytics
- ✅ **Particionamento**: Partições por data e formato facilitam queries incrementais
- ✅ **Exportação Incremental**: Apenas dados novos são exportados (watermark por tenant)
- ✅ **Compactação Automática**: Consolidação diária de arquivos reduz custos
- ✅ **Retenção Inteligente**: Lifecycle policies movem dados entre tiers automaticamente

---

## 🚀 Configuração Passo a Passo

### 1. Criar Storage Account no Azure

```bash
# Variáveis
RESOURCE_GROUP="rg-airflow-datalake"
LOCATION="eastus"
STORAGE_ACCOUNT="stticketsdatalake"  # deve ser único globalmente
CONTAINER_NAME="tickets-data"

# Login no Azure
az login

# Criar Resource Group
az group create \
  --name $RESOURCE_GROUP \
  --location $LOCATION

# Criar Storage Account com Data Lake Gen2 habilitado
az storage account create \
  --name $STORAGE_ACCOUNT \
  --resource-group $RESOURCE_GROUP \
  --location $LOCATION \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true \
  --access-tier Hot

# Criar Container
az storage container create \
  --name $CONTAINER_NAME \
  --account-name $STORAGE_ACCOUNT \
  --auth-mode login
```

---

## ✅ Modelo de Controle de Acesso: Apenas ACLs POSIX

O Azure Data Lake Gen2 oferece **isolamento multi-tenant verdadeiro** usando **apenas ACLs POSIX**, sem RBAC no nível do Storage Account.

### Como Funciona a Avaliação

Segundo a [documentação oficial da Microsoft](https://learn.microsoft.com/pt-br/azure/storage/blobs/data-lake-storage-access-control-model):

1. **Azure RBAC é avaliado primeiro**: Se houver role RBAC no Storage Account (ex: "Storage Blob Data Reader") que conceda acesso suficiente, **as ACLs são ignoradas** ❌
2. **ACLs POSIX são avaliadas**: Apenas se não houver RBAC ou se RBAC não conceder acesso suficiente ✅

> 🔑 **Chave para Isolamento**: NÃO atribuir RBAC no Storage Account aos Service Principals de clientes

### 1️⃣ **ACLs POSIX** (nível arquivo/pasta)

- **Onde:** Pastas e arquivos específicos no Data Lake
- **Formato:** `user:<objectId>:r-x` (read + execute)
- **Como configurar:** Script `scripts/setup_azure_acls.sh` ou comando `az storage fs access set`
- **Resultado:** ✅ Isolamento verdadeiro entre tenants

### 2️⃣ **Permissão Mínima no Diretório Raiz**

Para autenticação Power BI, Service Principal precisa de `--x` (execute-only) no diretório raiz:

```bash
# Permite atravessar até a pasta da tenant, mas não listar raiz
az storage fs access set \
  --permissions "user:<objectId>:--x" \
  --path "/" \
  --file-system "tickets-data" \
  --account-name "stticketsdatalake" \
  --auth-mode login
```

### ⚠️ IMPORTANTE: Airflow Service Principal

Apenas o Service Principal do Airflow (exportação) deve ter RBAC "Storage Blob Data Contributor" no Storage Account, pois precisa criar/escrever em todas as pastas.

**Service Principals de clientes: APENAS ACLs, SEM RBAC**

---

### 2. Criar Service Principal Principal (Exportação Airflow)

```bash
# Service Principal que o Airflow usará para exportar dados
SP_NAME="sp-airflow-datalake-export"
SUBSCRIPTION_ID=$(az account show --query id -o tsv)
RESOURCE_GROUP="rg-airflow-datalake"  # Seu Resource Group

# Criar Service Principal com role RBAC já atribuída
az ad sp create-for-rbac \
  --name $SP_NAME \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT \
  --years 2  # Secret expira em 2 anos

# Retorno (GUARDAR ESSAS CREDENCIAIS NO .env):
# {
#   "appId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",        # AZURE_CLIENT_ID
#   "password": "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",      # AZURE_CLIENT_SECRET
#   "tenant": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"       # AZURE_TENANT_ID
# }

# ⚠️ IMPORTANTE: 
# - O CLIENT_SECRET só é exibido uma vez!
# - Expira em 2 anos (agendar renovação para 01/2028)
# - Adicionar ao arquivo .env do projeto
```

⚠️ **Nota:** Este comando já atribui a role RBAC "Storage Blob Data Contributor" automaticamente com `--role` e `--scopes`.

### 3. Criar Service Principal por Tenant (Leitura)

**Para cada cliente**, crie um Service Principal com acesso SOMENTE à sua pasta:

```bash
# Exemplo para Tenant Empresa X (tenant_id = 123e4567-e89b-12d3-a456-426614174000)
SP_NAME="sp-client-empresaX-read"
TENANT_FOLDER="tenant_8c1dca21-ba17-5018-b56d-cf6395d413e5"

# Criar Service Principal
az ad sp create-for-rbac \
  --name $SP_NAME \
  --skip-assignment \
  --years 2  # Secret expira em 2 anos

# ⚠️ IMPORTANTE: Guardar CLIENT_ID e CLIENT_SECRET retornados!

# Obter IDs do Service Principal
APP_ID=$(az ad sp list --display-name $SP_NAME --query "[0].appId" -o tsv)
OBJECT_ID=$(az ad sp list --display-name $SP_NAME --query "[0].id" -o tsv)
echo "App ID (Client ID): $APP_ID"
echo "Object ID (para ACLs): $OBJECT_ID"

# 1. Atribuir role RBAC no Storage Account (obrigatório!)
echo "\n🔐 Atribuindo permissão RBAC no Storage Account..."
SUBSCRIPTION_ID=$(az account show --query id -o tsv)
RESOURCE_GROUP="rg-airflow-datalake"  # Seu Resource Group

az role assignment create \
  --assignee $APP_ID \
  --role "Storage Blob Data Reader" \
  --scope "/subscriptions/$SUBSCRIPTION_ID/resourceGroups/$RESOURCE_GROUP/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT"

echo "✅ Role RBAC atribuída"

# 2. Configurar ACLs POSIX na pasta específica
echo "\n📋 Configurando ACLs na pasta $TENANT_FOLDER..."

# Verificar ACL atual da pasta
az storage fs access show \
  --account-name $STORAGE_ACCOUNT \
  --file-system $CONTAINER_NAME \
  --path $TENANT_FOLDER \
  --auth-mode login \
  --query "acl" -o tsv

# Ler ACL atual e adicionar entrada do Service Principal (preserva outras entradas)
CURRENT_ACL=$(az storage fs access show \
  --account-name $STORAGE_ACCOUNT \
  --file-system $CONTAINER_NAME \
  --path $TENANT_FOLDER \
  --auth-mode login \
  --query "acl" -o tsv)

# Adicionar permissão SOMENTE para este Service Principal na pasta da tenant
az storage fs access set \
  --account-name $STORAGE_ACCOUNT \
  --file-system $CONTAINER_NAME \
  --path $TENANT_FOLDER \
  --acl "${CURRENT_ACL},user:${OBJECT_ID}:r-x" \
  --auth-mode login

echo "✅ ACLs POSIX configuradas"

# Aplicar permissão recursivamente em TODOS os arquivos/subpastas existentes
echo "\n🔄 Aplicando ACL recursivamente (pode demorar para muitos arquivos)..."
az storage fs access update-recursive \
  --account-name $STORAGE_ACCOUNT \
  --file-system $CONTAINER_NAME \
  --path $TENANT_FOLDER \
  --acl "user:${OBJECT_ID}:r-x" \
  --auth-mode login \
  --continue-on-failure true

echo "✅ Permissão aplicada recursivamente"

# Definir ACL padrão para que NOVOS arquivos/pastas herdem a permissão automaticamente
echo "\n🔄 Configurando ACL padrão para novos arquivos..."
az storage fs access set-recursive \
  --account-name $STORAGE_ACCOUNT \
  --file-system $CONTAINER_NAME \
  --path $TENANT_FOLDER \
  --acl "default:user:${OBJECT_ID}:r-x" \
  --auth-mode login \
  --continue-on-failure true

echo "✅ ACL padrão configurada"

# Verificar ACL final
echo "\n📋 ACL final da pasta $TENANT_FOLDER:"
az storage fs access show \
  --account-name $STORAGE_ACCOUNT \
  --file-system $CONTAINER_NAME \
  --path $TENANT_FOLDER \
  --auth-mode login
```

**🚀 Script automatizado:** Use `scripts/setup_azure_acls.sh` para configurar ACLs de forma idempotente:

```bash
# Modo interativo (uma tenant por vez)
./scripts/setup_azure_acls.sh

# Modo direto (argumentos)
./scripts/setup_azure_acls.sh <tenant_id> <object_id> "Nome da Tenant"

# Modo batch (múltiplas tenants de uma vez via CSV)

./scripts/setup_azure_acls.sh --batch scripts/tenants_acls.csv
```

**Formato do CSV** (`tenants_acls.csv`):

```csv
tenant_id,object_id,tenant_name
8c1dca21-ba17-5018-b56d-cf6395d413e5,xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx,Empresa X 
a1b2c3d4-e5f6-7890-abcd-ef1234567890,yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy,Empresa B Parts
```

**Script de geração completa:** Veja `scripts/generate_azure_service_principals.py` para gerar Service Principals E configurar ACLs para todas as tenants automaticamente.

### 4. Configurar Variáveis de Ambiente

Adicione ao arquivo `.env`:

```bash
# Azure Data Lake Storage Gen2
AZURE_STORAGE_ACCOUNT_NAME=stticketsdatalake
AZURE_CONTAINER_NAME=tickets-data
AZURE_TENANT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
AZURE_CLIENT_ID=xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
AZURE_CLIENT_SECRET=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

### 5. Instalar Dependências no Airflow

Adicione ao `docker-compose.yaml`:

```yaml
x-airflow-common:
  &airflow-common
  build:
    context: .
    dockerfile: Dockerfile
  environment:
    # ... variáveis existentes ...
    AZURE_STORAGE_ACCOUNT_NAME: ${AZURE_STORAGE_ACCOUNT_NAME}
    AZURE_CONTAINER_NAME: ${AZURE_CONTAINER_NAME}
    AZURE_TENANT_ID: ${AZURE_TENANT_ID}
    AZURE_CLIENT_ID: ${AZURE_CLIENT_ID}
    AZURE_CLIENT_SECRET: ${AZURE_CLIENT_SECRET}
```

Crie/atualize o `Dockerfile`:

```dockerfile
FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y postgresql-client

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-microsoft-azure==8.7.0 \
    azure-storage-file-datalake==12.14.0 \
    pyarrow==14.0.2 \
    pandas==2.1.4
```

### 6. Deploy do DAG de Exportação

O DAG completo está em `dags/export_to_azure_datalake.py`.

**Rebuild do Airflow:**

```bash
docker-compose down
docker-compose build
docker-compose up -d
```

---

## 👥 Consumo pelos Clientes

> 🔒 **IMPORTANTE - Autenticação com ACLs:** O projeto utiliza **ACLs POSIX sem RBAC** para isolamento multi-tenant. Esta abordagem garante segurança máxima, mas possui uma limitação: **Power BI Desktop requer RBAC** para autenticação OAuth. Por isso, **recomendamos Power BI Service** como primeira opção.

### 1. Power BI Service (Recomendado)

**Vantagens:**

- ✅ Funciona nativamente com Service Principal + ACLs (sem RBAC)
- ✅ Mantém isolamento de segurança perfeito
- ✅ Permite exportar relatório .pbix para Desktop após criar

**Passos no Power BI Service (app.powerbi.com):**

1. **Get Data** → **Azure Data Lake Storage Gen2**
2. URL Parquet: `https://stticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/tickets/format=parquet/`
3. Autenticação: **Service Principal**
   - Tenant ID: `xxx`
   - Service Principal ID: `yyy`
   - Service Principal Key: `zzz`
4. Use botão **"Combine"** ou script Power Query para combinar Parquet
5. Crie relatório e dashboards
6. **Opcional:** Baixe .pbix e abra no Desktop

### 2. Power BI Desktop (Uso Secundário)

**Arquivo de conexão para o cliente (exemplo: `Empresa X_DataLake_Connection.pbids`):**

```json
{
  "version": "0.1",
  "connections": [
    {
      "details": {
        "protocol": "abfss",
        "address": {
          "url": "abfss://tickets-data@stticketsdatalake.dfs.core.windows.net/tenant_123e4567-e89b-12d3-a456-426614174000/"
        },
        "authentication": {
          "method": "servicePrincipal",
          "tenantId": "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
          "clientId": "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
        }
      },
      "mode": "DirectQuery",
      "pbiServicePrincipal": {
        "clientSecret": "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
      }

    }
  ]
}
```

**Passos no Power BI Desktop:**

> ⚠️ **ATENÇÃO:** Criar conexão nova no Desktop falhará com "Access forbidden". **Sempre crie primeiro no Power BI Service**, depois baixe o .pbix.

**Se já possui .pbix do Service:**

1. Abra o arquivo .pbix baixado do Power BI Service
2. As conexões já estarão configuradas
3. Edite visualizações conforme necessário
4. Republique no Service quando finalizar

**Para criar nova conexão (requer RBAC - NÃO recomendado):**

1. **Get Data** → **More** → **Azure Data Lake Storage Gen2**
2. URL: `https://stticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/`
3. Autenticação: **Service Principal**
   - Tenant ID: `xxx`
   - Client ID: `yyy`
   - Client Secret: `zzz`
4. Navegar para a pasta `tickets/`
5. Combinar arquivos Parquet por pasta
6. Criar relacionamentos e dashboards

### 3. Python (Pandas)

```python
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import pandas as pd
import io

# Credenciais fornecidas ao cliente
TENANT_ID = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
CLIENT_ID = "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
CLIENT_SECRET = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
STORAGE_ACCOUNT = "stticketsdatalake"
CONTAINER = "tickets-data"
TENANT_ID = "123e4567-e89b-12d3-a456-426614174000"

# Autenticação
credential = ClientSecretCredential(TENANT_ID, CLIENT_ID, CLIENT_SECRET)
account_url = f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
service_client = DataLakeServiceClient(account_url=account_url, credential=credential)

# Ler arquivo Parquet
file_system_client = service_client.get_file_system_client(CONTAINER)
file_path = f"tenant_{TENANT_ID}/tickets/format=parquet/year=2026/month=01/day=22/tickets_20260122_150000.parquet"
file_client = file_system_client.get_file_client(file_path)

# Download e leitura
download = file_client.download_file()
parquet_data = download.readall()
df = pd.read_parquet(io.BytesIO(parquet_data))

print(f"Total de tickets: {len(df)}")
print(df.head())
```

### 3. Azure Synapse Analytics

```sql
-- External Table no Synapse
CREATE EXTERNAL DATA SOURCE TicketsDataLake
WITH (
    TYPE = HADOOP,
    LOCATION = 'abfss://tickets-data@stticketsdatalake.dfs.core.windows.net/tenant_123e4567-e89b-12d3-a456-426614174000/'
);

CREATE EXTERNAL FILE FORMAT ParquetFormat
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);

CREATE EXTERNAL TABLE dbo.tickets_external (
    tenant_id VARCHAR(50),
    ticket_id VARCHAR(50),
    ticket_number INT,
    title VARCHAR(500),
    status VARCHAR(50),
    created_at DATETIME2
)
WITH (
    LOCATION = 'tickets/format=parquet/year=2026/month=01/day=*/*.parquet',
    DATA_SOURCE = TicketsDataLake,
    FILE_FORMAT = ParquetFormat
);

-- Query
SELECT status, COUNT(*) as total
FROM dbo.tickets_external
WHERE created_at >= '2026-01-01'
GROUP BY status;
```

### 4. Tableau

1. **Connect** → **To a Server** → **Azure Data Lake Storage Gen2**
2. Fornecer credenciais do Service Principal
3. Selecionar container e pasta `tenant_<uuid>/`
4. Importar arquivos Parquet
5. Criar visualizações

### 5. Excel (Power Query)

```m
let
    Source = AzureStorage.DataLake("https://stticketsdatalake.dfs.core.windows.net/tickets-data/tenant_123e4567-e89b-12d3-a456-426614174000/tickets/format=csv/"),
    tickets_folder = Source{[Name="tickets"]}[Content],
    #"Filtered Rows" = Table.SelectRows(tickets_folder, each Text.EndsWith([Name], ".parquet")),
    #"Combined Files" = Table.Combine(List.Transform(#"Filtered Rows"[Content], each Parquet.Document(_)))
in
    #"Combined Files"
```

---

## 🔒 Segurança e Isolamento

### ACLs por Tenant

Cada Service Principal do cliente tem permissões **SOMENTE** na pasta de sua tenant:

```bash
# ACL da tenant Empresa X (exemplo)
az storage fs access show \
  --account-name stticketsdatalake \
  --file-system tickets-data \
  --path tenant_123e4567-e89b-12d3-a456-426614174000 \
  --auth-mode login

# Resultado:

# {
#   "acl": "user::rwx,user:sp-empresaX-object-id:r-x,group::r-x,other::---",
#   "permissions": "rwxr-x---"
# }
```

**Princípios:**

- ✅ Service Principal da tenant X **NÃO CONSEGUE** listar ou acessar pasta de tenant Y
- ✅ Service Principal do Airflow tem `Storage Blob Data Contributor` (necessário para criar/atualizar)
- ✅ Service Principals dos clientes têm `r-x` (read + execute) APENAS em suas pastas
- ✅ Logs de acesso no Azure Monitor rastreiam quem acessou o quê e quando

### Teste de Isolamento

```bash
# Tentar acessar pasta de outra tenant deve falhar
az storage fs file show \
  --account-name stticketsdatalake \
  --file-system tickets-data \
  --path tenant_<outro_uuid>/tickets/format=<formato>/year=2026/month=01/day=14/data.parquet \
  --auth-mode key

# Erro esperado: 403 Forbidden - This request is not authorized to perform this operation
```

---

## � Exportação Incremental e Compactação

### Estratégia de Exportação Otimizada

O pipeline implementa exportação incremental baseada em **watermark** para evitar duplicação de dados:

**Como Funciona:**

1. **Tabela de Controle** (`export_watermark`): Rastreia o último `created_at` exportado por tenant
2. **Query Incremental**: Exporta apenas tickets com `created_at > última_exportação`
3. **Fallback**: Se watermark não existir (primeira exportação), usa janela de 30 minutos
4. **Atualização Automática**: Watermark é atualizado após exportação bem-sucedida

**Benefícios:**

- ✅ **Elimina duplicatas**: Cada ticket é exportado apenas uma vez
- ✅ **Reduz custos**: ~96x menos write operations no Azure
- ✅ **Melhora performance**: Queries no Power BI são mais rápidas
- ✅ **Rastreabilidade**: Histórico completo de exportações por tenant

### Configuração Inicial

**1. Criar tabela de controle:**

```bash
# Executar script SQL no PostgreSQL
docker compose exec -i airflow-postgres psql -U airflow -d airflow_db < sql/04_create_export_watermark.sql
```

**2. Verificar configuração:**

```sql
-- Ver status de exportação por tenant
SELECT * FROM vw_export_watermark_status;

-- Tenants com exportação atrasada (> 1 hora)
SELECT * FROM vw_export_watermark_status WHERE hours_since_last_export > 1;
```

### DAG de Compactação Diária

**Problema:** Mesmo com exportação incremental, podem ser gerados múltiplos arquivos por dia devido a múltiplas execuções do DAG.

**Solução:** DAG `azure_datalake_compaction` consolida arquivos do dia anterior (D-1) em arquivo único:

#### 📋 Especificações Técnicas

- **Arquivo:** [`dags/azure_datalake_compaction.py`](dags/azure_datalake_compaction.py)
- **Schedule:** `0 2 * * *` (Diariamente às 2:00 AM)
- **Execução:** D-1 (dia anterior)
- **Catchup:** False (não reprocessa histórico)
- **Tags:** `['azure', 'datalake', 'compaction', 'maintenance', 'parquet']`

#### 🔄 Fluxo de Processamento

1. **Descoberta:**
   - Lista tenants com diretórios no Azure (`tenant_*`)
   - Para cada tenant, identifica arquivos do dia anterior (D-1)
   - Exemplo: `tenant_abc123/tickets/format=parquet/year=2026/month=01/day=20/*.parquet`

2. **Download e Consolidação:**
   - Baixa todos os arquivos Parquet do D-1
   - Combina em um único DataFrame usando `pd.concat()`
   - Remove duplicatas com `drop_duplicates()` (se houver)

3. **Upload:**
   - Gera arquivo consolidado: `tickets_YYYYMMDD_consolidated.parquet`
   - Compressão: Snappy (padrão Parquet)
   - Engine: PyArrow
   - Sobrescreve se já existir (`overwrite=True`)

4. **Validação:**
   - Re-download do arquivo consolidado
   - Verifica número de linhas: `len(df_validation) == rows_after_dedup`
   - Garante integridade dos dados

5. **Limpeza:**
   - Remove arquivos individuais apenas após validação
   - Mantém arquivo consolidado
   - Registra estatísticas no `bi_refresh_log`

#### 📊 Exemplo de Execução

```
========================================
COMPACTAÇÃO DE ARQUIVOS AZURE DATA LAKE
========================================
Data alvo: 2026-01-20 (D-1)
Execução: 2026-01-21 02:00:00
========================================

📂 Tenants a processar: 5

[1/5] Processando tenant: abc-123-uuid
========================================
Compactando: abc-123-uuid - 2026/01/20
========================================
📂 Encontrados 96 arquivos para compactar
   ✅ tickets_20260120_174500.parquet: 1,234 rows
   ✅ tickets_20260120_180000.parquet: 856 rows
   ... (94 arquivos)

🔄 Consolidando 96 DataFrames...
⚠️ Removidas 124 linhas duplicadas
📊 Total consolidado: 118,456 rows
✅ Uploaded 118,456 rows (45.2 MB) to tickets_20260120_consolidated.parquet
✅ Validação bem-sucedida

🗑️ Removendo 96 arquivos individuais...
   ✅ Removido: tickets_20260120_174500.parquet
   ... (95 arquivos)
✅ Compactação concluída: 96 arquivos → 1 arquivo consolidado

========================================
📊 RESUMO DA COMPACTAÇÃO
========================================
✅ Tenants compactadas: 5/5
ℹ️ Tenants ignoradas: 0
❌ Tenants com erro: 0
📂 Total de arquivos: 480 → 5
💾 Redução de arquivos: 99.0%
========================================
```

#### 🎯 Benefícios

- ✅ **Redução de arquivos**: 96 arquivos/dia → 1 arquivo/dia
- ✅ **Economia de custos**: Menos operações de leitura no Azure (~USD 0.004/10.000 operações)
- ✅ **Performance**: Power BI combina 1 arquivo em vez de 96 (10-15x mais rápido)
- ✅ **Validação**: Verifica integridade antes de remover originais
- ✅ **Duplicatas**: Remove automaticamente registros duplicados
- ✅ **Auditoria**: Registra estatísticas no banco de dados

#### 📈 Monitoramento

**View SQL para histórico de compactações:**

```sql
-- Ver histórico de compactações (últimas 10)
SELECT 
    refresh_timestamp,
    tenant_id,
    files_created,
    files_deleted,
    duplicates_removed,
    rows_processed,
    status
FROM vw_compaction_history
ORDER BY refresh_timestamp DESC
LIMIT 10;
```

**Estatísticas agregadas:**

```sql
-- Estatísticas de compactação (últimos 7 dias)
SELECT 
    DATE(refresh_timestamp) as compaction_date,
    COUNT(DISTINCT tenant_id) as tenants_compacted,
    SUM(files_deleted) as total_files_removed,
    SUM(duplicates_removed) as total_duplicates_removed,
    SUM(rows_processed) as total_rows_processed
FROM bi_refresh_log
WHERE export_type = 'COMPACTION'
  AND refresh_timestamp >= NOW() - INTERVAL '7 days'
GROUP BY DATE(refresh_timestamp)
ORDER BY compaction_date DESC;
```

**Comandos Airflow:**

```bash
# Verificar DAG está ativo
docker compose exec airflow-webserver airflow dags list | grep azure_datalake_compaction

# Trigger manual (útil para testar)
docker compose exec airflow-webserver airflow dags trigger azure_datalake_compaction

# Ver logs de execução
docker compose exec airflow-webserver airflow tasks logs azure_datalake_compaction compact_datalake_files <execution_date>

# Ver próxima execução agendada
docker compose exec airflow-webserver airflow dags next-execution azure_datalake_compaction
```

#### ⚠️ Considerações

**Quando a compactação é ignorada:**

- Nenhum arquivo encontrado para o D-1 (tenant sem dados naquele dia)
- Apenas 1 arquivo encontrado (já consolidado)
- Tenant não tem diretório no Azure

**Cenários de erro:**

- Falha no download de algum arquivo (registra erro, não processa tenant)
- Erro no upload do arquivo consolidado (mantém originais)
- Validação falha (não remove originais)

**Segurança:**

- Validação obrigatória antes de deletar originais
- Transação "all-or-nothing" por tenant
- Logs completos registrados em `bi_refresh_log`

**Performance:**

- Processa tenants sequencialmente (evita sobrecarga)
- Download paralelo de arquivos dentro de uma tenant (via Azure SDK)
- Consolidação em memória (requer RAM suficiente)

#### 🔧 Configuração Adicional

**Variáveis de ambiente necessárias (`.env`):**

```bash
# Azure Data Lake
AZURE_STORAGE_ACCOUNT_NAME=stticketsdatalake
AZURE_CONTAINER_NAME=tickets-data
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret

# PostgreSQL
POSTGRES_CONN_ID=postgres_prod
POSTGRES_SCHEMA=public
```

**Dependências Python:**

```dockerfile
# Já incluídas no Dockerfile
RUN pip install \
    azure-storage-file-datalake \
    azure-identity \
    pandas \
    pyarrow
```

---

## 📦 Retenção e Lifecycle Management

### Política de Retenção Implementada

O Azure Storage Account está configurado com **Lifecycle Management Policies** para otimizar custos automaticamente:

**Regra 1: MoveToArchiveAfter90Days**

- **Hot Tier** (0-30 dias): Dados recentes, acesso frequente
- **Cool Tier** (30-90 dias): Dados intermediários, acesso ocasional
- **Archive Tier** (90-730 dias): Dados antigos, acesso raro
- **Delete** (> 730 dias / 2 anos): Remoção automática

**Regra 2: DeleteOldCompactionFiles** (opcional)

- Remove arquivos marcados como `compaction_status=superseded` após 7 dias
- Requer uso de Blob Index Tags no upload

### Aplicar Lifecycle Policy

**1. Revisar política:**

```bash
cat scripts/lifecycle-policy.json
```

**2. Aplicar no Azure:**

```bash
# Executar script de aplicação
./scripts/apply_lifecycle_policy.sh
```

**3. Verificar política aplicada:**

```bash
az storage account management-policy show \
  --account-name stticketsdatalake \
  --resource-group rg-airflow-datalake \
  --output json | jq '.policy.rules'
```

### Personalizar Política de Retenção

**Ajustar períodos no arquivo `lifecycle-policy.json`:**

```json
{
  "rules": [
    {
      "name": "MoveToArchiveAfter90Days",
      "definition": {
        "actions": {
          "baseBlob": {
            "tierToCool": {
              "daysAfterModificationGreaterThan": 30  // ← Ajustar aqui
            },
            "tierToArchive": {
              "daysAfterModificationGreaterThan": 90  // ← Ajustar aqui
            },
            "delete": {
              "daysAfterModificationGreaterThan": 730  // ← Ajustar aqui (2 anos padrão)
            }
          }
        }
      }
    }
  ]
}
```

**Depois de editar, reaplique:**

```bash
./scripts/apply_lifecycle_policy.sh
```

### Backup Antes de Deletar

Para evitar perda de dados, configure backup antes da deleção automática:

```bash
# Opção 1: Azure Blob Snapshot (manual)
az storage blob snapshot \
  --account-name stticketsdatalake \
  --container-name tickets-data \
  --name tenant_<uuid>/tickets/format=parquet/year=2024/month=01/day=01/tickets_consolidated.parquet

# Opção 2: Replicação para outra conta (automatizado)
# Configure Geo-redundant storage (GRS) ou Object Replication
az storage account update \
  --name stticketsdatalake \
  --resource-group rg-airflow-datalake \
  --sku Standard_GRS
```

---

## 📊 Monitoramento de Exportações

### Queries Úteis de Monitoramento

**1. Estatísticas por tenant:**

```sql
SELECT * FROM vw_export_stats_by_tenant;
```

**2. Exportações recentes (últimas 24h):**

```sql
SELECT * FROM vw_recent_exports;
```

**3. Falhas de exportação (últimos 7 dias):**

```sql
SELECT * FROM vw_export_failures;
```

**4. Resumo diário de exportações:**

```sql
SELECT 
    DATE(refresh_timestamp) as export_date,
    COUNT(*) as total_exports,
    SUM(rows_affected) as total_rows,
    SUM(files_created) as total_files,
    COUNT(DISTINCT tenant_id) as tenants_exported,
    AVG(duration_seconds) as avg_duration_seconds
FROM bi_refresh_log
WHERE export_destination = 'AZURE_DATALAKE'
  AND refresh_timestamp >= NOW() - INTERVAL '30 days'
GROUP BY DATE(refresh_timestamp)
ORDER BY export_date DESC;
```

**5. Tenants sem exportação recente (alerta):**

```sql
SELECT 
    c.id,
    c.name,
    COALESCE(ew.last_export_success_at, '1970-01-01'::timestamptz) as last_export,
    EXTRACT(EPOCH FROM (NOW() - COALESCE(ew.last_export_success_at, '1970-01-01'::timestamptz)))/3600 as hours_since_last_export
FROM tenants c
LEFT JOIN export_watermark ew ON c.id = ew.tenant_id
WHERE COALESCE(ew.last_export_success_at, '1970-01-01'::timestamptz) < NOW() - INTERVAL '2 hours'
ORDER BY hours_since_last_export DESC;
```

### Dashboard Azure Monitor

**Criar workbook com métricas:**

1. **Transactions**: Total de write/read operations
2. **Ingress**: Volume de dados enviados para Azure
3. **Egress**: Volume de dados lidos pelos clientes
4. **Availability**: % de uptime do Storage Account
5. **Tier Distribution**: % de dados em Hot/Cool/Archive

**Query KQL para logs de acesso:**

```kql
StorageBlobLogs
| where AccountName == "stticketsdatalake"
| where TimeGenerated >= ago(24h)
| summarize 
    TotalRequests = count(),
    SuccessRate = countif(StatusCode < 400) * 100.0 / count(),
    AvgLatency = avg(DurationMs)
  by bin(TimeGenerated, 1h), OperationName, CallerIpAddress
| order by TimeGenerated desc
```

---

## �📊 Monitoramento e Auditoria

### Query de Logs de Acesso (Azure Monitor)

```kql
// Storage Account Logs
StorageBlobLogs
| where AccountName == "stticketsdatalake"
| where TimeGenerated >= ago(7d)
| where OperationName in ("GetBlob", "ListBlobs")
| extend TenantFolder = extract(@"tenant_([a-f0-9\-]+)", 1, Uri)
| summarize AccessCount = count(), LastAccess = max(TimeGenerated) by 
    CallerIpAddress, 
    AuthenticationType,
    TenantFolder,
    bin(TimeGenerated, 1h)
| order by TimeGenerated desc
```

### Alertas Recomendados

1. **Acesso Negado (403)**: Alerta se algum Service Principal tentar acessar pasta de outra tenant
2. **Volume Anômalo**: Alerta se downloads ultrapassarem 10x a média (possível data leak)
3. **Falha de Exportação**: Alerta se DAG do Airflow falhar 3x consecutivas
4. **Crescimento de Storage**: Alerta se storage crescer > 50% em 24h

---

## 🆘 Troubleshooting

### Erro: "invalid when specifying both --acl and --permissions"

**Causa:** Você está tentando usar `--permissions` e `--acl` no mesmo comando. Essas opções são mutuamente exclusivas.

**Por quê?**

- `--permissions`: Define string POSIX simples owner/group/other (ex: "rwxr-x---")
- `--acl`: Define entradas ACL nomeadas completas (ex: "user:<OID>:rwx")

Usar ambas ao mesmo tempo seria ambíguo e a CLI rejeita.

**Solução:**

```bash
# ❌ ERRADO - combina as duas opções
az storage fs access set \
  --path $TENANT_FOLDER \
  --permissions "r-x" \
  --acl "user:$OBJECT_ID:r-x" \
  --auth-mode login

# ✅ CORRETO - Opção 1: Apenas --permissions (POSIX simples)
az storage fs access set \
  --path $TENANT_FOLDER \
  --permissions "rwxr-x---" \
  --auth-mode login

# ✅ CORRETO - Opção 2: Apenas --acl (recomendado para multi-tenant)
# Ler ACL atual e adicionar entrada
CURRENT_ACL=$(az storage fs access show \
  --path $TENANT_FOLDER \
  --auth-mode login \
  --query "acl" -o tsv)

az storage fs access set \
  --path $TENANT_FOLDER \
  --acl "${CURRENT_ACL},user:${OBJECT_ID}:r-x" \
  --auth-mode login

# ✅ CORRETO - Opção 3: Usar update-recursive (não sobrescreve)
az storage fs access update-recursive \
  --path $TENANT_FOLDER \
  --acl "user:${OBJECT_ID}:r-x" \
  --auth-mode login
```

**Recomendação:** Use o script `scripts/setup_azure_acls.sh` que já implementa o fluxo correto.

### Erro: "Failed to get properties of path ... 403"

1*Causa:** Você não tem permissão suficiente no Storage Account ou na pasta.

**Verificações:**

1. **Você está autenticado?**

```bash
az account show
# Se não: az login
```

1. **Tem permissões adequadas?**

```bash
1 Verificar roles atribuídas

az role assignment list \
  --assignee $(az ad signed-in-user show --query objectId -o tsv) \
  --scope /subscriptions/<SUB_ID>/resourceGroups/<RG>/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT \
  --output table

# Você precisa de uma dessas roles:
# - Storage Blob Data Owner
# - Storage Blob Data Contributor (para escrever)
# - Owner / Contributor (no recurso)
1``


1. **HNS (Hierarchical Namespace) está habilitado?**

```bash
az storage account show \
  --name $STORAGE_ACCOUNT \
  --query "isHnsEnabled" -o tsv

# Resultado esperado: true

# Se for false, ACLs POSIX não funcionam (ADLS Gen2 requer HNS)
```

1. **Está usando --auth-mode login?**

```bash
# ✅ Correto (usa token Azure AD)
az storage fs access set --auth-mode login ...

# ❌ Errado para ACLs (usa chave compartilhada, sem identidade)
az storage fs access set --auth-mode key ...
```

**Solução:** Atribua a role adequada:

```bash
# Exemplo: dar Storage Blob Data Owner para seu usuário
az role assignment create \
  --role "Storage Blob Data Owner" \
  --assignee $(az ad signed-in-user show --query objectId -o tsv) \
  --scope /subscriptions/<SUB_ID>/resourceGroups/<RG>/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT
```

### Erro: "appId vs objectId confusion"

**Problema:** Você está usando `appId` (Client ID) em vez de `objectId` nas ACLs.

**Diferença:**

- **appId** (Application ID / Client ID): Identificador público do App Registration. Usado para autenticação (login).

- **objectId**: ID único do Service Principal no Azure AD. Usado em ACLs (autorização).

**Como obter o objectId:**

```bash
# Se você tem o appId (Client ID)
APP_ID="xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"  # CLIENT_ID do az ad sp create-for-rbac
OBJECT_ID=$(az ad sp show --id $APP_ID --query id -o tsv)
echo "Object ID: $OBJECT_ID"


# Alternativa: buscar por nome
SP_NAME="sp-client-empresaX-read"
OBJECT_ID=$(az ad sp list --display-name $SP_NAME --query "[0].id" -o tsv)
```

**Validar:**

```bash
# Mostrar informações completas do SP
az ad sp show --id $APP_ID --query "{appId:appId, objectId:id, displayName:displayName}" -o table
```

---

## 🛠️ Troubleshooting

### Erro: "This request is not authorized"

**Causa:** Service Principal não tem permissões na pasta ou credenciais incorretas.

**Solução:**

```bash
# Verificar permissões ACL
az storage fs access show \
  --account-name stticketsdatalake \
  --file-system tickets-data \
  --path tenant_<uuid> \

  --auth-mode login

# Atribuir permissões corretas
az storage fs access set \
  --account-name stticketsdatalake \
  --file-system tickets-data \
  --path tenant_<uuid> \
  --permissions "r-x" \
  --acl "user:<object-id>:r-x" \
  --auth-mode login \
  --recursive
```

### Erro: "ModuleNotFoundError: No module named 'azure'"

**Causa:** Dependências Azure não instaladas no Airflow.

**Solução:**

```bash
# Rebuild Docker image
docker-compose down
docker-compose build --no-cache

docker-compose up -d
```

### Performance: Parquet lento no Power BI

**Causa:** Muitos arquivos pequenos em vez de arquivos consolidados.

**Solução:** O DAG `azure_datalake_compaction` já está configurado para consolidar arquivos diariamente:

```bash
# Verificar se DAG de compactação está ativo
docker compose exec airflow-webserver airflow dags list | grep azure_datalake_compaction

# Executar manualmente (teste)
docker compose exec airflow-webserver airflow dags trigger azure_datalake_compaction

# Ver logs de compactação
SELECT * FROM vw_compaction_history ORDER BY refresh_timestamp DESC LIMIT 10;
```

### Custo Alto de Storage

**Causa:** Dados antigos em Hot Tier, lifecycle policy não aplicada.

**Solução:** Aplicar lifecycle policy (já criada em `scripts/lifecycle-policy.json`):

```bash
# Aplicar política
./scripts/apply_lifecycle_policy.sh

# Verificar após 24-48h (políticas são avaliadas diariamente pelo Azure)
az storage account management-policy show \
  --account-name stticketsdatalake \
  --resource-group rg-airflow-datalake

# Monitorar tier distribution
az monitor metrics list \
  --resource /subscriptions/<subscription-id>/resourceGroups/rg-airflow-datalake/providers/Microsoft.Storage/storageAccounts/stticketsdatalake \
  --metric "BlobCapacity" \
  --aggregation Average
```

### Muitos arquivos duplicados no Azure

**Causa:** Exportação incremental não configurada (ainda usando janela de 24h).

**Solução:**

```bash
# 1. Criar tabela de controle
docker compose exec -i airflow-postgres psql -U airflow -d airflow_db < sql/04_create_export_watermark.sql

# 2. Reiniciar Airflow para aplicar mudanças no DAG
docker-compose restart airflow-webserver airflow-scheduler

# 3. Verificar watermark após próxima execução
docker compose exec -i airflow-postgres psql -U airflow -d airflow_db -c \
  "SELECT * FROM vw_export_watermark_status;"

# 4. Limpar arquivos antigos duplicados (opcional - CUIDADO!)
# Executar DAG de compactação para consolidar
docker compose exec airflow-webserver airflow dags trigger azure_datalake_compaction
```

---

## 🔗 Manutenção

### Tarefas Mensais

- **Revisar custos**: Azure Portal → Cost Management
- **Verificar falhas**: `SELECT * FROM vw_export_failures WHERE refresh_timestamp >= NOW() - INTERVAL '30 days'`
- **Monitorar crescimento**: `SELECT * FROM vw_export_stats_by_tenant`
- **Validar lifecycle**: Azure Portal → Storage Account → Lifecycle Management

### Tarefas Trimestrais

- **Renovar secrets**: Service Principals expiram em 2 anos (agendar renovação)
- **Auditar acessos**: Revisar logs do Azure Monitor
- **Otimizar particionamento**: Avaliar se partições por mês/ano seriam mais eficientes
- **Backup crítico**: Verificar replicação/snapshot dos últimos 90 dias

### Automação Recomendada

**1. Alertas Azure Monitor:**

```bash
# Alerta: Falha em exportação
az monitor metrics alert create \
  --name "ExportacaoAzureFalhou" \
  --resource-group rg-airflow-datalake \
  --scopes /subscriptions/<id>/resourceGroups/rg-airflow-datalake/providers/Microsoft.Storage/storageAccounts/stticketsdatalake \
  --condition "count ServerErrors > 10" \
  --window-size 15m \
  --evaluation-frequency 5m \
  --action email <seu-email@tenant.com>
```

**2. Notificações Airflow:**

Configurar no `docker-compose.yaml`:

```yaml
AIRFLOW__SMTP__SMTP_HOST: smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT: 587
AIRFLOW__SMTP__SMTP_USER: seu-email@tenant.com
AIRFLOW__SMTP__SMTP_PASSWORD: sua-senha-app
AIRFLOW__SMTP__SMTP_MAIL_FROM: airflow@tenant.com
```

**3. Dashboard Grafana (opcional):**

Conectar ao PostgreSQL e criar painéis com:

- Total de exportações por dia
- Taxa de falhas
- Watermark lag por tenant
- Tamanho total por tenant no Azure

---

## 📚 Referências

- [Azure Data Lake Storage Gen2 - Documentação Oficial](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Access Control Lists (ACLs)](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control)
- [Azure Lifecycle Management](https://learn.microsoft.com/en-us/azure/storage/blobs/lifecycle-management-overview)
- [Apache Parquet Format](https://parquet.apache.org/docs/)
