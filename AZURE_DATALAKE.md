# Azure Data Lake Storage Gen2 - Integração Multi-Tenant

## 📋 Visão Geral

Esta documentação descreve como exportar dados do PostgreSQL para Azure Data Lake Storage Gen2 (ADLS Gen2) com **isolamento físico por tenant** usando `tenant_id`. Cada cliente terá acesso apenas aos seus próprios dados através de **Service Principals dedicados** com permissões ACL no nível de diretório.

**Arquitetura:**

```
PostgreSQL (Materialized Views)
    ↓ Airflow DAG (a cada 15 minutos)
Azure Data Lake Storage Gen2
    ├── tenant_<uuid_1>/
    │   ├── tickets/year=2026/month=01/day=14/*.parquet
    ├── tenant_<uuid_2>/
    │   └── ...
    └── tenant_<uuid_N>/
        └── ...
```

**Benefícios:**

- ✅ **Isolamento físico**: Cada tenant tem sua própria pasta isolada
- ✅ **Segurança**: ACLs do Azure garantem que tenant X não veja dados da tenant Y
- ✅ **Self-service**: Clientes consomem dados com suas próprias ferramentas (Power BI Service, Python, Excel, Tableau, Synapse)
- ✅ **Custo**: ~USD 25-30/mês para 1TB (muito mais barato que Power BI Premium)
- ✅ **Performance**: Formato Parquet com compressão Snappy otimizado para analytics
- ✅ **Particionamento**: Partições por data facilitam queries incrementais

---

## 🚀 Configuração Passo a Passo

### 1. Criar Storage Account no Azure

```bash
# Variáveis
RESOURCE_GROUP="rg-airflow-datalake"
LOCATION="eastus"
STORAGE_ACCOUNT="ticketsdatalake"  # deve ser único globalmente
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
  --account-name "ticketsdatalake" \
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
# Exemplo para Tenant teste (tenant_id = 8c1dca21-ba17-5018-b56d-cf6395d413e5)
SP_NAME="sp-client-teste-read"
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

# Adicionar permissão SOMENTE para este Service Principal na pasta do tenant
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
# Modo interativo (um tenant por vez)
./scripts/setup_azure_acls.sh

# Modo direto (argumentos)
./scripts/setup_azure_acls.sh <tenant_id> <object_id> "Nome do Tenant"

# Modo batch (múltiplos tenants de uma vez via CSV)

./scripts/setup_azure_acls.sh --batch scripts/tenants_acls.csv
```

**Formato do CSV** (`tenants_acls.csv`):

```csv
tenant_id,object_id,tenant_name
8c1dca21-ba17-5018-b56d-cf6395d413e5,xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx,Tenant A
a1b2c3d4-e5f6-7890-abcd-ef1234567890,yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy,Tenant B
```

**Script de geração completa:** Veja `scripts/generate_azure_service_principals.py` para gerar Service Principals E configurar ACLs para todas os tenants automaticamente.

### 4. Configurar Variáveis de Ambiente

Adicione ao arquivo `.env`:

```bash
# Azure Data Lake Storage Gen2
AZURE_STORAGE_ACCOUNT_NAME=ticketsdatalake
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
2. URL: `https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/tickets/`
3. Autenticação: **Service Principal**
   - Tenant ID: `xxx`
   - Service Principal ID: `yyy`
   - Service Principal Key: `zzz`
4. Use botão **"Combine"** ou script Power Query para combinar Parquet
5. Crie relatório e dashboards
6. **Opcional:** Baixe .pbix e abra no Desktop

### 2. Power BI Desktop (Uso Secundário)

**Arquivo de conexão para o cliente (exemplo: `teste_DataLake_Connection.pbids`):**

```json
{
  "version": "0.1",
  "connections": [
    {
      "details": {
        "protocol": "abfss",
        "address": {
          "url": "abfss://tickets-data@ticketsdatalake.dfs.core.windows.net/tenant_8c1dca21-ba17-5018-b56d-cf6395d413e5/"
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
2. URL: `https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/`
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
AZURE_TENANT_ID = "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx"
AZURE_CLIENT_ID = "yyyyyyyy-yyyy-yyyy-yyyy-yyyyyyyyyyyy"
AZURE_CLIENT_SECRET = "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
STORAGE_ACCOUNT = "ticketsdatalake"
CONTAINER = "tickets-data"
TENANT_ID = "8c1dca21-ba17-5018-b56d-cf6395d413e5"

# Autenticação
credential = ClientSecretCredential(AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET)
account_url = f"https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
service_client = DataLakeServiceClient(account_url=account_url, credential=credential)

# Ler arquivo Parquet
file_system_client = service_client.get_file_system_client(CONTAINER)
file_path = f"tenant_{TENANT_ID}/tickets/year=2026/month=01/day=14/tickets_20260114_103000.parquet"
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
    LOCATION = 'abfss://tickets-data@ticketsdatalake.dfs.core.windows.net/tenant_8c1dca21-ba17-5018-b56d-cf6395d413e5/'
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
    LOCATION = 'tickets/year=2026/month=01/day=*/*.parquet',
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
    Source = AzureStorage.DataLake("https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_8c1dca21-ba17-5018-b56d-cf6395d413e5/"),
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
# ACL da tenant teste (exemplo)
az storage fs access show \
  --account-name ticketsdatalake \
  --file-system tickets-data \
  --path tenant_8c1dca21-ba17-5018-b56d-cf6395d413e5 \
  --auth-mode login

# Resultado:

# {
#   "acl": "user::rwx,user:sp-teste-object-id:r-x,group::r-x,other::---",
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
  --account-name ticketsdatalake \
  --file-system tickets-data \
  --path tenant_<outro_uuid>/tickets/year=2026/month=01/day=14/data.parquet \
  --auth-mode key

# Erro esperado: 403 Forbidden - This request is not authorized to perform this operation
```

---

## 📊 Monitoramento e Auditoria

### Query de Logs de Acesso (Azure Monitor)

```kql
// Storage Account Logs
StorageBlobLogs
| where AccountName == "ticketsdatalake"
| where TimeGenerated >= ago(7d)
| where OperationName in ("GetBlob", "ListBlobs")
| extend tenantFolder = extract(@"tenant_([a-f0-9\-]+)", 1, Uri)
| summarize AccessCount = count(), LastAccess = max(TimeGenerated) by 
    CallerIpAddress, 
    AuthenticationType,
    tenantFolder,
    bin(TimeGenerated, 1h)
| order by TimeGenerated desc
```

### Alertas Recomendados

1. **Acesso Negado (403)**: Alerta se algum Service Principal tentar acessar pasta de outra tenant
2. **Volume Anômalo**: Alerta se downloads ultrapassarem 10x a média (possível data leak)
3. **Falha de Exportação**: Alerta se DAG do Airflow falhar 3x consecutivas
4. **Crescimento de Storage**: Alerta se storage crescer > 50% em 24h

---

## 💰 Custos Estimados

**Exemplo para 1TB de dados:**

| Item | Cálculo | Custo/Mês (USD) |
|------|---------|----------------|
| Storage (Hot Tier) | 1TB × $0.020/GB | $20.48 |
| Write Operations | 1M writes × $0.065/10k | $6.50 |
| Read Operations | 10M reads × $0.004/10k | $4.00 |
| **Total** | | **~$30.98** |

**Comparação:**

- Power BI Premium: USD 4.995/mês (capacidade compartilhada)
- Power BI Pro: USD 10/usuário/mês × 50 usuários = USD 500/mês
- **Azure Data Lake: USD 30/mês** ✅

---

## 📝 Checklist de Implementação

- [ ] 1. Criar Storage Account com hierarchical namespace
- [ ] 2. Criar container `tickets-data`
- [ ] 3. Criar Service Principal para Airflow (write)
- [ ] 4. Criar Service Principals por tenant (read)
- [ ] 5. Configurar ACLs por pasta de tenant
- [ ] 6. Adicionar variáveis Azure ao `.env`
- [ ] 7. Atualizar `docker-compose.yaml` com dependências
- [ ] 8. Build da nova imagem Docker do Airflow
- [ ] 9. Deploy do DAG `export_to_azure_datalake.py`
- [ ] 10. Testar exportação para uma tenant
- [ ] 11. Validar isolamento (tenant A não vê dados de B)
- [ ] 12. Fornecer credenciais aos clientes
- [ ] 13. Criar documentação de consumo por ferramenta
- [ ] 14. Configurar alertas no Azure Monitor
- [ ] 15. Documentar processo de onboarding de novos clientes

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
SP_NAME="sp-client-teste-read"
OBJECT_ID=$(az ad sp list --display-name $SP_NAME --query "[0].id" -o tsv)
```

**Validar:**

```bash
# Mostrar informações completas do SP
az ad sp show --id $APP_ID --query "{appId:appId, objectId:id, displayName:displayName}" -o table
```

### Erro: "This request is not authorized"

**Causa:** Service Principal não tem permissões na pasta ou credenciais incorretas.

**Solução:**

```bash
# Verificar permissões ACL
az storage fs access show \
  --account-name ticketsdatalake \
  --file-system tickets-data \
  --path tenant_<uuid> \

  --auth-mode login

# Atribuir permissões corretas
az storage fs access set \
  --account-name ticketsdatalake \
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

**Solução:** Implementar compactação periódica:

```python
# DAG de compactação semanal
# Combinar múltiplos arquivos Parquet de uma partição em um único arquivo
# Executar aos domingos às 2am
```

### Custo Alto de Storage

**Causa:** Dados antigos em Hot Tier.

**Solução:** Implementar lifecycle policy:

```bash
az storage account management-policy create \
  --account-name ticketsdatalake \
  --policy @lifecycle-policy.json

# lifecycle-policy.json:
{
  "rules": [
    {
      "enabled": true,
      "name": "MoveToArchive",
      "type": "Lifecycle",
      "definition": {
        "actions": {
          "baseBlob": {
            "tierToArchive": {
              "daysAfterModificationGreaterThan": 90
            }
          }
        },
        "filters": {
          "blobTypes": ["blockBlob"],
          "prefixMatch": ["tickets-data/"]
        }
      }
    }
  ]
}
```

---

## 🔗 Próximos Passos

1. **Script de Automação**: `scripts/generate_azure_service_principals.py` para criar Service Principals em batch
2. **Documentação para Clientes**: Guias específicos por ferramenta (Power BI, Tableau, Python)
3. **Dashboard de Monitoramento**: Criar workbook no Azure Monitor com métricas de uso
4. **Backup**: Configurar Azure Backup para disaster recovery
5. **Otimização**: Implementar compactação de arquivos Parquet para melhor performance

---

## 📚 Referências

- [Azure Data Lake Storage Gen2 - Documentação Oficial](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)
- [Access Control Lists (ACLs)](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-access-control)
- [Apache Parquet Format](https://parquet.apache.org/docs/)
