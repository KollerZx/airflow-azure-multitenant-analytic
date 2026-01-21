# 🚀 Guia Rápido - Azure Data Lake Multi-Tenant

Guia prático para configurar exportação de dados multi-tenant para Azure Data Lake Storage Gen2.

---

## ✅ Pré-requisitos

- ✅ PostgreSQL rodando com views materializadas criadas
- ✅ Docker e Docker Compose instalados
- ✅ Python 3.11+ instalado
- ✅ Azure CLI instalado (`az`)
- ✅ Conta Azure ativa

### ⚠️ IMPORTANTE: PostgreSQL no Host com Firewall UFW

Se o PostgreSQL está rodando na **máquina host** (não em Docker) e você tem **UFW habilitado**, será necessário configurar o firewall para permitir conexões da rede bridge Docker:

```bash
# Identificar interface bridge
docker network inspect airflow-network | grep "com.docker.network.bridge.name"

# Liberar porta 5432 para a interface (substituir pelo nome da sua bridge)
sudo ufw allow in on br-a1b2c3d4e5f6 to any port 5432 proto tcp
sudo ufw reload
```

**Também é necessário** adicionar o IP da rede Docker ao `pg_hba.conf`. Veja instruções detalhadas em [Troubleshooting - Firewall UFW](README.md#-problema-firewall-ufw-bloqueando-docker-bridge).

---

## 📋 Configuração em 5 Passos

### Passo 1: Setup Azure (1-2 horas)

```bash
# 1. Login no Azure
az login

# 2. Criar Resource Group e Storage Account
az group create --name rg-airflow-datalake --location eastus

az storage account create \
  --name ticketsdatalake \
  --resource-group rg-airflow-datalake \
  --location eastus \
  --sku Standard_LRS \
  --kind StorageV2 \
  --hierarchical-namespace true

# 3. Criar Container
az storage container create \
  --name tickets-data \
  --account-name ticketsdatalake \
  --auth-mode login

# 4. Criar Service Principal para Airflow
# ⚠️ IMPORTANTE: Azure Data Lake Gen2 requer DUAS camadas de permissão:
#    1. RBAC (nível do Storage Account) - obrigatório para autenticação
#    2. ACL POSIX (nível de arquivo/diretório) - obrigatório para isolamento
# O comando abaixo configura automaticamente o RBAC via --role + --scopes
az ad sp create-for-rbac \
  --name "sp-airflow-datalake-export" \
  --role "Storage Blob Data Contributor" \
  --scopes /subscriptions/<SUBSCRIPTION_ID>/resourceGroups/rg-airflow-datalake/providers/Microsoft.Storage/storageAccounts/ticketsdatalake \
  --years 2

# ⚠️ GUARDAR OUTPUT: appId (CLIENT_ID), password (CLIENT_SECRET), tenant (TENANT_ID)
# ⚠️ ATENÇÃO: Secret expira em 2 anos - agendar renovação!
```

---

### Passo 2: Configurar Ambiente Local (30 min)

```bash
# 1. Copiar template
cp .env.example .env

# 2. Editar com credenciais PostgreSQL + Azure
nano .env
```

Configurar no `.env`:

```bash
# PostgreSQL
POSTGRES_HOST=<your_postgres_host>
POSTGRES_PORT=5432
POSTGRES_DB=<your_database>
POSTGRES_USER=<your_postgres_user>
POSTGRES_PASSWORD=<your_postgres_password>
POSTGRES_SCHEMA=<your_schema>

# Azure Data Lake (do Passo 1)
AZURE_STORAGE_ACCOUNT_NAME=ticketsdatalake
AZURE_CONTAINER_NAME=tickets-data
AZURE_TENANT_ID=<tenant_id_do_passo_1>
AZURE_CLIENT_ID=<app_id_do_passo_1>
AZURE_CLIENT_SECRET=<password_do_passo_1>
```

```bash
# 3. Rebuild Docker com Azure SDK
docker compose down
docker compose build --no-cache
docker compose up -d

# 4. Aguardar inicialização (2-3 min)
docker logs -f airflow-webserver
```

---

### Passo 3: Ativar DAGs no Airflow (5 min)

#### Opção A: Via Interface Web (Recomendado para Primeira Vez)

```bash
# 1. Acessar Airflow UI
# http://localhost:8080
# Usuário: airflow / Senha: airflow
```

A conexão `postgres_prod` é criada **automaticamente** na inicialização do Airflow.

**Verificar se as DAGs estão ativas:**

- DAGs → `tickets_powerbi_etl` → Toggle ON (se necessário)
- DAGs → `export_tickets_to_azure_datalake` → Toggle ON (se necessário)

**Executar manualmente para testar:**

- Clicar em ▶️ em cada DAG

#### Opção B: Via Airflow CLI (Recomendado para Automação)

**Verificar conexões:**

```bash
# Listar todas as conexões
docker compose exec airflow-webserver airflow connections list

# Ver detalhes da conexão postgres_prod
docker compose exec airflow-webserver airflow connections get postgres_prod
```

**Gerenciar DAGs:**

```bash
# Listar todas as DAGs e seus status (coluna 'paused' indica se está ativa)
docker compose exec airflow-webserver airflow dags list --output table

# Interpretação da coluna 'paused':
# - paused = False → DAG ATIVA (toggle ON) - executará no schedule
# - paused = True  → DAG PAUSADA (toggle OFF) - não executará

# Despausar DAGs (ativar para execução periódica)
docker compose exec airflow-webserver airflow dags unpause tickets_powerbi_etl
docker compose exec airflow-webserver airflow dags unpause export_tickets_to_azure_datalake

# Pausar DAGs (desativar execução periódica)
docker compose exec airflow-webserver airflow dags pause tickets_powerbi_etl

# Executar DAG manualmente (trigger)
docker compose exec airflow-webserver airflow dags trigger tickets_powerbi_etl

# Ver últimas execuções de uma DAG (todas)
docker compose exec airflow-webserver airflow dags list-runs -d tickets_powerbi_etl

# Ver apenas execuções com sucesso
docker compose exec airflow-webserver airflow dags list-runs -d tickets_powerbi_etl --state success

# Ver apenas execuções falhadas
docker compose exec airflow-webserver airflow dags list-runs -d tickets_powerbi_etl --state failed

# Ver apenas execuções em andamento
docker compose exec airflow-webserver airflow dags list-runs -d tickets_powerbi_etl --state running
```

**Verificar status de execução:**

```bash

# Testar uma task isoladamente (não afeta histórico)
docker compose exec airflow-webserver airflow tasks test tickets_powerbi_etl check_views_exist 2026-01-20
```

**Comandos úteis para troubleshooting:**

```bash
# Pausar uma DAG temporariamente
docker compose exec airflow-webserver airflow dags pause tickets_powerbi_etl

# Cancelar uma execução específica por data
docker compose exec airflow-webserver airflow tasks clear tickets_powerbi_etl \
  --start-date 2026-01-20 \
  --end-date 2026-01-20 \
  --yes

# Marcar uma run como falhada (para em qualquer tarefa em execução)
docker compose exec airflow-webserver airflow dags state tickets_powerbi_etl 2026-01-20T18:00:00+00:00 failed

# Limpar histórico de execuções falhadas
docker compose exec airflow-webserver airflow dags delete tickets_powerbi_etl

# Ver variáveis de ambiente do Airflow
docker compose exec airflow-webserver airflow config list
```

---

### Passo 4: Gerar Service Principals por Tenant (1 hora)

#### 4.1. Opções de Geração

**Opção A: Todos os tenants**

```bash
# Instalar dependências (se ainda não fez)
pip install psycopg2-binary python-dotenv

# Gerar para TODOS os tenants
python scripts/generate_azure_service_principals.py
```

**Opção B: Tenants específicos por nome**

```bash
# Busca parcial, case-insensitive
python scripts/generate_azure_service_principals.py --tenants "Tenant A" "Tenant B"

# Também aceita nome parcial
python scripts/generate_azure_service_principals.py -t "Tenant A"
```

**Opção C: Tenants específicos por ID**

```bash
python scripts/generate_azure_service_principals.py --ids \
  "8c1dca21-ba17-5018-b56d-cf6395d413e5" \
  "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

**Opção D: Modo interativo (recomendado)**

```bash
# Mostra lista e permite selecionar
python scripts/generate_azure_service_principals.py --interactive

# Você verá:
# 1. Tenant A Trucks        |   1,234 tickets
# 2. Tenant B Parts        |     567 tickets
# 3. Tenant C    |     890 tickets
#
# Selecione os tenants: 1 3
# (ou use intervalos: 1-3, ou "all" para todas)
```

#### 4.2. Executar Script Gerado

```bash
# Outputs do passo anterior:
# ✅ azure/create_service_principals_YYYYMMDD_HHMMSS.sh
# ✅ azure_client_docs_YYYYMMDD_HHMMSS/ (documentação por tenant)

# Executar script para criar Service Principals no Azure
chmod +x azure/create_service_principals_*.sh
bash azure/create_service_principals_*.sh

# Script cria automaticamente:
# - Service Principal por tenant (sp-client-<tenant>-read)
# - ACLs de leitura na pasta tenant_<uuid>/
# - Arquivo de credenciais: azure_client_docs_*/empresa_access_guide.md
```

#### 4.3. Distribuir Credenciais aos Clientes

```bash
# Cada tenant recebe:
ls azure_client_docs_*/<tenant>_access_guide.md

# O arquivo contém:
# - CLIENT_ID, TENANT_ID para autenticação
# - URLs específicas por tipo de dado (tickets/, etc.)
# - Instruções Power BI Desktop e Power BI Service
# - Exemplos Python para acesso programático
# - FAQ com troubleshooting
```

**⚠️ IMPORTANTE:** Enviar credenciais por canal seguro (NÃO por email):

- Azure Tenant ID
- Azure Client ID  
- Azure Client Secret
- Storage Account Name: `ticketsdatalake`
- Container Name: `tickets-data`

---

### Passo 5: Validar Exportação e Isolamento

#### 5.1. Verificar Exportação no Azure

```bash
# Via Azure Portal
# https://portal.azure.com → Storage Account → ticketsdatalake → Containers → tickets-data

# Estrutura esperada:
# tenant_<uuid>/
#   ├── tickets/year=2026/month=01/day=19/*.parquet


# Via Azure CLI
az storage fs file list \
  --account-name ticketsdatalake \
  --file-system tickets-data \
  --path "tenant_<uuid>/" \
  --auth-mode login
```

#### 5.2. Testar Isolamento de Service Principal

```python
# test_sp_isolation.py
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient

credential = ClientSecretCredential(
    tenant_id="<TENANT_ID>",
    client_id="<CLIENT_ID_TENANT_A>",
    client_secret="<CLIENT_SECRET_TENANT_A>"
)

service_client = DataLakeServiceClient(
    account_url="https://ticketsdatalake.dfs.core.windows.net",
    credential=credential
)

fs_client = service_client.get_file_system_client("tickets-data")

# ✅ Deve funcionar: acesso à própria pasta
try:
    paths_a = fs_client.get_paths(path="tenant_<uuid_a>")
    print("✅ Acesso à própria pasta OK")
except Exception as e:
    print(f"❌ Erro: {e}")

# ❌ Deve falhar: acesso à pasta de outra tenant
try:
    paths_b = fs_client.get_paths(path="tenant_<uuid_b>")
    print("❌ FALHA DE SEGURANÇA: Acesso não autorizado!")
except Exception as e:
    print(f"✅ Isolamento OK: {e}")
```

---

## 📊 Conectar Power BI ao Azure Data Lake

> 🔒 **IMPORTANTE - Autenticação com ACLs:** Este projeto usa **ACLs POSIX sem RBAC** para isolamento multi-tenant. Power BI Desktop requer RBAC, então **recomendamos Power BI Service** para conexões iniciais.

### Conexão Power BI Service (Recomendado)

```
Get Data → Azure → Azure Data Lake Storage Gen2

Account name or URL:
  https://ticketsdatalake.dfs.core.windows.net

Authentication:
  - Service Principal
  - Tenant ID: <tenant_id_do_cliente>
  - Application (Client) ID: <client_id_do_cliente>
  - Client Secret: <secret_do_cliente>

Navigator:
  tickets-data/
    └── tenant_<uuid>/
        ├── tickets/
        
```

**⚠️ IMPORTANTE:** Consulte o guia específico da tenant (`azure_client_docs_*/empresa_access_guide.md`) para:

- URLs exatas por tipo de dado
- Scripts Power Query para combinar Parquet
- Exemplos de código Python para acesso programático

### Conexão Power BI Desktop (Uso Secundário)

> ⚠️ **ATENÇÃO:** Criar conexão nova no Desktop falhará com "Access forbidden". **Sempre crie primeiro no Power BI Service**, depois baixe o .pbix.

**Se já possui .pbix do Service:**

1. Abra o arquivo .pbix baixado do Power BI Service
2. As conexões já estarão configuradas
3. Edite visualizações e republique

**Para criar nova conexão (NÃO recomendado - requer RBAC):**

```
Get Data → Azure → Azure Data Lake Storage Gen2

Account name or URL (conectar diretamente à pasta):
  https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/tickets

Authentication:
  - Service Principal
  - Tenant ID: <tenant_id_do_cliente>
  - Service Principal ID: <client_id_do_cliente>
  - Service Principal Key: <client_secret_do_cliente>
```

**Criar conexões separadas** (uma para cada tipo de dado):

1. **Tickets:**
   - URL: `https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/tickets`
   - Nome da tabela: `Tickets`

**⚠️ IMPORTANTE:**

- Cada URL aponta diretamente para a pasta do tipo de dado
- Use **Service Principal** (não conta organizacional)
- Mesmas credenciais em qualquer ferramenta (Service, Desktop após exportar, Python)

### Transformações Power Query

```powerquery
// Combinar arquivos Parquet particionados por data
let
    Source = AzureStorage.DataLake(
        "https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/tickets/"
    ),
    
    // Filtrar apenas arquivos .parquet
    FilteredFiles = Table.SelectRows(Source, each Text.EndsWith([Name], ".parquet")),
    
    // Combinar todos os Parquet
    CombinedBinary = Table.Combine(
        List.Transform(
            FilteredFiles[Content],
            each Parquet.Document(_)
        )
    )
in
    CombinedBinary
```

---

## 🔍 Monitoramento

### Ver Execuções do Airflow

```
http://localhost:8080 → DAGs

✅ tickets_powerbi_etl → Atualiza views materializadas no PostgreSQL
✅ export_tickets_to_azure_datalake → Exporta para Azure Data Lake
```

### Ver Logs de Exportação Azure

```bash
# Logs do Airflow
docker logs -f airflow-scheduler | grep "export_tickets_to_azure_datalake"

# Ver últimas exportações no PostgreSQL
psql -U <your_postgres_user> -d <your_database> -c "
SELECT 
    tenant_id,
    tenant_name,
    COUNT(*) as tickets_exported,
    MAX(updated_at) as last_export
FROM <your_schema>.bi_tickets_flat
GROUP BY tenant_id, tenant_name
ORDER BY last_export DESC;
"
```

### Ver Status do DAG

```bash
# Via CLI
docker compose exec airflow-scheduler airflow dags list-runs \
  -d export_tickets_to_azure_datalake \
  --state success \
  --output table

# Ver arquivos exportados no Azure
az storage fs file list \
  --account-name ticketsdatalake \
  --file-system tickets-data \
  --auth-mode login \
  --output table
```

---

## 📋 Checklist de Validação

- [ ] ✅ Docker Airflow rodando (`docker compose ps`)
- [ ] ✅ DAG `tickets_powerbi_etl` ativa e executando
- [ ] ✅ DAG `export_tickets_to_azure_datalake` ativa e executando
- [ ] ✅ Views materializadas criadas no PostgreSQL (sql/01-03)
- [ ] ✅ Azure Storage Account criado (ticketsdatalake)
- [ ] ✅ Container `tickets-data` criado
- [ ] ✅ Service Principal Airflow criado (sp-airflow-datalake-export)
- [ ] ✅ Credenciais Azure configuradas no `.env`
- [ ] ✅ Service Principals por tenant criados (via script)
- [ ] ✅ ACLs configuradas nas pastas `tenant_<uuid>/`
- [ ] ✅ Dados exportados para Azure Data Lake
- [ ] ✅ Estrutura de pastas validada (year=/month=/day=/)
- [ ] ✅ Arquivos Parquet criados (*.parquet)
- [ ] ✅ Guias de acesso gerados (`azure_client_docs_*/`)
- [ ] ✅ Credenciais distribuídas aos clientes
- [ ] ✅ Power BI Service conecta ao Data Lake
- [ ] ✅ Isolamento testado (Service Principal A não acessa pasta B)

---

## 🎯 Regras de Ouro

### Para Analistas de BI

```python
# ✅ SEMPRE usar credenciais corretas da tenant
from azure.identity import ClientSecretCredential

credential = ClientSecretCredential(
    tenant_id="<TENANT_ID_EMPRESA>",
    client_id="<CLIENT_ID_EMPRESA>",
    client_secret="<SECRET_EMPRESA>"
)

# ✅ Acessar APENAS a pasta da tenant
tenant_path = f"tenant_{tenant_uuid}/tickets/"

# ❌ NUNCA compartilhar credenciais entre tenants
# ❌ NUNCA tentar acessar pastas de outras tenants
```

### Para Administradores

1. **Criar Service Principal para nova tenant:**

   ```bash
   python scripts/generate_azure_service_principals.py -c "Nova Tenant"
   bash azure/create_service_principals_*.sh
   ```

2. **Rotacionar secrets semestralmente:**

   ```bash
   # Listar Service Principals próximos do vencimento
   az ad sp list --query "[?passwordCredentials[0].endDateTime < '2026-06-01'].{name:displayName, expiry:passwordCredentials[0].endDateTime}" -o table
   
   # Criar novo secret
   az ad sp credential reset --id <app-id> --years 2
   ```

3. **Remover acesso de tenant:**

   ```bash
   # Remover ACLs da pasta
   az storage fs access remove-recursive \
     --account-name ticketsdatalake \
     --file-system tickets-data \
     --path "tenant_<uuid>" \
     --acl "user:<sp-object-id>:---"
   
   # Deletar Service Principal
   az ad sp delete --id <app-id>
   ```

4. **Auditar acessos mensalmente:**

   ```bash
   # Ver logs de acesso no Azure Monitor
   az monitor activity-log list \
     --resource-group rg-airflow-datalake \
     --start-time 2026-01-01 \
     --query "[?contains(operationName.value, 'Microsoft.Storage')].{Time:eventTimestamp, Operation:operationName.localizedValue, Caller:caller}"
   ```

---

## 🆘 Problemas Comuns

### Airflow não conecta ao PostgreSQL

**Teste 1: Verificar conectividade básica**

```bash
# Testar se resolve o host
docker compose exec airflow-webserver ping -c 3 host.docker.internal

# Se falhar, já está configurado em docker-compose.yaml:
extra_hosts:
  - "host.docker.internal:host-gateway"
```

**Teste 2: Verificar porta PostgreSQL**

```bash
# Testar se a porta 5432 está acessível
docker compose exec airflow-webserver nc -zv host.docker.internal 5432
```

**Se ping funciona mas porta 5432 falha:**

- Provável causa: **Firewall UFW bloqueando tráfego da rede Docker**
- Solução: Veja [README - Firewall UFW](README.md#-problema-firewall-ufw-bloqueando-docker-bridge)
- Quick fix:

  ```bash
  # Liberar porta para interface bridge Docker
  BRIDGE=$(docker network inspect airflow-network | grep -oP '"com.docker.network.bridge.name": "\K[^"]+"')
  sudo ufw allow in on $BRIDGE to any port 5432 proto tcp
  sudo ufw reload
  ```

### DAG de exportação falha com erro de autenticação Azure

**Erro:** `azure.core.exceptions.ClientAuthenticationError`

**Soluções:**

1. Verificar credenciais no `.env`:

   ```bash
   cat .env | grep AZURE
   ```

2. Testar credenciais manualmente:

   ```python
   from azure.identity import ClientSecretCredential
   credential = ClientSecretCredential(
       tenant_id="<TENANT_ID>",
       client_id="<CLIENT_ID>",
       client_secret="<CLIENT_SECRET>"
   )
   token = credential.get_token("https://storage.azure.com/.default")
   print("✅ Credenciais válidas")
   ```

3. Verificar permissões RBAC:

   ```bash
   az role assignment list \
     --assignee <CLIENT_ID> \
     --scope /subscriptions/<SUB_ID>/resourceGroups/rg-airflow-datalake
   ```

### Power BI Service mostra "Esta tabela não tem nenhuma linha" ou "Alguns passos não foram concluídos"

**Erro completo:**

```
Alguns passos não foram concluídos
A criação do modelo semântico foi bem-sucedida, mas certas etapas 
não terminaram como esperado. Você pode precisar completar uma 
configuração adicional.
```

**Sintomas:**

- ✅ Estrutura das tabelas é importada
- ❌ Tabelas aparecem vazias (sem dados)
- ✅ Autenticação funcionou corretamente

**Causa:** Power BI Service não consegue navegar automaticamente nas subpastas particionadas (`year=/month=/day=`) do Data Lake Gen2 quando você conecta à pasta raiz.

**Solução 1: Conectar diretamente à pasta específica (RECOMENDADO)**

Ao invés de conectar em `.../tenant_<uuid>/`, conecte diretamente na pasta do tipo de dado:

```
❌ NÃO FUNCIONA no Power BI Service:
https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/

✅ FUNCIONA no Power BI Service:
https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/tickets/
```

**Passos no Power BI Service:**

1. Get Data → Azure → Azure Data Lake Storage Gen2
2. URL: `https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/tickets/`
3. Authentication: Service Principal
4. No Navigator, clique no botão **"Combine"** ou use o script abaixo em Power Query:

**Opção 1: Usar Botão "Combine" (Mais Simples)**

No Navigator do Power BI Service, após conectar:

- Você verá a estrutura de pastas `year=2026/month=01/day=20/`
- Clique no botão **"Combine"** no canto inferior direito
- Power BI detecta automaticamente os arquivos Parquet e combina todos

**Opção 2: Script Power Query Manual**

```powerquery
let
    Source = AzureStorage.DataLake("https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/tickets/"),
    
    // Navegar recursivamente em todas as subpastas
    #"Kept Errors" = Table.SelectRows(Source, each true),
    
    // Função para processar cada linha (pode ser pasta ou arquivo)
    ProcessFiles = Table.AddColumn(#"Kept Errors", "FileContent", each
        if [Folder Path] <> null and [Extension] = ".parquet" then
            try Parquet.Document([Content]) otherwise null
        else
            null
    ),
    
    // Remover linhas sem conteúdo (pastas)
    FilterFiles = Table.SelectRows(ProcessFiles, each [FileContent] <> null),
    
    // Combinar todos os Parquet em uma única tabela
    CombineParquet = if Table.RowCount(FilterFiles) > 0 then
        Table.Combine(FilterFiles[FileContent])
    else
        #table({"Erro"}, {{"Nenhum arquivo Parquet encontrado"}})
in
    CombineParquet
```

**Opção 3: Usar Folder.Files (Mais Robusto para Power BI Service)**

```powerquery
let
    Source = AzureStorage.DataLake("https://ticketsdatalake.dfs.core.windows.net/tickets-data/tenant_<uuid>/tickets/"),
    
    // Filtrar apenas arquivos (não pastas)
    FilterFiles = Table.SelectRows(Source, each [Extension] = ".parquet"),
    
    // Adicionar coluna com conteúdo dos Parquet
    AddParquetContent = Table.AddColumn(FilterFiles, "ParquetData", each 
        try Parquet.Document([Content]) otherwise null
    ),
    
    // Remover linhas com erro
    RemoveErrors = Table.SelectRows(AddParquetContent, each [ParquetData] <> null),
    
    // Combinar todos os arquivos
    CombinedData = Table.Combine(RemoveErrors[ParquetData]),
    
    // Remover colunas de metadados se necessário
    Result = CombinedData
in
    Result
```

**Solução 2: Usar Storage Explorer + Gateway**

Se a Solução 1 não funcionar, use Azure Storage Explorer localmente:

1. Baixar [Azure Storage Explorer](https://azure.microsoft.com/en-us/products/storage/storage-explorer/)
2. Conectar com Service Principal
3. Baixar arquivos Parquet localmente
4. Importar no Power BI Service via Gateway

**⚠️ IMPORTANTE:** Com ACLs (sem RBAC), **Power BI Desktop não consegue criar conexões novas**. Use Power BI Service primeiro, depois exporte .pbix para Desktop.

### Service Principal não consegue acessar dados

**Erro:** `This request is not authorized to perform this operation`

**Soluções:**

1. Verificar ACLs na pasta:

   ```bash
   az storage fs access show \
     --account-name ticketsdatalake \
     --file-system tickets-data \
     --path "tenant_<uuid>" \
     --auth-mode login
   ```

2. Reconfigurar ACLs:

   ```bash
   # Pegar Object ID do Service Principal
   SP_OBJECT_ID=$(az ad sp show --id <client-id> --query id -o tsv)
   
   # Configurar ACLs
   az storage fs access set-recursive \
     --account-name ticketsdatalake \
     --file-system tickets-data \
     --path "tenant_<uuid>" \
     --acl "user:${SP_OBJECT_ID}:r-x" \
     --auth-mode login
   ```

---

## � Exemplos Práticos do Passo 4

### Exemplo 1: Gerar apenas para clientes VIP

```bash
# Ver todas as tenants primeiro
python scripts/generate_azure_service_principals.py --interactive

# Selecionar as 3 primeiras: 1-3
# Ou individual: 1 5 7
```

### Exemplo 2: Gerar para novas tenants

```bash
# Buscar tenants que começam com "New"
python scripts/generate_azure_service_principals.py --tenants "New"

# Resultado: New Tenant Ltd, NewTech Solutions, etc.
```

### Exemplo 3: Gerar para ID específico

```bash
# Copiar tenant_id do banco
psql -U <your_postgres_user> -d <your_database> -c \
  "SELECT tenant_id, tenant_name FROM <your_schema>.tenants WHERE tenant_name LIKE '%Tenant A%';"

# Usar o ID retornado
python scripts/generate_azure_service_principals.py --ids \
  "8c1dca21-ba17-5018-b56d-cf6395d413e5"
```

### Exemplo 4: Adicionar tenant novo posteriormente

```bash
# 1. Nova tenant foi cadastrada no sistema
# 2. Gerar Service Principal apenas para ela
python scripts/generate_azure_service_principals.py -c "Novo Tenant Ltda"

# 3. Executar script gerado
bash azure/create_service_principals_*.sh

# 4. Distribuir credenciais ao cliente
```

---

## �📚 Documentação Complementar

- **[AZURE_DATALAKE.md](AZURE_DATALAKE.md)** - Guia completo Azure Data Lake

---

## 📞 Resumo dos Comandos

```bash
# 1. Configurar Azure
az login
az group create --name rg-airflow-datalake --location eastus
az storage account create --name ticketsdatalake --hierarchical-namespace true
az ad sp create-for-rbac --name "sp-airflow-datalake-export" --role "Storage Blob Data Contributor"

# 2. Configurar ambiente local
cp .env.example .env
nano .env  # Adicionar credenciais Azure + PostgreSQL

# 3. Subir Airflow
docker compose build --no-cache
docker compose up -d
docker compose ps
# Configurar no UI: http://localhost:8080

# 4. Gerar Service Principals por tenant
pip install psycopg2-binary python-dotenv
python scripts/generate_azure_service_principals.py --interactive
bash azure/create_service_principals_*.sh

# 5. Monitorar exportações
docker logs -f airflow-scheduler | grep export_tickets_to_azure_datalake
az storage fs file list --account-name ticketsdatalake --file-system tickets-data --auth-mode login
```
