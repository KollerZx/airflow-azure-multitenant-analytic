#!/usr/bin/env python3
"""
Script para gerar Service Principals do Azure por tenant.

Este script:
1. Conecta ao PostgreSQL para listar todas os tenants (tenant_id)
2. Gera comandos Azure CLI para criar Service Principals por tenant
3. Configura ACLs POSIX para isolamento (SEM RBAC no Storage Account)
4. Gera documentação de conexão para cada cliente

Uso:
    # Todos os tenants
    python scripts/generate_azure_service_principals.py
    
    # Tenants específicos por nome (case-insensitive, busca parcial)
    python scripts/generate_azure_service_principals.py --tenants "Tenant A" "Tenant B"
    
    # Tenants específicos por ID
    python scripts/generate_azure_service_principals.py --ids "8c1dca21-ba17-5018-b56d-cf6395d413e5"
    
    # Interativo: escolher da lista
    python scripts/generate_azure_service_principals.py --interactive

Requisitos:
    - Azure CLI instalado (az)
    - Python 3.11+
    - psycopg2-binary
    - Variáveis de ambiente configuradas (.env)

Autor: Airflow Team
Data: 2026-01-15
"""

import os
import sys
import argparse
from datetime import datetime
import psycopg2
from dotenv import load_dotenv

# Carregar variáveis de ambiente
load_dotenv()

# Configurações do banco de dados
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': os.getenv('POSTGRES_PORT', '5432'),
    'database': os.getenv('POSTGRES_DB'),
    'user': os.getenv('POSTGRES_USER'),
    'password': os.getenv('POSTGRES_PASSWORD'),
}

# Schema do banco de dados
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')

# Configurações Azure
STORAGE_ACCOUNT = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
CONTAINER = os.getenv('AZURE_CONTAINER_NAME', 'tickets-data')
SUBSCRIPTION_ID = os.getenv('AZURE_SUBSCRIPTION_ID', '<SUBSTITUA_COM_SUBSCRIPTION_ID>')
RESOURCE_GROUP = os.getenv('AZURE_RESOURCE_GROUP', 'rg-airflow-datalake')


def get_tenants(filter_names=None, filter_ids=None):
    """
    Busca tenants do banco de dados com filtros opcionais.
    
    Args:
        filter_names (list): Lista de nomes para filtrar (busca parcial, case-insensitive)
        filter_ids (list): Lista de tenant_ids para filtrar (busca exata)
    
    Returns:
        list: Lista de tuplas (tenant_id, tenant_name, ticket_count)
    """
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        query = f"""
            SELECT DISTINCT
                tn.id as tenant_id,
                tn.name as tenant_name,
                COUNT(t.id) as ticket_count
            FROM {POSTGRES_SCHEMA}.tenants tn
            LEFT JOIN {POSTGRES_SCHEMA}.tickets t ON tn.id = t.tenant_id
        """
        
        conditions = []
        params = []
        
        # Filtro por nomes (busca parcial, case-insensitive)
        if filter_names:
            name_conditions = []
            for name in filter_names:
                name_conditions.append("LOWER(tn.name) LIKE LOWER(%s)")
                params.append(f"%{name}%")
            conditions.append(f"({' OR '.join(name_conditions)})")
        
        # Filtro por IDs (busca exata)
        if filter_ids:
            placeholders = ','.join(['%s'] * len(filter_ids))
            conditions.append(f"tn.id IN ({placeholders})")
            params.extend(filter_ids)
        
        if conditions:
            query += " WHERE " + " AND ".join(conditions)
        
        query += """
            GROUP BY tn.id, tn.name
            ORDER BY ticket_count DESC
        """
        
        cursor.execute(query, params)
        tenants = cursor.fetchall()
        
        cursor.close()
        conn.close()
        
        return tenants
    
    except Exception as e:
        print(f"❌ Erro ao buscar tenants: {e}")
        sys.exit(1)


def sanitize_sp_name(tenant_name):
    """
    Sanitiza nome do tenant para usar em Service Principal.
    
    Args:
        tenant_name (str): Nome do tenant
    
    Returns:
        str: Nome sanitizado (minúsculas, sem espaços, apenas alfanumérico)
    """
    import re
    name = tenant_name.lower()
    name = re.sub(r'[^a-z0-9-]', '-', name)
    name = re.sub(r'-+', '-', name)  # Remove hífens duplicados
    name = name.strip('-')
    return name[:50]  # Limitar tamanho


def generate_azure_cli_scripts(tenants):
    """
    Gera scripts Azure CLI para criar Service Principals por tenant.
    
    Args:
        tenants (list): Lista de tenants
    
    Returns:
        str: Script bash com comandos Azure CLI
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    script = f"""#!/bin/bash
# Script de criação de Service Principals por tenant
# Gerado automaticamente em {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
# Total de tenants: {len(tenants)}
#
# Requisitos:
# 1. Azure CLI instalado: https://learn.microsoft.com/en-us/cli/azure/install-azure-cli
# 2. Login realizado: az login
# 3. Subscription configurado: az account set --subscription <ID>
#
# IMPORTANTE: Salve os outputs (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET) em um local seguro!

set -e  # Parar em caso de erro

# Variáveis globais
STORAGE_ACCOUNT="{STORAGE_ACCOUNT}"
CONTAINER="{CONTAINER}"
SUBSCRIPTION_ID="{SUBSCRIPTION_ID}"
RESOURCE_GROUP="{RESOURCE_GROUP}"

echo "=================================================="
echo "Criação de Service Principals por Tenant"
echo "MODELO: ACLs POSIX apenas (SEM RBAC no Storage Account)"
echo "=================================================="
echo "Storage Account: $STORAGE_ACCOUNT"
echo "Container: $CONTAINER"
echo "Total de tenants: {len(tenants)}"
echo "=================================================="
echo ""
echo "⚠️  IMPORTANTE: Service Principals NÃO recebem RBAC no Storage Account"
echo "⚠️  Isolamento baseado apenas em ACLs POSIX nas pastas"
echo ""

# Verificar se está logado
if ! az account show &> /dev/null; then
    echo "❌ Azure CLI não está autenticado. Execute: az login"
    exit 1
fi

echo "✅ Azure CLI autenticado"
echo ""

# Criar arquivo de saída para credenciais
OUTPUT_FILE="azure_credentials_${{timestamp}}.txt"
echo "# Credenciais dos Service Principals por Tenant" > $OUTPUT_FILE
echo "# Gerado em: $(date)" >> $OUTPUT_FILE
echo "" >> $OUTPUT_FILE

"""
    
    for idx, (tenant_id, tenant_name, ticket_count) in enumerate(tenants, 1):
        sp_name = sanitize_sp_name(tenant_name)
        sp_full_name = f"sp-client-{sp_name}-read"
        tenant_folder = f"tenant_{tenant_id}"
        
        script += f"""
# ------------------------------------------------------------------------------
# [{idx}/{len(tenants)}] Tenant: {tenant_name}
# Tenant ID: {tenant_id}
# Total de tickets: {ticket_count:,}
# Service Principal: {sp_full_name}
# ------------------------------------------------------------------------------

echo "[{idx}/{len(tenants)}] Processando: {tenant_name}"

# 1. Verificar se Service Principal já existe
echo "  Verificando se Service Principal já existe..."
EXISTING_SP=$(az ad sp list --display-name "{sp_full_name}" --query "[0]" -o json 2>/dev/null)

if [ -z "$EXISTING_SP" ] || [ "$EXISTING_SP" == "null" ]; then
    echo "  Service Principal não existe. Criando..."
    
    # Criar Service Principal (sem permissões ainda)
    SP_OUTPUT=$(az ad sp create-for-rbac \\
        --name "{sp_full_name}" \\
        --skip-assignment \\
        --output json)
    
    if [ $? -ne 0 ]; then
        echo "❌ Erro ao criar Service Principal. Pulando tenant..."
        continue
    fi
    
    # Extrair credenciais
    APP_ID=$(echo $SP_OUTPUT | jq -r '.appId')
    AZURE_TENANT_ID=$(echo $SP_OUTPUT | jq -r '.tenant // .tenantId')
    PASSWORD=$(echo $SP_OUTPUT | jq -r '.password')
    
    echo "  ✅ Service Principal criado com sucesso"
    echo "     App ID: $APP_ID"
    echo "     Tenant ID: $AZURE_TENANT_ID"
    echo "     ⚠️  Client Secret só é exibido uma vez! Guardar: $PASSWORD"
    
    # Aguardar propagação do AD (crítico para novos SPs)
    echo "  Aguardando propagação do Azure AD (30s)..."
    sleep 30
else
    echo "  ✅ Service Principal já existe (modo idempotente)"
    
    # Extrair informações do SP existente
    APP_ID=$(echo $EXISTING_SP | jq -r '.appId')
    AZURE_TENANT_ID=$(az account show --query tenantId -o tsv)
    PASSWORD="***EXISTENTE_NAO_RECUPERAVEL***"
    
    echo "     App ID: $APP_ID"
    echo "     Tenant ID: $AZURE_TENANT_ID"
    echo "     ⚠️  Client Secret não pode ser recuperado (já foi criado anteriormente)"
    echo "     💡 Se perdeu o secret, delete o SP e execute novamente"
fi

# 2. Obter Object ID do Service Principal
OBJECT_ID=$(az ad sp show --id $APP_ID --query id -o tsv)
echo "  Object ID: $OBJECT_ID"

# 3. Configurar ACLs no Data Lake (IMPORTANTE: sem RBAC no Storage Account)
echo "  Configurando ACLs POSIX (isolamento baseado apenas em ACLs)..."
echo "  ℹ️  Não atribuindo RBAC no Storage Account (garantir isolamento)"
echo "  Configurando ACLs no Data Lake..."

if [ -f "./scripts/setup_azure_acls.sh" ]; then
    # Usar script idempotente existente
    ./scripts/setup_azure_acls.sh "{tenant_id}" "$OBJECT_ID" "{tenant_name}" || \\
        echo "⚠️  Erro ao configurar ACLs. Configure manualmente."
else
    # Fallback: configurar ACLs manualmente (menos robusto)
    echo "  ⚠️  Script setup_azure_acls.sh não encontrado. Usando método alternativo..."
    
    # Verificar se pasta existe
    if ! az storage fs directory exists \\
        --account-name $STORAGE_ACCOUNT \\
        --file-system $CONTAINER \\
        --name "{tenant_folder}" \\
        --auth-mode login \\
        --query "exists" -o tsv 2>/dev/null | grep -q "true"; then
        
        echo "     Criando pasta {tenant_folder}..."
        az storage fs directory create \\
            --account-name $STORAGE_ACCOUNT \\
            --file-system $CONTAINER \\
            --name "{tenant_folder}" \\
            --auth-mode login || echo "⚠️  Erro ao criar pasta"
    fi
    
    # Ler ACL atual ou usar padrão
    CURRENT_ACL=$(az storage fs access show \\
        --account-name $STORAGE_ACCOUNT \\
        --file-system $CONTAINER \\
        --path "{tenant_folder}" \\
        --auth-mode login \\
        --query "acl" -o tsv 2>/dev/null) || CURRENT_ACL="user::rwx,group::r-x,other::---"
    
    # Verificar se ACL já existe para este Object ID
    if echo "$CURRENT_ACL" | grep -q "user:$OBJECT_ID"; then
        echo "     ✅ ACL já configurada (idempotente)"
    else
        echo "     Adicionando ACL..."
        az storage fs access set \\
            --account-name $STORAGE_ACCOUNT \\
            --file-system $CONTAINER \\
            --path "{tenant_folder}" \\
            --acl "user::rwx,user:${{OBJECT_ID}}:r-x,group::---,other::---" \\
            --auth-mode login || echo "⚠️  Erro ao configurar ACL"
        
        # ACL recursiva
        az storage fs access update-recursive \\
            --account-name $STORAGE_ACCOUNT \\
            --file-system $CONTAINER \\
            --path "{tenant_folder}" \\
            --acl "user:${{OBJECT_ID}}:r-x" \\
            --auth-mode login \\
            --continue-on-failure true &>/dev/null || true
        
        # ACL padrão
        az storage fs access set-recursive \\
            --account-name $STORAGE_ACCOUNT \\
            --file-system $CONTAINER \\
            --path "{tenant_folder}" \\
            --acl "default:user::rwx,default:user:${{OBJECT_ID}}:r-x,default:group::---,default:other::---" \\
            --auth-mode login \\
            --continue-on-failure true &>/dev/null || true
    fi
fi

echo "✅ Tenant {tenant_name} processado com sucesso"
echo ""

# Salvar credenciais no arquivo
cat >> $OUTPUT_FILE << EOF

# ------------------------------------------------------------------------------
# Tenant: {tenant_name}
# ------------------------------------------------------------------------------
Tenant ID: {tenant_id}
Tenant Name: {tenant_name}
Total de Tickets: {ticket_count:,}
Service Principal: {sp_full_name}
Pasta no Data Lake: {tenant_folder}

# Credenciais (GUARDAR EM LOCAL SEGURO!)
AZURE_TENANT_ID=$AZURE_TENANT_ID
AZURE_CLIENT_ID=$APP_ID
AZURE_CLIENT_SECRET=$PASSWORD

# URL de acesso
Data Lake URL: https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/

EOF

"""
    
    script += f"""
echo ""
echo "=================================================="
echo "✅ Criação de Service Principals concluída!"
echo "=================================================="
echo "Total criado: {len(tenants)} tenants"
echo "Credenciais salvas em: $OUTPUT_FILE"
echo ""
echo "PRÓXIMOS PASSOS:"
echo "1. Revisar o arquivo $OUTPUT_FILE"
echo "2. Distribuir credenciais para cada cliente (comunicação segura!)"
echo "3. Testar isolamento (ver MULTI_TENANT_ISOLATION_LIMITATIONS.md)"
echo "4. Configurar alertas no Azure Monitor"
echo ""
echo "⚠️  LEMBRETE: Isolamento funciona porque NÃO há RBAC no Storage Account"
echo "⚠️  Apenas ACLs POSIX controlam o acesso"
echo ""
"""
    
    return script


def generate_client_documentation(tenants):
    """
    Gera documentação individual para cada cliente.
    
    Args:
        tenants (list): Lista de tenants
    """
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_dir = f"azure_client_docs_{timestamp}"
    os.makedirs(output_dir, exist_ok=True)
    
    for tenant_id, tenant_name, ticket_count in tenants:
        sp_name = sanitize_sp_name(tenant_name)
        tenant_folder = f"tenant_{tenant_id}"
        
        doc_content = f"""# Acesso aos Dados - {tenant_name}

## 📋 Informações do Tenant

- **Nome:** {tenant_name}
- **Tenant ID:** `{tenant_id}`
- **Total de Tickets:** {ticket_count:,}
- **Pasta no Data Lake:** `{tenant_folder}/`

## 🔑 Credenciais de Acesso

**IMPORTANTE:** Estas credenciais são confidenciais e permitem acesso SOMENTE aos seus dados.

```
Storage Account: {STORAGE_ACCOUNT}
Container: {CONTAINER}
Tenant ID: <SERÁ_FORNECIDO>
Client ID: <SERÁ_FORNECIDO>
Client Secret: <SERÁ_FORNECIDO>
```

## 📂 Estrutura de Dados

Seus dados estão organizados da seguinte forma:

```
{CONTAINER}/
└── {tenant_folder}/
    ├── tickets/
    │   └── year=2026/month=01/day=14/*.parquet
```

## 🔗 URLs de Acesso Direto

**IMPORTANTE para Power BI Service (app.powerbi.com):** Use as URLs específicas abaixo, apontando diretamente para cada tipo de dado:

### 📊 Tickets (Chamados)
```
https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/tickets/
```

```

> ⚠️ **Nota:** A URL raiz (`{tenant_folder}/`) contém apenas subdiretórios. Você deve apontar diretamente para a pasta específica do tipo de dado desejado.

## 🔗 Como Consumir os Dados

> 🔒 **IMPORTANTE - Autenticação com ACLs:** Suas credenciais utilizam **ACLs POSIX** para isolamento multi-tenant (sem RBAC). Esta abordagem garante segurança máxima, mas possui uma limitação: **Power BI Desktop requer RBAC** para autenticação OAuth interativa. Por isso, **recomendamos usar Power BI Service** para criar conexões iniciais.

### Opção 1A: Power BI Service (app.powerbi.com) - **RECOMENDADO**

**✅ Por que usar o Service primeiro?**
- Funciona nativamente com autenticação Service Principal + ACLs
- Não requer RBAC (mantém isolamento de segurança)
- Após criar, você pode exportar o relatório e abrir no Power BI Desktop

### Opção 1B: Power BI Desktop (Uso Secundário)

**Passo 1: Conectar ao Data Lake**

1. Acesse **app.powerbi.com**
2. Vá em **Get Data** → **Azure Data Lake Storage Gen2**
3. **Use URL específica** (não use a pasta raiz `{tenant_folder}/`):
   - ✅ **Correto:** `https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/tickets/`
   - ❌ **Incorreto:** `https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/`
4. Configure credenciais:
   - Authentication: **Service Principal**
   - Tenant ID: `<fornecido>`
   - Service Principal ID: `<fornecido>`
   - Service Principal Key: `<fornecido>`

**Passo 2: Combinar Arquivos Parquet (escolha uma opção)**

**Opção A: Botão "Combine" (MAIS SIMPLES)**

No Navigator:
1. Você verá a estrutura de pastas `year=2026/month=01/day=20/`
2. Clique no botão **"Combine"** no canto inferior direito
3. Power BI detecta automaticamente os arquivos Parquet e combina todos
4. Clique em **"Load"** para carregar os dados

**Opção B: Script Power Query Manual (SE "COMBINE" NÃO APARECER)**

Clique em **"Transform Data"** e cole um dos scripts abaixo no Power Query Editor:

**Script 1: Simples (Recomendado)**

```powerquery
let
    Source = AzureStorage.DataLake("https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/tickets/"),
    
    // Filtrar apenas arquivos .parquet (não pastas)
    FilterFiles = Table.SelectRows(Source, each [Extension] = ".parquet"),
    
    // Adicionar coluna com conteúdo dos Parquet
    AddParquetContent = Table.AddColumn(FilterFiles, "ParquetData", each 
        try Parquet.Document([Content]) otherwise null
    ),
    
    // Remover linhas com erro
    RemoveErrors = Table.SelectRows(AddParquetContent, each [ParquetData] <> null),
    
    // Combinar todos os arquivos
    CombinedData = Table.Combine(RemoveErrors[ParquetData]),
    
    Result = CombinedData
in
    Result
```

**Script 2: Com Tratamento de Pastas Vazias**

```powerquery
let
    Source = AzureStorage.DataLake("https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/tickets/"),
    
    // Navegar recursivamente em todas as subpastas
    KeepAllRows = Table.SelectRows(Source, each true),
    
    // Processar cada linha (pasta ou arquivo)
    ProcessFiles = Table.AddColumn(KeepAllRows, "FileContent", each
        if [Folder Path] <> null and [Extension] = ".parquet" then
            try Parquet.Document([Content]) otherwise null
        else
            null
    ),
    
    // Remover linhas sem conteúdo
    FilterFiles = Table.SelectRows(ProcessFiles, each [FileContent] <> null),
    
    // Combinar todos os Parquet
    CombineParquet = if Table.RowCount(FilterFiles) > 0 then
        Table.Combine(FilterFiles[FileContent])
    else
        #table({"Erro"}, {{"Nenhum arquivo Parquet encontrado"}})
in
    CombineParquet
```

**Script 3: Usando Table.SelectRows Avançado**

```powerquery
let
    Source = AzureStorage.DataLake("https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/tickets/"),
    
    // Filtrar apenas arquivos que terminam com .parquet
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

**Importante:** Substitua:
- `{STORAGE_ACCOUNT}` pelo nome real: `{STORAGE_ACCOUNT}`
- `{CONTAINER}` pelo container: `{CONTAINER}`
- `{tenant_folder}` pela sua pasta: `{tenant_folder}`

**Passo 3: Criar Múltiplas Conexões (se necessário)**

Para acessar múltiplos tipos de dados, crie conexões separadas:

1. **Tickets:**
   - URL: `https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/tickets/`
   - Nome da Query: `Tickets`

**Passo 4: Exportar para Power BI Desktop (Opcional)**

Se desejar trabalhar no Power BI Desktop:

1. Após criar o relatório no Power BI Service, clique em **"Download this file"** → **".pbix file"**
2. Abra o arquivo .pbix baixado no Power BI Desktop
3. O Desktop usará as conexões já configuradas no Service
4. Faça edições e republique quando necessário

> ⚠️ **Nota:** Tentar criar conexão nova diretamente no Desktop falhará com "Access forbidden" devido à limitação de ACLs. Sempre crie primeiro no Service.

### Opção 2: Python (Pandas)

```python
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import pandas as pd
from io import BytesIO

# Suas credenciais
AZURE_TENANT_ID = "<fornecido>"
AZURE_CLIENT_ID = "<fornecido>"
AZURE_CLIENT_SECRET = "<fornecido>"

# Autenticação
credential = ClientSecretCredential(AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET)
account_url = "https://{STORAGE_ACCOUNT}.dfs.core.windows.net"
service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
file_system_client = service_client.get_file_system_client("{CONTAINER}")

# Exemplo 1: Listar subpastas disponíveis
print("Tipos de dados disponíveis:")
base_path = "{tenant_folder}"
directory_client = file_system_client.get_directory_client(base_path)
paths = directory_client.get_paths()
for path in paths:
    if path.is_directory:
        print(f"  - {{path.name.split('/')[-1]}}/")

# Exemplo 2: Ler um arquivo Parquet específico
file_path = "{tenant_folder}/tickets/year=2026/month=01/day=14/tickets_20260114_103000.parquet"
file_client = file_system_client.get_file_client(file_path)
download = file_client.download_file()
df = pd.read_parquet(BytesIO(download.readall()))

print(f"\nTotal de tickets: {{len(df)}}")
print(df.head())

# Exemplo 3: Ler todos os Parquets de uma pasta (combinar)
tickets_path = "{tenant_folder}/tickets"
dir_client = file_system_client.get_directory_client(tickets_path)
all_files = [p for p in dir_client.get_paths(recursive=True) if p.name.endswith('.parquet')]

dataframes = []
for file_path in all_files[:5]:  # Primeiros 5 arquivos como exemplo
    file_client = file_system_client.get_file_client(file_path.name)
    download = file_client.download_file()
    df = pd.read_parquet(BytesIO(download.readall()))
    dataframes.append(df)

df_combined = pd.concat(dataframes, ignore_index=True)
print(f"\nTotal combinado: {{len(df_combined)}} registros")
```

### Opção 3: Excel (Power Query)

1. Abra o Excel
2. Vá em **Dados** → **Obter Dados** → **Do Azure** → **Azure Data Lake Storage Gen2**
3. Insira credenciais conforme fornecido
4. Selecione os arquivos Parquet desejados
5. Transforme e carregue os dados

### Opção 4: Tableau

1. Abra o Tableau
2. Conecte a **Azure Data Lake Storage Gen2**
3. Forneça as credenciais do Service Principal
4. Selecione os arquivos Parquet
5. Crie suas visualizações

## 🔒 Segurança

- ✅ Suas credenciais dão acesso SOMENTE à pasta `{tenant_folder}/`
- ✅ Você NÃO consegue ver dados de outros tenants
- ✅ Todos os acessos são auditados e logados
- ❌ NUNCA compartilhe suas credenciais com terceiros

## 📊 Atualização dos Dados

- **Frequência:** A cada 15 minutos
- **Janela de dados:** Últimas 24 horas (incremental)
- **Formato:** Parquet com compressão Snappy
- **Particionamento:** Por ano/mês/dia

## 🆘 Suporte

### ❓ FAQ - Problemas Comuns

**Q: "Esta tabela não tem nenhuma linha" ou "Alguns passos não foram concluídos" no Power BI Service**

A: Este erro tem duas causas principais:

1. **URL incorreta:** Você está apontando para a pasta raiz (`{tenant_folder}/`). Use URLs específicas:
   - ✅ `https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/tickets/`
   - ❌ `https://{STORAGE_ACCOUNT}.dfs.core.windows.net/{CONTAINER}/{tenant_folder}/`

2. **Arquivos não combinados:** A estrutura é importada mas sem dados. Solução:
   - Use o botão **"Combine"** no Navigator, OU
   - Use um dos **3 scripts Power Query** fornecidos na seção "Opção 1B" acima

**Q: Erro "Não conseguimos converter Table em Record" no Power Query**

A: O script Power Query está incorreto. Use um dos **3 scripts fornecidos** na seção "Opção 1B":
- Script 1 (Simples) - funciona na maioria dos casos
- Script 2 (Com tratamento de erros) - mais robusto
- Script 3 (Avançado) - alternativa usando List.Transform

**Q: Power BI Desktop mostra "Access forbidden" ou "não autorizado"**

A: Isso é esperado! Suas credenciais usam **ACLs sem RBAC** para segurança. O Power BI Desktop requer RBAC para autenticação OAuth. Solução:
1. Crie a conexão no **Power BI Service** (app.powerbi.com) usando as instruções da "Opção 1A"
2. Após criar o relatório no Service, baixe o arquivo .pbix
3. Abra o .pbix no Desktop - ele usará as credenciais já configuradas
4. Edite e republique quando necessário

**Q: Power BI Service não lista arquivos ou mostra "tabela vazia"**

A: Certifique-se de:
1. Usar URL da **subpasta específica** (`.../tickets/`) não a raiz
2. Ter configurado as credenciais do Service Principal corretamente
3. Usar botão "Combine" OU um dos scripts Power Query fornecidos

**Q: Preciso acessar múltiplos tipos de dados (tickets + telemetria)?**

A: Crie **múltiplas conexões** no Power BI, uma para cada tipo:
- Conexão 1: `.../{tenant_folder}/tickets/`
- Conexão 2: `.../{tenant_folder}/telemetry/` (Se disponível)
- Depois relacione as tabelas usando campos comuns (ticket_id, tenant_id, etc.)

**Q: Como sei quais pastas estão disponíveis?**

A: Duas opções:
1. Use o script Python (Exemplo 1) para listar programaticamente
2. As pastas padrão são: `tickets/`

### 📞 Contato para Suporte

Se os problemas persistirem:

1. ✅ Verifique se as credenciais estão corretas
2. ✅ Confirme que está usando a URL da **subpasta** (não a raiz `{tenant_folder}/`)
3. ✅ Teste a conexão usando **Power BI Service** ou Python
4. 📧 Entre em contato com o suporte técnico informando:
   - URL exata que está tentando acessar
   - Ferramenta utilizada (Power BI Desktop/Service, Python, Excel, etc.)
   - Mensagem de erro completa

---

**Última atualização:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
"""
        
        # Salvar documentação
        filename = f"{output_dir}/{sp_name}_access_guide.md"
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(doc_content)
        
        print(f"✅ Documentação criada: {filename}")
    
    print(f"\n📁 Documentação salva em: {output_dir}/")
    return output_dir


def interactive_select_tenants(all_tenants):
    """
    Permite seleção interativa de tenants.
    
    Args:
        all_tenants (list): Lista de todos os tenants disponíveis
    
    Returns:
        list: Lista de tenants selecionados
    """
    print("\n" + "=" * 80)
    print("MODO INTERATIVO - Selecione os tenants")
    print("=" * 80)
    print("\nTenants disponíveis:")
    print("-" * 80)
    
    for idx, (tenant_id, tenant_name, ticket_count) in enumerate(all_tenants, 1):
        print(f"{idx:2d}. {tenant_name:40s} | {ticket_count:>8,} tickets")
    
    print("-" * 80)
    print("\nComo selecionar:")
    print("  • Números individuais: 1 3 5")
    print("  • Intervalos: 1-5")
    print("  • Combinado: 1 3-5 7")
    print("  • Todas: all ou *")
    print("  • Cancelar: q ou quit\n")
    
    while True:
        selection = input("Selecione os tenants: ").strip().lower()
        
        if selection in ['q', 'quit', 'exit']:
            print("❌ Operação cancelada")
            sys.exit(0)
        
        if selection in ['all', '*', '']:
            return all_tenants
        
        try:
            selected_indices = set()
            
            for part in selection.split():
                if '-' in part:
                    # Intervalo (ex: 1-5)
                    start, end = map(int, part.split('-'))
                    selected_indices.update(range(start, end + 1))
                else:
                    # Número individual
                    selected_indices.add(int(part))
            
            # Validar índices
            if any(i < 1 or i > len(all_tenants) for i in selected_indices):
                print(f"❌ Erro: números devem estar entre 1 e {len(all_tenants)}")
                continue
            
            # Retornar tenants selecionados
            selected = [all_tenants[i-1] for i in sorted(selected_indices)]
            
            print(f"\n✅ {len(selected)} tenant(s) selecionada(s):")
            for tenant_id, tenant_name, ticket_count in selected:
                print(f"   • {tenant_name}")
            
            confirm = input("\nConfirmar seleção? (s/n): ").strip().lower()
            if confirm in ['s', 'y', 'yes', 'sim']:
                return selected
            
        except ValueError:
            print("❌ Erro: formato inválido. Exemplo válido: 1 3-5 7")
            continue


def parse_arguments():
    """
    Parse argumentos de linha de comando.
    
    Returns:
        argparse.Namespace: Argumentos parseados
    """
    parser = argparse.ArgumentParser(
        description='Gera Service Principals do Azure por tenant',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Exemplos de uso:

            # Todos os tenants
            python scripts/generate_azure_service_principals.py
            
            # Tenants específicos por nome (busca parcial)
            python scripts/generate_azure_service_principals.py --tenants "Tenant A" "Tenant B"
            
            # Tenants específicos por ID

            # Modo interativo (escolher da lista)
            python scripts/generate_azure_service_principals.py --interactive
        """
    )
    
    parser.add_argument(
        '--tenants', '-t',
        nargs='+',
        metavar='NOME',
        help='Filtrar tenants por nome (busca parcial, case-insensitive)'
    )
    
    parser.add_argument(
        '--ids', '-i',
        nargs='+',
        metavar='UUID',
        help='Filtrar tenants por tenant_id (busca exata)'
    )
    
    parser.add_argument(
        '--interactive', '-I',
        action='store_true',
        help='Modo interativo: escolher tenants da lista'
    )
    
    return parser.parse_args()


def main():
    # Parse argumentos
    args = parse_arguments()
    
    print("=" * 80)
    print("Script de Geração de Service Principals do Azure por Tenant")
    print("=" * 80)
    print()
    
    # Validar configurações
    if not STORAGE_ACCOUNT:
        print("❌ AZURE_STORAGE_ACCOUNT_NAME não configurado em .env")
        sys.exit(1)
    
    if not all([DB_CONFIG['host'], DB_CONFIG['database'], DB_CONFIG['user']]):
        print("❌ Configurações do PostgreSQL não encontradas em .env")
        sys.exit(1)
    
    print(f"✅ Storage Account: {STORAGE_ACCOUNT}")
    print(f"✅ Container: {CONTAINER}")
    print(f"✅ Banco de Dados: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    # Mostrar filtros aplicados
    if args.tenants:
        print(f"🔍 Filtro por nome: {', '.join(args.tenants)}")
    if args.ids:
        print(f"🔍 Filtro por ID: {', '.join(args.ids)}")
    if args.interactive:
        print(f"🎮 Modo interativo ativado")
    
    print()
    
    # Buscar tenants (com ou sem filtros)
    print("🔍 Buscando tenants no banco de dados...")
    
    if args.interactive:
        # Modo interativo: buscar todas e deixar usuário escolher
        all_tenants = get_tenants()
        if not all_tenants:
            print("⚠️  Nenhuma tenant encontrada no banco de dados")
            sys.exit(0)
        tenants = interactive_select_tenants(all_tenants)
    else:
        # Modo normal: aplicar filtros se fornecidos
        tenants = get_tenants(
            filter_names=args.tenants,
            filter_ids=args.ids
        )
    
    if not tenants:
        print("⚠️  Nenhuma tenant encontrada com os filtros especificados")
        print("\nDica: Use --interactive para ver todas as tenants disponíveis")
        sys.exit(0)
    
    print(f"✅ {len(tenants)} tenant(s) encontrada(s)")
    print()
    
    # Mostrar resumo
    if not args.interactive:
        print("Tenants selecionadas:")
        print("-" * 80)
        for idx, (tenant_id, tenant_name, ticket_count) in enumerate(tenants, 1):
            print(f"{idx:2d}. {tenant_name:40s} | {ticket_count:>8,} tickets | ID: {tenant_id}")
        print("-" * 80)
        print()
    
    # Gerar script Azure CLI
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    script_filename = f"azure/create_service_principals_{timestamp}.sh"
    os.makedirs('azure', exist_ok=True)
    
    print("📝 Gerando script Azure CLI...")
    azure_script = generate_azure_cli_scripts(tenants)
    
    with open(script_filename, 'w') as f:
        f.write(azure_script)
    
    # Dar permissão de execução
    os.chmod(script_filename, 0o755)
    
    print(f"✅ Script criado: {script_filename}")
    print()
    
    # Gerar documentação por cliente
    print("📄 Gerando documentação por cliente...")
    docs_dir = generate_client_documentation(tenants)
    print()
    
    # Resumo final
    print("=" * 80)
    print("✅ GERAÇÃO CONCLUÍDA COM SUCESSO!")
    print("=" * 80)
    print()
    print("Próximos passos:")
    print(f"1. Execute o script: bash {script_filename}")
    print(f"2. Revise as credenciais geradas em: azure_credentials_<timestamp>.txt")
    print(f"3. Distribua documentação aos clientes: {docs_dir}/")
    print("4. Teste o acesso de pelo menos um cliente")
    print()
    print("IMPORTANTE: As credenciais são sensíveis! Use comunicação segura.")
    print()


if __name__ == '__main__':
    main()
