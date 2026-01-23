# Scripts de Automação e Configuração

Esta pasta contém scripts para automatizar a configuração do projeto, incluindo Service Principals Azure, ACLs, e lifecycle policies.

---

## 📜 Scripts Disponíveis

### 1. Scripts Azure Multi-Tenant

#### `generate_azure_service_principals.py` - Geração Automatizada

Gera Service Principals do Azure e configura ACLs no Data Lake para tenants selecionadas.

#### Pré-requisitos

```bash
pip install psycopg2-binary python-dotenv
```

#### Como Usar

**Modo 1: Todas as tenants**

```bash
python scripts/generate_azure_service_principals.py
```

**Modo 2: Tenants específicas por nome**

```bash

# Busca parcial, case-insensitive

python scripts/generate_azure_service_principals.py --tenants "Empresa A" "Empresa B"

# Atalho

python scripts/generate_azure_service_principals.py -c "Empresa A"
```

**Modo 3: Tenants específicas por ID**

```bash
python scripts/generate_azure_service_principals.py --ids \
  "b33d33da-def7-5132-a75b-a2bad300743d" \
  "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

**Modo 4: Interativo (Recomendado)**

```bash
python scripts/generate_azure_service_principals.py --interactive

# Exemplo de interação

# Tenants disponíveis

# 1. Empresa A   Trucks      |   1,234 tickets

# 2. Empresa B Parts        |     567 tickets

# 3. Empresa C Service    |     890 tickets

#

# Selecione as tenants: 1 3

# (aceita: números, intervalos 1-3, "all", "q" para sair)

```

#### O que o script faz?

1. ✅ Conecta ao PostgreSQL e lista tenants (com filtros se fornecidos)
2. ✅ Gera script bash com comandos Azure CLI para:
   - Criar Service Principal por tenant
   - Obter Object ID do Service Principal
   - Chamar `setup_azure_acls.sh` para configurar ACLs
3. ✅ Gera documentação individual para cada cliente
4. ✅ Salva credenciais em arquivo seguro (CLIENT_ID, CLIENT_SECRET, TENANT_ID)

#### Outputs Gerados

```
azure/
  └── create_service_principals_YYYYMMDD_HHMMSS.sh  # Script executável

azure_client_docs_YYYYMMDD_HHMMSS/
  ├── empresaA-trucks_access_guide.md     # Documentação Empresa A
  ├── empresaB-parts_access_guide.md     # Documentação Empresa B
  └── ...

azure_credentials_YYYYMMDD_HHMMSS.txt  # Credenciais (GUARDAR SEGURO!)
```

#### Exemplos Práticos

**Adicionar nova tenant:**

```bash

# Nova tenant cadastrada no sistema

python scripts/generate_azure_service_principals.py -c "Nova Tenant"

# Executar script gerado

bash azure/create_service_principals_*.sh
```

**Gerar para top 5 clientes:**

```bash

# Modo interativo ordena por número de tickets

python scripts/generate_azure_service_principals.py -I

# Selecionar: 1-5

```

**Re-gerar para tenant específica:**

```bash

# Por ID (mais seguro que por nome)

python scripts/generate_azure_service_principals.py --ids \
  "b33d33da-def7-5132-a75b-a2bad300743d"
```

#### Troubleshooting

**Erro: "Nenhuma tenant encontrada"**

- Verifique se o filtro está correto (busca é case-insensitive e parcial)
- Use `--interactive` para ver todas as tenants disponíveis
- Confirme que existem registros na tabela `tenants`

**Erro: "AZURE_STORAGE_ACCOUNT_NAME não configurado"**

```bash

# Verificar .env

cat .env | grep AZURE

# Adicionar se necessário

echo "AZURE_STORAGE_ACCOUNT_NAME=stticketsdatalake" >> .env
echo "AZURE_CONTAINER_NAME=tickets-data" >> .env
```

**Ver ajuda completa:**

```bash
python scripts/generate_azure_service_principals.py --help
```

---

### 2. `setup_azure_acls.sh` - Configuração de ACLs

Configura ACLs (Access Control Lists) no Azure Data Lake Storage Gen2 para isolar dados por tenant.

#### Como Usar

**Modo 1: Interativo (uma tenant por vez)**

```bash
./scripts/setup_azure_acls.sh

# O script pedirá

# - Tenant ID (UUID)

# - Service Principal Object ID

# - Nome do Tenant (opcional)

```

**Modo 2: Argumentos diretos**

```bash
./scripts/setup_azure_acls.sh \
  "b33d33da-def7-5132-a75b-a2bad300743d" \
  "eba1770b-0826-4d5f-b7db-68fdb47a8ded" \
  "Empresa A "
```

**Modo 3: Batch (múltiplas tenants via CSV)**

```bash

# Criar arquivo CSV

cat > tenants_acls.csv << EOF
tenant_id,object_id,tenant_name
b33d33da-def7-5132-a75b-a2bad300743d,eba1770b-0826-4d5f-b7db-68fdb47a8ded,Empresa A 
a1b2c3d4-e5f6-7890-abcd-ef1234567890,obj-id-2,Empresa B Parts
EOF

# Executar

./scripts/setup_azure_acls.sh --batch tenants_acls.csv
```

#### Pré-requisitos

1. **Azure CLI instalado e autenticado:**

```bash

# Instalar

curl -sL <https://aka.ms/InstallAzureCLIDeb> | sudo bash

# Autenticar

az login

# Configurar subscription

az account set --subscription "<SUBSCRIPTION_ID>"
```

1. **Variáveis no `.env`:**

```bash
AZURE_STORAGE_ACCOUNT_NAME=stticketsdatalake
AZURE_CONTAINER_NAME=tickets-data
```

1. **Permissões adequadas:**
   - Role `Storage Blob Data Owner` no Storage Account
   - Ou `Owner`/`Contributor` no recurso

#### O que o script faz?

1. ✅ Verifica se HNS (Hierarchical Namespace) está habilitado
2. ✅ Cria pasta `tenant_<uuid>` se não existir
3. ✅ Lê ACL atual e adiciona entrada para Service Principal (r-x)
4. ✅ Configura permissão --x no diretório raiz (/) para navegação
5. ✅ Aplica ACL recursivamente em arquivos existentes
6. ✅ Define ACL padrão para novos arquivos
7. ✅ É idempotente (pode executar múltiplas vezes sem problemas)

#### Exemplo de Output

```
==================================================

Azure Data Lake ACL Setup - Multi-tenant
==================================================

Storage Account: stticketsdatalake
Container: tickets-data
==================================================

✅ Azure CLI autenticado
Usuário: <admin@tenant.com>

🔍 Verificando se Hierarchical Namespace está habilitado...
✅ HNS habilitado

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📁 Tenant: Empresa A 
   Tenant ID: b33d33da-def7-5132-a75b-a2bad300743d
   Pasta: tenant_b33d33da-def7-5132-a75b-a2bad300743d
   Service Principal Object ID: eba1770b-0826-4d5f-b7db-68fdb47a8ded
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

📋 ACL atual da pasta:
   user::rwx,group::---,other::---

🔧 Adicionando ACL para Service Principal...
✅ ACL adicionada: user:eba1770b-0826-4d5f-b7db-68fdb47a8ded:r-x

🔄 Aplicando ACL recursivamente (arquivos existentes)...
Successfully processed 150 file(s), 10 director(ies)
✅ ACL aplicada recursivamente

🔄 Configurando ACL padrão (novos arquivos)...
Successfully processed 10 director(ies)
✅ ACL padrão configurada

🔑 Configurando permissão no diretório raiz (--x)...
✅ Permissão --x adicionada no diretório raiz

✅ ACLs configuradas para Empresa A 
```

#### Troubleshooting

**Erro: "This request is not authorized (403)"**

```bash

# Verificar permissões

az role assignment list \
  --assignee $(az ad signed-in-user show --query objectId -o tsv) \
  --scope /subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT

# Atribuir role necessária

az role assignment create \
  --role "Storage Blob Data Owner" \
  --assignee $(az ad signed-in-user show --query objectId -o tsv) \
  --scope /subscriptions/<SUB>/resourceGroups/<RG>/providers/Microsoft.Storage/storageAccounts/$STORAGE_ACCOUNT
```

**Erro: "Hierarchical Namespace not enabled"**

- ACLs POSIX requerem ADLS Gen2 (HNS habilitado)
- Você precisa recriar o Storage Account com `--hierarchical-namespace true`

---

### 3. `create_isolated_storage_accounts.sh` - Storage Accounts Separados (Opcional)

**Nota**: Este script implementa isolamento via Storage Accounts separados (não é a estratégia principal).

#### Quando Usar

Use este script apenas se um cliente **exigir isolamento físico absoluto**:

- Requisitos rigorosos de compliance (HIPAA, LGPD nível máximo)
- Dados altamente sensíveis
- Exigências contratuais específicas de auditoria

#### Como Funciona

Cria um Storage Account dedicado por tenant:

- `sttickets-empresaA`
- `sttickets-empresaB`
- `sttickets-cliente3`

O Service Principal tem acesso **apenas ao seu próprio Storage Account**, impossibilitando acesso a dados de outras tenants.

#### Uso

```bash

# Simular (dry-run)

bash scripts/create_isolated_storage_accounts.sh --dry-run

# Executar

bash scripts/create_isolated_storage_accounts.sh

# Tenant específica

bash scripts/create_isolated_storage_accounts.sh <TENANT_ID>
```

Veja [MULTI_TENANT_ISOLATION_LIMITATIONS.md](../MULTI_TENANT_ISOLATION_LIMITATIONS.md) para comparação detalhada entre as estratégias.

---

## 🔐 Fluxo de Trabalho Completo

### Nova Tenant no Sistema

```bash

# 1. Tenant é cadastrada no PostgreSQL

# 2. Gerar Service Principal + ACLs

python scripts/generate_azure_service_principals.py -c "Nova Tenant"

# 3. Executar script gerado

bash azure/create_service_principals_*.sh

# 4. Distribuir credenciais

# Enviar arquivo azure_client_docs_*/nova-tenant_access_guide.md por canal seguro

# 5. DAG do Airflow detecta automaticamente e começa a exportar dados

```

### Validar Isolamento

```bash

# Testar acesso da Tenant A à sua pasta (deve funcionar)

az storage fs file list \
  --account-name stticketsdatalake \
  --file-system tickets-data \
  --path "tenant_<UUID_EMPRESA_A>/" \
  --auth-mode login \
  --account-key <CLIENT_SECRET_EMPRESA_A>

# Testar acesso da Tenant A à pasta da Tenant B (deve falhar com 403)

az storage fs file list \
  --account-name stticketsdatalake \
  --file-system tickets-data \
  --path "tenant_<UUID_EMPRESA_B>/" \
  --auth-mode login \
  --account-key <CLIENT_SECRET_EMPRESA_A>
```

Veja guia completo: [TESTE_ISOLAMENTO.md](../TESTE_ISOLAMENTO.md)

---

### 2. Scripts de Lifecycle Management (Azure Storage)

#### `lifecycle-policy.json` - Política de Retenção

Configuração de lifecycle management para otimizar custos do Azure Storage:

- **Hot Tier** (0-30 dias): Dados recentes
- **Cool Tier** (30-90 dias): Dados intermediários  
- **Archive Tier** (90-730 dias): Dados antigos
- **Delete** (> 730 dias): Remoção automática após 2 anos

**Personalizar política:**

```bash
# Editar períodos conforme necessidade
vim scripts/lifecycle-policy.json

# Após editar, reaplicar
./scripts/apply_lifecycle_policy.sh
```

#### `apply_lifecycle_policy.sh` - Aplicar Lifecycle Policy

Aplica a política de lifecycle no Azure Storage Account.

**Pré-requisitos:**

- Azure CLI instalado e autenticado (`az login`)
- Variáveis de ambiente configuradas (`.env`)
- Permissão de Contributor no Storage Account

**Como usar:**

```bash
# Tornar executável (primeira vez)
chmod +x scripts/apply_lifecycle_policy.sh

# Executar
./scripts/apply_lifecycle_policy.sh
```

**Economia estimada:** ~77% nos custos após 90 dias (Hot → Archive)

---

## 🔒 Segurança

- **NUNCA** commite arquivos gerados (`azure/*`, `azure_credentials_*`, `azure_client_docs_*`)
- Rotacione credenciais regularmente (Service Principals expiram em 2 anos por padrão)
- Use Azure Key Vault para armazenar secrets em produção
- Habilite auditoria e alertas no Azure Monitor
- Distribua credenciais por canais seguros (não por email)
- Configure alertas para acessos negados (403) em excesso

---

## 📚 Documentação Relacionada

- [AZURE_DATALAKE.md](../AZURE_DATALAKE.md) - Guia completo de setup Azure
- [QUICKSTART.md](../QUICKSTART.md) - Setup rápido do projeto
