# Scripts de Automação Multi-Tenant

Esta pasta contém scripts para automatizar a configuração de Service Principals e ACLs no Azure Data Lake para isolamento multi-tenant.

---

## 📜 Scripts Disponíveis

### 1. `generate_azure_service_principals.py` - Geração Automatizada

Gera Service Principals do Azure e configura ACLs no Data Lake para tenants selecionadas.

#### Pré-requisitos

```bash
pip install psycopg2-binary python-dotenv
```

#### Como Usar

**Modo 1: Todas os tenants**

```bash
python scripts/generate_azure_service_principals.py
```

**Modo 2: Tenants específicas por nome**

```bash

# Busca parcial, case-insensitive

python scripts/generate_azure_service_principals.py --tenants "Tenant A" "Tenant B"

# Atalho

python scripts/generate_azure_service_principals.py -t "Tenant A"
```

**Modo 3: Tenants específicas por ID**

```bash
python scripts/generate_azure_service_principals.py --ids \
  "4ed977b6-51dd-59f8-acb6-0908a57cc1b7" \
  "a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

**Modo 4: Interativo (Recomendado)**

```bash
python scripts/generate_azure_service_principals.py --interactive

# Exemplo de interação

# Tenants disponíveis

# 1. Tenant A Trucks        |   1,234 tickets

# 2. Tenant B Parts        |     567 tickets

# 3. Tenant C    |     890 tickets

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
4. ✅ Salva credenciais em arquivo seguro (AZURE_CLIENT_ID, AZURE_CLIENT_SECRET, AZURE_TENANT_ID)

#### Outputs Gerados

```
azure/
  └── create_service_principals_YYYYMMDD_HHMMSS.sh  # Script executável

azure_client_docs_YYYYMMDD_HHMMSS/
  ├── empresaA-trucks_access_guide.md     # Documentação Tenant A
  ├── empresaB-parts_access_guide.md     # Documentação Tenant B
  └── ...

azure_credentials_YYYYMMDD_HHMMSS.txt  # Credenciais (GUARDAR SEGURO!)
```

#### Exemplos Práticos

**Adicionar novo tenant:**

```bash

# Nova tenant cadastrada no sistema

python scripts/generate_azure_service_principals.py -t "Novo Tenant"

# Executar script gerado

bash azure/create_service_principals_*.sh
```

**Gerar para top 5 clientes:**

```bash

# Modo interativo ordena por número de tickets

python scripts/generate_azure_service_principals.py -I

# Selecionar: 1-5

```

**Re-gerar para tenant específico:**

```bash

# Por ID (mais seguro que por nome)

python scripts/generate_azure_service_principals.py --ids \
  "4ed977b6-51dd-59f8-acb6-0908a57cc1b7"
```

#### Troubleshooting

**Erro: "Nenhum tenant encontrado"**

- Verifique se o filtro está correto (busca é case-insensitive e parcial)
- Use `--interactive` para ver todas as tenants disponíveis
- Confirme que existem registros na tabela `tenants`

**Erro: "AZURE_STORAGE_ACCOUNT_NAME não configurado"**

```bash

# Verificar .env

cat .env | grep AZURE

# Adicionar se necessário

echo "AZURE_STORAGE_ACCOUNT_NAME=ticketsdatalake" >> .env
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

**Modo 1: Interativo (um tenant por vez)**

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
  "4ed977b6-51dd-59f8-acb6-0908a57cc1b7" \
  "eba1770b-0826-4d5f-b7db-68fdb47a8ded" \
  "Tenant A Trucks"
```

**Modo 3: Batch (múltiplas tenants via CSV)**

```bash

# Criar arquivo CSV

cat > tenants_acls.csv << EOF
tenant_id,object_id,tenant_name
4ed977b6-51dd-59f8-acb6-0908a57cc1b7,eba1770b-0826-4d5f-b7db-68fdb47a8ded,Tenant A Trucks
a1b2c3d4-e5f6-7890-abcd-ef1234567890,obj-id-2,Tenant B Parts
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
AZURE_STORAGE_ACCOUNT_NAME=ticketsdatalake
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

Storage Account: ticketsdatalake
Container: tickets-data
==================================================

✅ Azure CLI autenticado
Usuário: <admin@tenant.com>

🔍 Verificando se Hierarchical Namespace está habilitado...
✅ HNS habilitado

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📁 Tenant: Tenant A Trucks
   Tenant ID: 4ed977b6-51dd-59f8-acb6-0908a57cc1b7
   Pasta: tenant_4ed977b6-51dd-59f8-acb6-0908a57cc1b7
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

✅ ACLs configuradas para Tenant A Trucks
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

## 🔐 Fluxo de Trabalho Completo

### Nova Tenant no Sistema

```bash

# 1. Tenant é cadastrada no PostgreSQL

# 2. Gerar Service Principal + ACLs

python scripts/generate_azure_service_principals.py -t "Novo Tenant"

# 3. Executar script gerado

bash azure/create_service_principals_*.sh

# 4. Distribuir credenciais

# Enviar arquivo azure_client_docs_*/nova-empresa_access_guide.md por canal seguro

# 5. DAG do Airflow detecta automaticamente e começa a exportar dados

```

### Validar Isolamento

```bash

# Testar acesso da Tenant A à sua pasta (deve funcionar)

az storage fs file list \
  --account-name ticketsdatalake \
  --file-system tickets-data \
  --path "tenant_<UUID_TENANT_A>/" \
  --auth-mode login \
  --account-key <CLIENT_SECRET_TENANT_A>

# Testar acesso da Tenant A à pasta da Tenant B (deve falhar com 403)

az storage fs file list \
  --account-name ticketsdatalake \
  --file-system tickets-data \
  --path "tenant_<UUID_TENANT_B>/" \
  --auth-mode login \
  --account-key <CLIENT_SECRET_TENANT_A>
```

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
