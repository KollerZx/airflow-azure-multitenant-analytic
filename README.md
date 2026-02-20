# 🚀 Pipeline Airflow → PostgreSQL → Azure Data Lake (Multi-Tenant)

Pipeline ETL automatizado para exportação de dados de chamados (tickets) para Azure Data Lake Storage Gen2, com **isolamento físico por tenant** (tenant_id).

[![Airflow](https://img.shields.io/badge/Airflow-2.8.1-blue)](https://airflow.apache.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-12%2B-blue)](https://www.postgresql.org/)
[![Azure](https://img.shields.io/badge/Azure-Data%20Lake%20Gen2-blue)](https://azure.microsoft.com/services/storage/data-lake-storage/)

---

## 📋 O Que Este Projeto Faz?

✅ **Extrai** dados de chamados (tickets) do PostgreSQL via views materializadas  
✅ **Transforma** em múltiplos formatos (Parquet + CSV) otimizados  
✅ **Exporta** para Azure Data Lake Gen2 a cada 15 minutos via Airflow  
✅ **Isola** dados por tenant com pastas físicas separadas (tenant_id)  
✅ **Distribui** acesso via Service Principals dedicados por cliente  
✅ **Permite** consumo self-service (Power BI, Tableau, Python, Excel, Synapse)  

---

## 🏗️ Arquitetura

```
┌─────────────────────────────────────────────────────────────────┐
│              BANCO DE DADOS PRODUÇÃO (PostgreSQL)                │
│                   Schema: <your_schema>                        │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────────┐   │
│  │ tickets  │  │  users   │  │  queues  │  │devices_data  │   │
│  │tenants │  │  items   │  │locations │  │   stages     │   │
│  └──────────┘  └──────────┘  └──────────┘  └──────────────┘   │
└─────────────────────────────────────────────────────────────────┘
                                 ↓
                    ┌────────────────────────┐
                    │   AIRFLOW (Docker)     │
                    │  DAG 1: Refresh Views  │
                    │  DAG 2: Export to ADLS │
                    │  Execução: 15 minutos  │
                    └────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────┐
│         VIEWS MATERIALIZADAS (Dados Denormalizados)              │
│  ┌─────────────────────┐                                        │
│  │ bi_tickets_flat     │                                        │
│  │ + tenant_id 🔒     │   (Views simplificadas - apenas tickets)│
│  └─────────────────────┘                                        │
└─────────────────────────────────────────────────────────────────┘
                                 ↓
┌─────────────────────────────────────────────────────────────────┐
│              AZURE DATA LAKE STORAGE GEN2                        │
│                  (Isolamento Físico)                             │
│                                                                   │
│  Container: tickets-data                                         │
│  ├── tenant_<uuid_1>/  🔒 (Service Principal 1)                │
│  │   ├── tickets/format=parquet/year=2026/month=01/day=14/*.parquet           │
│  │   ├── tickets/format=csv/year=2026/month=01/day=14/*.csv           │
│  │                                                               │
│  ├── tenant_<uuid_2>/  🔒 (Service Principal 2)                │
│  │   └── ...                                                     │
│  │                                                               │
│  └── tenant_<uuid_N>/  🔒 (Service Principal N)                │
│      └── ...                                                     │
└─────────────────────────────────────────────────────────────────┘
                                 ↓
        ┌────────────────────────────────────────────┐
        │    CLIENTES (Self-Service Analytics)       │
        │  ✓ Power BI Desktop                        │
        │  ✓ Tableau                                 │
        │  ✓ Python (Pandas, PySpark)                │
        │  ✓ Azure Synapse Analytics                 │
        │  ✓ Excel (Power Query)                     │
        └────────────────────────────────────────────┘
```

---

## 📚 Documentação

### 🚀 Guias de Setup

| Documento | Descrição | Quando Usar |
|-----------|-----------|-------------|
| **[QUICKSTART.md](QUICKSTART.md)** | ⚡ Setup rápido em 5 passos | Primeira vez configurando |
| **[AZURE_DATALAKE.md](AZURE_DATALAKE.md)** | 📘 Guia completo do Azure Data Lake | Configuração detalhada do Azure |
| **[sql/README.md](sql/README.md)** | 📜 Views materializadas e SQL | Entender estrutura dos dados |

### 🤖 Automação

| Documento | Descrição | Quando Usar |
|-----------|-----------|-------------|
| **[scripts/README.md](scripts/README.md)** | 🐍 Scripts de automação Python/Bash | Criar Service Principals automaticamente |

---

## 🔒 Segurança Multi-Tenant

✅ **Isolamento verdadeiro com ACLs POSIX**

- Cada tenant tem seu próprio Service Principal
- Service Principals têm APENAS ACLs (sem RBAC no Storage Account)
- Permissão `r-x` na pasta da tenant + `--x` no diretório raiz
- Configuração 100% automatizada via scripts Python

✅ **Cada tenant vê apenas seus dados**

- Pastas isoladas: `tenant_<uuid>/`
- Tentativas de acesso a outras pastas retornam erro 403

✅ **Processo automatizado**

```bash
# Criar Service Principal + configurar ACLs automaticamente
python scripts/generate_azure_service_principals.py --tenants "Nome da Tenant"
```

---

## 🚀 Quick Start

### 1. Clone e Configure

```bash
cd airflow-datalake-etl-azure-multitenant
cp .env.example .env
# Edite .env com suas credenciais PostgreSQL E Azure
```

### 2. Configure Azure Data Lake

Veja **[AZURE_DATALAKE.md](AZURE_DATALAKE.md)** para:

- Criar Storage Account no Azure
- Criar Service Principal para Airflow (exportação)
- Criar Service Principals por tenant (leitura)
- Configurar ACLs de isolamento

### 3. Inicie o Airflow

```bash
# Build com dependências Azure
docker compose build
docker compose up -d
```

### 4. Execute Scripts SQL (Views Materializadas)

**⚠️ IMPORTANTE:** Antes de executar, edite os scripts SQL e substitua `<your_schema>` pelo schema real do PostgreSQL.

```bash
psql -h <your_postgres_host> -U <your_postgres_user> -d <your_database> -f sql/01_create_bi_tickets_flat.sql
psql -h <your_postgres_host> -U <your_postgres_user> -d <your_database> -f sql/02_create_bi_refresh_log.sql
psql -h <your_postgres_host> -U <your_postgres_user> -d <your_database> -f sql/03_initial_refresh.sql
psql -h <your_postgres_host> -U <your_postgres_user> -d <your_database> -f sql/04_create_export_watermark.sql
psql -h <your_postgres_host> -U <your_postgres_user> -d <your_database> -f sql/05_extend_bi_refresh_log_azure.sql
psql -h <your_postgres_host> -U <your_postgres_user> -d <your_database> -f sql/06_create_tenant_export_settings.sql
```

### 5. Ativar DAGs no Airflow

1. Acesse <http://localhost:8080> (usuário: airflow / senha: airflow)
2. Admin → Connections → Add
   - Connection id: `<your_postgres_connection_id>`
   - Connection Type: `Postgres`
   - Host: `<your_postgres_host>`
   - Database: `<your_database>`
   - Login: `<your_postgres_user>`
   - Password: `<your_postgres_password>`
   - Port: `5432`

### 6. Ativar DAGs

Navegue até as DAGs e ative:

- ✅ `tickets_powerbi_etl` (refresh views materializadas - 15 min)
- ✅ `export_tickets_to_azure_datalake` (exportação incremental para Azure - 15 min)
- ✅ `azure_datalake_compaction` (consolidação diária de arquivos - 2:00 AM)

### 7. Gerar Service Principals para Clientes

Para executar os scripts Python, é recomendado criar um ambiente virtual para isolar as dependências.

```bash
# 1. Criar e ativar ambiente virtual (faça isso uma vez)
python3 -m venv .venv
source .venv/bin/activate

# 2. Instalar dependências
pip install psycopg2-binary python-dotenv

# 3. Executar o script de geração
python scripts/generate_azure_service_principals.py

# 4. Executar o script .sh gerado para criar os recursos no Azure
bash azure/create_service_principals_*.sh
```

### 8. Distribuir Credenciais aos Clientes

Envie para cada cliente (comunicação segura):

- Tenant ID, Client ID, Client Secret
- Documentação de acesso (pasta `azure_client_docs_*/`)
- Exemplos de consumo (Power BI, Python, Tableau)

---

## 🔒 Isolamento Multi-Tenant

### Arquitetura de Segurança

**Isolamento físico no Azure Data Lake:**

```
Container: tickets-data
├── tenant_abc123/  🔒 Service Principal A (read-only)
│   ├── tickets/
│
├── tenant_def456/  🔒 Service Principal B (read-only)
│   ├── tickets/
│
└── tenant_xyz789/  🔒 Service Principal C (read-only)
    ├── tickets/
```

### Garantias de Segurança

✅ **Isolamento Físico:** Cada tenant em pasta separada  
✅ **ACLs Azure:** Service Principal A não consegue listar pasta de B  
✅ **Auditoria:** Logs de acesso no Azure Monitor  
✅ **Princípio do Menor Privilégio:** Read-only para clientes, write para Airflow  

**📖 Detalhes:** [AZURE_DATALAKE.md - Seção Segurança](AZURE_DATALAKE.md#🔒-segurança-e-isolamento)

---

## 📊 Estrutura de Dados

### Views Materializadas (PostgreSQL)

| View | Descrição | Registros Típicos | Refresh |
|------|-----------|-------------------|---------|
| **bi_tickets_flat** | Tickets completos com SLA, queue, location (simplificada) | 10K - 1M | 15 min |

### Exportação Azure Data Lake (Parquet)

**Formato:** Parquet com compressão Snappy  
**Particionamento:** `tenant_<uuid>/table_name/format=parquet/year=YYYY/month=MM/day=DD/*.parquet`  
**Retenção:** Últimas 24h (incremental)  
**Atualização:** A cada 15 minutos  

---

## 🎯 Consumo dos Dados

### Opção 1: Power BI Desktop (Recomendado)

1. **Get Data** → **Azure Data Lake Storage Gen2**
2. URL: `https://<storage>.dfs.core.windows.net/tickets-data/tenant_<uuid>/`
3. Autenticação: Service Principal (fornecido por tenant)
4. Combinar arquivos Parquet por pasta

**Vantagens:**

- ✅ Sem custos de licença Power BI Service
- ✅ Cada cliente gerencia seus próprios relatórios
- ✅ Atualização automática via Gateway (opcional)

### Opção 2: Python (Pandas)

```python
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import pandas as pd

credential = ClientSecretCredential(tenant_id, client_id, client_secret)
service_client = DataLakeServiceClient(account_url, credential=credential)

# Ler Parquet
file_client = service_client.get_file_system_client("tickets-data") \
    .get_file_client("tenant_<uuid>/tickets/format=parquet/year=2026/month=01/day=14/data.parquet")

df = pd.read_parquet(io.BytesIO(file_client.download_file().readall()))
```

### Opção 3: Tableau

1. Conectar a **Azure Data Lake Storage Gen2**
2. Fornecer credenciais do Service Principal
3. Selecionar arquivos Parquet
4. Criar visualizações

### Opção 4: Azure Synapse Analytics

```sql
CREATE EXTERNAL TABLE tickets_external (...)
WITH (
    LOCATION = 'tenant_<uuid>/tickets/',
    DATA_SOURCE = TicketsDataLake,
    FILE_FORMAT = ParquetFormat
);
```

**📖 Exemplos completos:** [AZURE_DATALAKE.md - Seção Consumo](AZURE_DATALAKE.md#👥-consumo-pelos-clientes)

---

## 🎯 Casos de Uso

### 1. Dashboard Executivo (Power BI)

- KPIs principais por tenant
- Taxa de cumprimento de SLA
- Performance por fila/usuário
- Tendências temporais

### 2. Análise Ad-Hoc (Python/Jupyter)

- Análises estatísticas customizadas
- Machine Learning (previsão de demanda)
- Integração com outras fontes de dados

### 3. Relatórios Corporativos (Tableau)

- Dashboards interativos
- Exportação para PDF/PPT
- Agendamento de distribuição

### 4. Data Warehouse (Synapse)

- Integração com outros datasets
- Queries SQL complexas
- Data Science em escala

---

## 🛠️ Tecnologias

- **Apache Airflow 2.8.1** - Orquestração ETL
- **PostgreSQL 12+** - Banco de dados transacional
- **Azure Data Lake Gen2** - Armazenamento e distribuição
- **Parquet + Snappy** - Formato otimizado para analytics
- **Docker & Docker Compose** - Containerização
- **Python 3.11** - Scripts de automação
- **Azure SDK** - Integração com Azure

---

## 📈 Performance

### PostgreSQL (Views Materializadas)

**Antes (Queries diretas):**

- ⏱️ 15-30 segundos (8 JOINs)
- 🔥 Alto impacto no banco transacional

**Depois (Views materializadas):**

- ⏱️ 0.1-0.5 segundos (30-300x mais rápido!)
- 🔥 Zero impacto no banco transacional

### Azure Data Lake (Parquet)

**Leitura:**

- ⚡ Compressão Snappy: ~70% menos dados transferidos
- ⚡ Formato colunar: Lê apenas colunas necessárias
- ⚡ Particionamento: Queries incrementais muito rápidas

**Escrita:**

- 📊 Exportação de 100K tickets: ~5 segundos
- 📦 Tamanho típico: 10-20 MB comprimido por tenant/dia

---

## 🔍 Monitoramento

### Airflow UI

```
http://localhost:8080
- Executações das DAGs
- Logs detalhados de cada task
- Métricas de performance
```

### Azure Monitor

```kql
// Logs de acesso ao Data Lake
StorageBlobLogs
| where AccountName == "stticketsdatalake"
| where OperationName in ("GetBlob", "ListBlobs")
| summarize AccessCount = count() by CallerIpAddress, bin(TimeGenerated, 1h)
```

### Logs do Refresh

```sql
SELECT * FROM <your_schema>.bi_refresh_log
WHERE refresh_timestamp >= NOW() - INTERVAL '24 hours'
ORDER BY refresh_timestamp DESC;
```

---

## 🆘 Troubleshooting

### Comandos CLI de Diagnóstico

Para investigar problemas sem usar a interface web:

```bash
# Ver erros de importação de DAGs
docker compose exec airflow-webserver airflow dags list-import-errors

# Ver últimas 5 execuções (todas)
docker compose exec airflow-webserver airflow dags list-runs \
  -d tickets_powerbi_etl \
  --limit 5

# Ver últimas execuções falhadas
docker compose exec airflow-webserver airflow dags list-runs \
  -d tickets_powerbi_etl \
  --state failed \
  --limit 5

# Ver logs de uma task específica falhada
docker compose exec airflow-webserver airflow tasks logs \
  tickets_powerbi_etl \
  refresh_bi_tickets_flat \
  2026-01-19T10:00:00+00:00

# Verificar se conexão postgres_prod está configurada
docker compose exec airflow-webserver airflow connections get postgres_prod

# Ver logs do scheduler (problemas de agendamento)
docker logs airflow-scheduler --tail 100

# Limpar tasks falhadas e reexecutar
docker compose exec airflow-webserver airflow tasks clear \
  tickets_powerbi_etl \
  --only-failed \
  --start-date "2026-01-19" \
  --end-date "2026-01-19" \
  --yes
```

---

### Erro: "This request is not authorized" (Azure)

**Causa:** Service Principal sem permissões ou credenciais incorretas.

**Solução:**

```bash
# Verificar ACLs
az storage fs access show \
  --account-name <storage> \
  --file-system tickets-data \
  --path tenant_<uuid>
```

### Airflow não conecta ao PostgreSQL

#### 🔍 Diagnóstico Rápido

**Teste 1: Conectividade básica**

```bash
# Testar se resolve o host
docker compose exec airflow-webserver ping -c 3 host.docker.internal

# Se falhar, adicionar em docker-compose.yaml:
extra_hosts:
  - "host.docker.internal:host-gateway"
```

**Teste 2: Verificar porta PostgreSQL**

```bash
# Testar se a porta 5432 está acessível
docker compose exec airflow-webserver nc -zv host.docker.internal 5432

# Output esperado: "Connection to host.docker.internal 5432 port [tcp/postgresql] succeeded!"
```

#### 🔥 Problema: Firewall UFW Bloqueando Docker Bridge

**Sintomas:**

- ✅ `ping host.docker.internal` funciona
- ❌ Conexão na porta 5432 falha com "Connection refused" ou timeout
- ❌ DAGs falham com erro de conexão PostgreSQL

**Causa:** O firewall UFW do host está bloqueando tráfego da rede bridge Docker (`airflow-network`)

**Solução:**

```bash
# 1. Identificar o nome da interface bridge criada pelo Docker
docker network inspect airflow-network | grep "com.docker.network.bridge.name"
# Exemplo de output: "com.docker.network.bridge.name": "br-a1b2c3d4e5f6"

# 2. Permitir tráfego PostgreSQL na interface bridge (substituir pelo nome da sua bridge)
sudo ufw allow in on br-a1b2c3d4e5f6 to any port 5432 proto tcp
sudo ufw reload

# 3. Verificar se a regra foi criada
sudo ufw status | grep 5432
# Output esperado: "5432/tcp on br-a1b2c3d4e5f6   ALLOW IN    Anywhere"

# 4. Descobrir o IP/CIDR da rede Docker
docker network inspect airflow-network | grep -E '(Subnet|Gateway)'
# Exemplo: "Subnet": "172.18.0.0/16", "Gateway": "172.18.0.1"

# 5. Adicionar o IP da bridge ao pg_hba.conf do PostgreSQL
# Localização típica: /etc/postgresql/<version>/main/pg_hba.conf
# Adicionar linha (ajustar CIDR conforme saída do passo 4):
echo "host    all    all    172.18.0.0/16    md5" | sudo tee -a /etc/postgresql/15/main/pg_hba.conf

# 6. Recarregar configuração do PostgreSQL
sudo systemctl reload postgresql
# Ou para outras versões:
# sudo systemctl reload postgresql@15-main

# 7. Testar novamente a conexão
docker compose exec airflow-webserver nc -zv host.docker.internal 5432
```

**⚠️ Nota de Segurança:**

- A regra UFW permite acesso **apenas da interface bridge Docker**, não de toda a rede
- O pg_hba.conf usa autenticação `md5`, garantindo senha criptografada
- Ajuste o CIDR (`172.18.0.0/16`) conforme sua rede Docker específica

#### 🌐 Banco PostgreSQL Externo

```bash
# Testar conectividade com banco remoto
docker compose exec airflow-webserver ping -c 3 <external_db_host>

# Testar porta PostgreSQL
docker compose exec airflow-webserver nc -zv <external_db_host> 5432

# Verificar firewall do servidor remoto
# Certifique-se de que o IP público do host Airflow está permitido
```

### DAG de exportação falha

**Verificar credenciais Azure em `.env`:**

- AZURE_STORAGE_ACCOUNT_NAME
- AZURE_TENANT_ID
- AZURE_CLIENT_ID
- AZURE_CLIENT_SECRET

**Rebuild Docker:**

```bash
docker compose down
docker compose build --no-cache
docker compose up -d
```

### Performance lenta no Parquet

**Causa:** Muitos arquivos pequenos.

**Solução:** Implementar DAG de compactação semanal para consolidar arquivos.

---

## 📞 Suporte

**Documentação Principal:**

- **[AZURE_DATALAKE.md](AZURE_DATALAKE.md)** - Guia completo Azure
- **[QUICKSTART.md](QUICKSTART.md)** - Setup rápido

**Problemas Comuns:**

- Verificar logs: `docker logs airflow-scheduler`
- Testar Azure CLI: `az storage account show --name <storage>`
- Validar Service Principals: Scripts em `azure/`

---

## ⚠️ Avisos Importantes

### Segurança

🔒 **Isolamento Físico:** Cada tenant em pasta separada no Azure  
🔒 **Credenciais:** NUNCA commitar CLIENT_SECRET no Git  
🔒 **Rotação:** Renovar Service Principals anualmente  
🔒 **Auditoria:** Monitorar acessos no Azure Monitor  
🔒 **Princípio do Menor Privilégio:** Read-only para clientes  

### Performance

⚡ Parquet otimizado para leitura colunar  
⚡ Particionamento por data reduz scan de dados  
⚡ Compactação semanal recomendada para consolidar arquivos  
⚡ Considerar Hot → Cool → Archive Tier após 30/90/365 dias  
