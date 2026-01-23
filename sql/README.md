# Scripts SQL - Pipeline de Dados

Este diretório contém scripts SQL para configurar e manter o pipeline de dados.

## ⚠️ IMPORTANTE: Onde Executar os Scripts

**TODOS os scripts SQL devem ser executados no BANCO DE PRODUÇÃO**, onde estão suas views e dados de negócio (tickets, companies, etc.), **NÃO no banco interno do Airflow**.

### Arquitetura de Bancos

```
┌─────────────────────────────────────────────┐
│  Banco Interno Airflow (postgres-airflow)   │
│  - Porta: 5433                               │
│  - Database: airflow                         │
│  - Uso: Metadados Airflow                   │
│  - ❌ NÃO executar scripts SQL aqui          │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│  Banco de Produção (postgres_prod)          │
│  - Config: .env (POSTGRES_HOST, ...)        │
│  - Database: seu_database                   │
│  - Schema: taskmanager (ou public)          │
│  - Uso: Dados de negócio + Views BI         │
│  - ✅ Executar scripts SQL aqui              │
└─────────────────────────────────────────────┘
```

## 🚀 Como Executar

### Método 1: Script Helper (Recomendado)

```bash
# Executa scripts de exportação incremental automaticamente
./run_sql_on_prod.sh

# Ou executar script específico
./run_sql_on_prod.sh sql/04_create_export_watermark.sql
```

### Método 2: Manualmente

```bash
# Carregar credenciais do .env
source .env

# Executar script
PGPASSWORD=$POSTGRES_PASSWORD psql \
  -h $POSTGRES_HOST \
  -p $POSTGRES_PORT \
  -U $POSTGRES_USER \
  -d $POSTGRES_DB \
  -f sql/01_create_bi_tickets_flat.sql
```

## 📋 Scripts Disponíveis

### Scripts de Criação (Views BI)

| Arquivo | Descrição |
|---------|-----------|
| `01_create_bi_tickets_flat.sql` | View materializada principal de tickets |
| `02_create_bi_refresh_log.sql` | Tabela de log de refresh das views |
| `03_initial_refresh.sql` | Refresh inicial das views |
| `04_create_export_watermark.sql` | Tabela de watermark para exportação incremental |
| `05_extend_bi_refresh_log_azure.sql` | Extensão do log de refresh para exportações Azure |
| `06_create_tenant_export_settings.sql` | Configurações de exportação por empresa |

### Scripts de Manutenção

| Arquivo | Descrição | Uso |
|---------|-----------|-----|
| `check_views_status.sql` | Verifica status das views materializadas | 🔍 Monitoramento |
| `performance_stats.sql` | Estatísticas de performance do ETL | 🔍 Análise |
| `refresh_all_views.sql` | Refresh manual de todas as views | 🔧 Manutenção |

## 🔍 Verificar se Scripts Foram Executados

```bash
# Verificar tabelas criadas
source .env
PGPASSWORD=$POSTGRES_PASSWORD psql \
  -h $POSTGRES_HOST \
  -p $POSTGRES_PORT \
  -U $POSTGRES_USER \
  -d $POSTGRES_DB \
  -c "SELECT schemaname, tablename FROM pg_tables WHERE schemaname = '$POSTGRES_SCHEMA' ORDER BY tablename;"

# Verificar views materializadas
PGPASSWORD=$POSTGRES_PASSWORD psql \
  -h $POSTGRES_HOST \
  -p $POSTGRES_PORT \
  -U $POSTGRES_USER \
  -d $POSTGRES_DB \
  -c "SELECT schemaname, matviewname FROM pg_matviews WHERE schemaname = '$POSTGRES_SCHEMA' ORDER BY matviewname;"
```
