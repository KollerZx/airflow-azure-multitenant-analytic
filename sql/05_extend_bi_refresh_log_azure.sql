-- ==============================================================================
-- Extensão da Tabela bi_refresh_log para Exportações Azure Data Lake
-- ==============================================================================
-- 
-- ⚠️ IMPORTANTE: Este script deve ser executado no BANCO DE PRODUÇÃO
--    (onde estão as views bi_tickets_flat, tenants, tickets),
--    NÃO no banco interno do Airflow!
--
-- Como executar:
--   Opção 1 (Recomendado): ./run_sql_on_prod.sh
--   Opção 2: psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/05_extend_bi_refresh_log_azure.sql
--
-- Adiciona colunas para rastrear exportações para o Azure Data Lake,
-- permitindo monitoramento unificado de ETL + Exportações.
--

-- ==============================================================================

-- Configurar search_path (ajustar se necessário)
-- SET search_path TO public;

-- Adicionar colunas para exportações Azure (se não existirem)
ALTER TABLE bi_refresh_log 
    ADD COLUMN IF NOT EXISTS tenant_id TEXT,
    ADD COLUMN IF NOT EXISTS export_type VARCHAR(50),  -- 'FULL', 'INCREMENTAL', 'COMPACTION'
    ADD COLUMN IF NOT EXISTS files_created INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS files_deleted INTEGER DEFAULT 0,
    ADD COLUMN IF NOT EXISTS duplicates_removed BIGINT DEFAULT 0,
    ADD COLUMN IF NOT EXISTS export_destination VARCHAR(255),  -- 'AZURE_DATALAKE', 'LOCAL', etc.
    ADD COLUMN IF NOT EXISTS azure_storage_account VARCHAR(255),
    ADD COLUMN IF NOT EXISTS azure_container VARCHAR(255),
    ADD COLUMN IF NOT EXISTS azure_file_path TEXT;

-- Criar índices para queries comuns
CREATE INDEX IF NOT EXISTS idx_bi_refresh_log_tenant 
    ON bi_refresh_log(tenant_id) WHERE tenant_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bi_refresh_log_export_type 
    ON bi_refresh_log(export_type) WHERE export_type IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_bi_refresh_log_destination 
    ON bi_refresh_log(export_destination) WHERE export_destination IS NOT NULL;

-- Comentários para documentação
COMMENT ON COLUMN bi_refresh_log.tenant_id IS 'ID do tenant para exportações específicas';
COMMENT ON COLUMN bi_refresh_log.export_type IS 'Tipo de exportação: FULL, INCREMENTAL, COMPACTION';
COMMENT ON COLUMN bi_refresh_log.files_created IS 'Quantidade de arquivos criados na exportação';
COMMENT ON COLUMN bi_refresh_log.files_deleted IS 'Quantidade de arquivos removidos (compactação)';
COMMENT ON COLUMN bi_refresh_log.duplicates_removed IS 'Quantidade de registros duplicados removidos';
COMMENT ON COLUMN bi_refresh_log.export_destination IS 'Destino da exportação: AZURE_DATALAKE, LOCAL, etc.';
COMMENT ON COLUMN bi_refresh_log.azure_storage_account IS 'Nome da Storage Account Azure (se aplicável)';
COMMENT ON COLUMN bi_refresh_log.azure_container IS 'Nome do Container Azure (se aplicável)';
COMMENT ON COLUMN bi_refresh_log.azure_file_path IS 'Caminho completo do arquivo no Azure';

-- ==============================================================================
-- Views para Monitoramento
-- ==============================================================================

-- View 1: Estatísticas de exportações por tenant
CREATE OR REPLACE VIEW vw_export_stats_by_tenant AS
SELECT 
    tenant_id,
    c.name as tenant_name,
    COUNT(*) as total_exports,
    SUM(rows_affected) as total_rows_exported,
    SUM(files_created) as total_files_created,
    MAX(refresh_timestamp) as last_export_date,
    AVG(duration_seconds) as avg_duration_seconds,
    COUNT(*) FILTER (WHERE status = 'SUCCESS') as successful_exports,
    COUNT(*) FILTER (WHERE status = 'ERROR') as failed_exports
FROM bi_refresh_log
LEFT JOIN tenants c ON bi_refresh_log.tenant_id = c.id
WHERE export_destination = 'AZURE_DATALAKE'
  AND tenant_id IS NOT NULL
GROUP BY tenant_id, c.name
ORDER BY total_rows_exported DESC;

COMMENT ON VIEW vw_export_stats_by_tenant IS 'Estatísticas de exportações Azure por tenant';

-- View 2: Histórico de compactações
CREATE OR REPLACE VIEW vw_compaction_history AS
SELECT 
    refresh_timestamp,
    tenant_id,
    c.name as tenant_name,
    rows_affected,
    files_created,
    files_deleted,
    duplicates_removed,
    duration_seconds,
    status,
    error_message
FROM bi_refresh_log
LEFT JOIN tenants c ON bi_refresh_log.tenant_id = c.id
WHERE export_type = 'COMPACTION'
ORDER BY refresh_timestamp DESC;

COMMENT ON VIEW vw_compaction_history IS 'Histórico de compactações de arquivos Azure Data Lake';

-- View 3: Últimas exportações (últimas 24h)
CREATE OR REPLACE VIEW vw_recent_exports AS
SELECT 
    refresh_timestamp,
    tenant_id,
    c.name as tenant_name,
    view_name,
    export_type,
    rows_affected,
    files_created,
    duration_seconds,
    status,
    azure_file_path
FROM bi_refresh_log
LEFT JOIN tenants c ON bi_refresh_log.tenant_id = c.id
WHERE refresh_timestamp >= NOW() - INTERVAL '24 hours'
  AND export_destination = 'AZURE_DATALAKE'
ORDER BY refresh_timestamp DESC;

COMMENT ON VIEW vw_recent_exports IS 'Exportações Azure das últimas 24 horas';

-- View 4: Alertas de falhas
CREATE OR REPLACE VIEW vw_export_failures AS
SELECT 
    refresh_timestamp,
    tenant_id,
    c.name as tenant_name,
    view_name,
    export_type,
    error_message,
    airflow_dag_id,
    airflow_task_id
FROM bi_refresh_log
LEFT JOIN tenants c ON bi_refresh_log.tenant_id = c.id
WHERE status = 'ERROR'
  AND export_destination = 'AZURE_DATALAKE'
  AND refresh_timestamp >= NOW() - INTERVAL '7 days'
ORDER BY refresh_timestamp DESC;

COMMENT ON VIEW vw_export_failures IS 'Falhas de exportação Azure dos últimos 7 dias';

-- ==============================================================================
-- Função Helper: Registrar Exportação Azure
-- ==============================================================================

CREATE OR REPLACE FUNCTION log_azure_export(
    p_tenant_id TEXT,
    p_export_type VARCHAR(50),
    p_rows_affected BIGINT,
    p_files_created INTEGER DEFAULT 1,
    p_duration_seconds NUMERIC DEFAULT NULL,
    p_azure_file_path TEXT DEFAULT NULL,
    p_airflow_dag_id VARCHAR(255) DEFAULT NULL,
    p_notes TEXT DEFAULT NULL
)
RETURNS BIGINT AS $$
DECLARE
    v_log_id BIGINT;
    v_storage_account VARCHAR(255);
    v_container VARCHAR(255);
BEGIN
    -- Obter configurações do ambiente
    v_storage_account := current_setting('app.azure_storage_account', true);
    v_container := current_setting('app.azure_container', true);
    
    -- Se não estiver configurado, usar valores padrão
    IF v_storage_account IS NULL THEN
        v_storage_account := 'stticketsdatalake';
    END IF;
    
    IF v_container IS NULL THEN
        v_container := 'tickets-data';
    END IF;
    
    -- Inserir log
    INSERT INTO bi_refresh_log (
        view_name,
        status,
        rows_affected,
        duration_seconds,
        tenant_id,
        export_type,
        files_created,
        export_destination,
        azure_storage_account,
        azure_container,
        azure_file_path,
        airflow_dag_id,
        notes
    ) VALUES (
        'export_tickets_to_datalake',
        'SUCCESS',
        p_rows_affected,
        p_duration_seconds,
        p_tenant_id,
        p_export_type,
        p_files_created,
        'AZURE_DATALAKE',
        v_storage_account,
        v_container,
        p_azure_file_path,
        p_airflow_dag_id,
        p_notes
    )
    RETURNING id INTO v_log_id;
    
    RETURN v_log_id;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION log_azure_export IS 'Helper para registrar exportações Azure Data Lake';

-- ==============================================================================
-- Queries Úteis para Monitoramento
-- ==============================================================================

-- Ver estatísticas por tenant
-- SELECT * FROM vw_export_stats_by_tenant;

-- Ver exportações recentes (últimas 24h)
-- SELECT * FROM vw_recent_exports;

-- Ver histórico de compactações
-- SELECT * FROM vw_compaction_history;

-- Ver falhas recentes
-- SELECT * FROM vw_export_failures;

-- Resumo geral de exportações por dia
-- SELECT 
--     DATE(refresh_timestamp) as export_date,
--     COUNT(*) as total_exports,
--     SUM(rows_affected) as total_rows,
--     SUM(files_created) as total_files,
--     COUNT(DISTINCT tenant_id) as tenants_exported
-- FROM bi_refresh_log
-- WHERE export_destination = 'AZURE_DATALAKE'
--   AND refresh_timestamp >= NOW() - INTERVAL '30 days'
-- GROUP BY DATE(refresh_timestamp)
-- ORDER BY export_date DESC;

COMMIT;
