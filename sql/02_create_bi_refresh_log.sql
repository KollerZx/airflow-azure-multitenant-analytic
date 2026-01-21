-- =============================================================================
-- TABELA DE LOG: bi_refresh_log
-- Registra execuções do ETL para auditoria e monitoramento
-- 
-- IMPORTANTE: Este script assume que você está usando o schema 'public'.
-- Para usar em outro schema, substitua 'public' pelo schema desejado.
-- =============================================================================

-- Configurar search_path para usar o schema correto
-- SET search_path TO <SCHEMA_NAME>;

-- Drop se existir
DROP TABLE IF EXISTS bi_refresh_log CASCADE;

-- Criar tabela de log
CREATE TABLE bi_refresh_log (
    id SERIAL PRIMARY KEY,
    refresh_timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    view_name VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    rows_affected BIGINT,
    duration_seconds NUMERIC(10, 2),
    error_message TEXT,
    airflow_dag_id VARCHAR(255),
    airflow_task_id VARCHAR(255),
    airflow_execution_date TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- =============================================================================
-- ÍNDICES
-- =============================================================================

CREATE INDEX idx_bi_refresh_log_timestamp ON bi_refresh_log(refresh_timestamp DESC);
CREATE INDEX idx_bi_refresh_log_status ON bi_refresh_log(status);
CREATE INDEX idx_bi_refresh_log_view_name ON bi_refresh_log(view_name);

-- =============================================================================
-- COMENTÁRIOS PARA DOCUMENTAÇÃO
-- =============================================================================

COMMENT ON TABLE bi_refresh_log IS 'Tabela de auditoria que registra todas as execuções do pipeline ETL do Airflow (refresh das views materializadas e exportação para Azure Data Lake)';
COMMENT ON COLUMN bi_refresh_log.status IS 'Status da execução: SUCCESS, FAILED, RUNNING, PARTIAL';
COMMENT ON COLUMN bi_refresh_log.rows_affected IS 'Número de registros processados na operação';
COMMENT ON COLUMN bi_refresh_log.duration_seconds IS 'Tempo de execução da operação em segundos';
