-- =============================================================================
-- SCRIPT DE VERIFICAÇÃO: Status das Views Materializadas
-- Execute este script para verificar o estado atual das views
-- 
-- IMPORTANTE: Este script assume que você está usando o schema 'public'.
-- Para usar em outro schema, substitua 'public' pelo schema desejado.
-- =============================================================================

-- Configurar search_path para usar o schema correto
-- SET search_path TO <SCHEMA_NAME>;

-- Verificar se as views existem e quando foram atualizadas pela última vez
SELECT 
    schemaname,
    matviewname as view_name,
    hasindexes as has_indexes,
    ispopulated as is_populated,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as total_size
FROM pg_matviews 
WHERE schemaname = 'public'
  AND matviewname = 'bi_tickets_flat'
ORDER BY matviewname;

-- Contar registros em cada view
SELECT 'bi_tickets_flat' as view_name, COUNT(*) as total_rows 
FROM bi_tickets_flat;

-- Últimas execuções do refresh
SELECT 
    view_name,
    refresh_timestamp,
    status,
    rows_affected,
    duration_seconds,
    airflow_dag_id
FROM bi_refresh_log
WHERE refresh_timestamp >= NOW() - INTERVAL '24 hours'
ORDER BY refresh_timestamp DESC
LIMIT 20;

-- Estatísticas de sucesso/falha nas últimas 24h
SELECT 
    view_name,
    status,
    COUNT(*) as executions,
    AVG(duration_seconds) as avg_duration_sec,
    MAX(duration_seconds) as max_duration_sec
FROM bi_refresh_log
WHERE refresh_timestamp >= NOW() - INTERVAL '24 hours'
GROUP BY view_name, status
ORDER BY view_name, status;
