-- =============================================================================
-- MÉTRICAS DE PERFORMANCE: Análise do Pipeline ETL
-- Consultas para monitorar performance e identificar problemas
-- 
-- IMPORTANTE: Este script assume que você está usando o schema 'public'.
-- Para usar em outro schema, substitua 'public' pelo schema desejado.
-- =============================================================================

-- Configurar search_path para usar o schema correto
-- SET search_path TO <SCHEMA_NAME>;

-- 1. TEMPO MÉDIO DE REFRESH POR VIEW (últimos 7 dias)
SELECT 
    view_name,
    COUNT(*) as total_executions,
    AVG(duration_seconds) as avg_duration_sec,
    MIN(duration_seconds) as min_duration_sec,
    MAX(duration_seconds) as max_duration_sec,
    STDDEV(duration_seconds) as stddev_duration_sec,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed
FROM bi_refresh_log
WHERE refresh_timestamp >= NOW() - INTERVAL '7 days'
    AND view_name != 'ALL_VIEWS'
GROUP BY view_name
ORDER BY avg_duration_sec DESC;

-- 2. TENDÊNCIA DE CRESCIMENTO (volume de dados ao longo do tempo)
SELECT 
    DATE(refresh_timestamp) as date,
    view_name,
    AVG(rows_affected) as avg_rows,
    MAX(rows_affected) as max_rows
FROM bi_refresh_log
WHERE refresh_timestamp >= NOW() - INTERVAL '30 days'
    AND view_name != 'ALL_VIEWS'
    AND status = 'SUCCESS'
GROUP BY DATE(refresh_timestamp), view_name
ORDER BY date DESC, view_name;

-- 3. EXECUÇÕES FALHADAS (últimos 7 dias)
SELECT 
    refresh_timestamp,
    view_name,
    error_message,
    airflow_dag_id,
    airflow_task_id
FROM bi_refresh_log
WHERE status = 'FAILED'
    AND refresh_timestamp >= NOW() - INTERVAL '7 days'
ORDER BY refresh_timestamp DESC;

-- 4. PERFORMANCE POR HORA DO DIA (identificar horários de pico)
SELECT 
    EXTRACT(HOUR FROM refresh_timestamp) as hour_of_day,
    view_name,
    COUNT(*) as executions,
    AVG(duration_seconds) as avg_duration_sec
FROM bi_refresh_log
WHERE refresh_timestamp >= NOW() - INTERVAL '7 days'
    AND view_name != 'ALL_VIEWS'
    AND status = 'SUCCESS'
GROUP BY EXTRACT(HOUR FROM refresh_timestamp), view_name
ORDER BY hour_of_day, view_name;

-- 5. TAMANHO DAS VIEWS E ÍNDICES
SELECT 
    schemaname,
    matviewname as view_name,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname)) as total_size,
    pg_size_pretty(pg_relation_size(schemaname||'.'||matviewname)) as table_size,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||matviewname) - pg_relation_size(schemaname||'.'||matviewname)) as indexes_size
FROM pg_matviews 
WHERE schemaname = 'public'
  AND matviewname = 'bi_tickets_flat'
ORDER BY pg_total_relation_size(schemaname||'.'||matviewname) DESC;

-- 6. ÍNDICES E SUA UTILIZAÇÃO
SELECT 
    schemaname,
    tablename,
    indexname,
    idx_scan as index_scans,
    idx_tup_read as tuples_read,
    idx_tup_fetch as tuples_fetched,
    pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
WHERE schemaname = 'public'
  AND tablename = 'bi_tickets_flat'
ORDER BY tablename, idx_scan DESC;

-- 7. TAXA DE SUCESSO DO ETL (últimos 30 dias)
WITH daily_stats AS (
    SELECT 
        DATE(refresh_timestamp) as date,
        COUNT(*) as total_runs,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_runs
    FROM bi_refresh_log
    WHERE refresh_timestamp >= NOW() - INTERVAL '30 days'
        AND view_name = 'ALL_VIEWS'
    GROUP BY DATE(refresh_timestamp)
)
SELECT 
    date,
    total_runs,
    successful_runs,
    ROUND(100.0 * successful_runs / NULLIF(total_runs, 0), 2) as success_rate_percent
FROM daily_stats
ORDER BY date DESC;

-- 8. DISTRIBUIÇÃO DE TICKETS POR STATUS (dados atuais na view)
SELECT 
    ticket_status,
    COUNT(*) as total_tickets,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage
FROM bi_tickets_flat
GROUP BY ticket_status
ORDER BY total_tickets DESC;

-- 9. DISTRIBUIÇÃO DE TICKETS POR SLA STATUS
SELECT 
    sla_status,
    COUNT(*) as total_tickets,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER(), 2) as percentage,
    ROUND(AVG(duration_minutes), 2) as avg_duration_minutes
FROM bi_tickets_flat
WHERE ticket_status = 'Active'
GROUP BY sla_status
ORDER BY total_tickets DESC;

-- 10. TOP 10 FILAS COM MAIS TICKETS ATIVOS
SELECT 
    requester_name,
    COUNT(*) as active_tickets,
    ROUND(AVG(duration_minutes), 2) as avg_duration_minutes,
    SUM(CASE WHEN sla_status IN ('SLA Breached', 'SLA At Risk') THEN 1 ELSE 0 END) as sla_violations
FROM bi_tickets_flat
WHERE ticket_status = 'Active'
GROUP BY requester_name
ORDER BY active_tickets DESC
LIMIT 10;
