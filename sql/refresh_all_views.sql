-- =============================================================================
-- SCRIPT DE REFRESH MANUAL: Atualizar todas as views materializadas
-- Use este script para refresh manual durante testes ou manutenção
-- 
-- IMPORTANTE: Este script assume que você está usando o schema 'public'.
-- Para usar em outro schema, substitua 'public' pelo schema desejado.
-- =============================================================================

-- Configurar search_path para usar o schema correto
-- SET search_path TO <SCHEMA_NAME>;

-- Registrar início
DO $$
DECLARE
    start_time TIMESTAMP;
BEGIN
    start_time := clock_timestamp();
    
    -- Refresh view principal de tickets
    RAISE NOTICE 'Refreshing bi_tickets_flat...';
    REFRESH MATERIALIZED VIEW CONCURRENTLY bi_tickets_flat;
    
    INSERT INTO bi_refresh_log (view_name, status, rows_affected, duration_seconds)
    VALUES (
        'bi_tickets_flat', 
        'SUCCESS', 
        (SELECT COUNT(*) FROM bi_tickets_flat),
        EXTRACT(EPOCH FROM (clock_timestamp() - start_time))
    );
    
    -- Atualizar estatísticas
    RAISE NOTICE 'Analyzing tables...';
    ANALYZE bi_tickets_flat;
    
    RAISE NOTICE 'All views refreshed successfully!';
END $$;

-- Mostrar resultado
SELECT 
    view_name,
    status,
    rows_affected,
    duration_seconds,
    refresh_timestamp
FROM bi_refresh_log
WHERE refresh_timestamp >= NOW() - INTERVAL '5 minutes'
ORDER BY refresh_timestamp DESC;
