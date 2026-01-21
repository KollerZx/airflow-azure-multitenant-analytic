-- =============================================================================
-- SCRIPT DE INICIALIZAÇÃO: Primeiro refresh das views materializadas
-- Execute este script após criar todas as views pela primeira vez
-- 
-- IMPORTANTE: Este script assume que você está usando o schema 'public'.
-- Para usar em outro schema, substitua 'public' pelo schema desejado.
-- =============================================================================

-- Configurar search_path para usar o schema correto
-- SET search_path TO <SCHEMA_NAME>;

-- Refresh da view principal de tickets
REFRESH MATERIALIZED VIEW bi_tickets_flat;

-- Verificar contagens
SELECT 
    'bi_tickets_flat' as view_name,
    COUNT(*) as total_rows
FROM bi_tickets_flat;

-- Registrar no log
INSERT INTO bi_refresh_log (view_name, status, rows_affected)
VALUES 
    ('bi_tickets_flat', 'SUCCESS', (SELECT COUNT(*) FROM bi_tickets_flat));
