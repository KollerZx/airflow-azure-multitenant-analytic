-- =============================================================================
-- VIEW MATERIALIZADA PRINCIPAL: bi_tickets_flat
-- Denormaliza dados de tickets com todos os relacionamentos.
-- Dados são exportados para Azure Data Lake via Airflow para consumo no Power BI.
-- 
-- IMPORTANTE: Este script assume que você está usando o schema 'public'.
-- Para usar em outro schema, substitua 'public' pelo schema desejado.
-- =============================================================================

-- Configurar search_path para usar o schema correto
-- SET search_path TO <SCHEMA_NAME>, <SCHEMA_NAME>;

-- Drop se existir
DROP MATERIALIZED VIEW IF EXISTS bi_tickets_flat CASCADE;

-- Criar view materializada
CREATE MATERIALIZED VIEW bi_tickets_flat AS
SELECT 
    -- ===== TICKET CORE =====
    t.id,
    t.tenant_id,  -- Necessário para isolamento multi-tenant
    t.title,
    t.coverage_time,
    t.assignee_name,
    t.created_at,
    t.finish_time,
    t.status as ticket_status,
    
    -- ===== TEMPOS CALCULADOS =====
    -- Duração do ticket em minutos
    CASE 
        WHEN t.finish_time IS NOT NULL THEN 
            EXTRACT(EPOCH FROM (t.finish_time - t.created_at))/60
        ELSE 
            EXTRACT(EPOCH FROM (NOW() - t.created_at))/60
    END as duration_minutes,
    
    -- Deadline do SLA
    t.created_at + INTERVAL '1 minute' * COALESCE(t.coverage_time, 0) as sla_deadline,
    
    -- Status do SLA
    CASE 
        WHEN t.finish_time IS NOT NULL AND t.finish_time <= (t.created_at + INTERVAL '1 minute' * COALESCE(t.coverage_time, 0))
            THEN 'Within SLA'
        WHEN t.finish_time IS NOT NULL AND t.finish_time > (t.created_at + INTERVAL '1 minute' * COALESCE(t.coverage_time, 0))
            THEN 'SLA Breached'
        WHEN NOW() > (t.created_at + INTERVAL '1 minute' * COALESCE(t.coverage_time, 0))
            THEN 'SLA At Risk'
        ELSE 'On Track'
    END as sla_status,
    
    -- Minutos de breach (negativo = dentro do SLA)
    CASE 
        WHEN t.finish_time IS NOT NULL THEN
            EXTRACT(EPOCH FROM (t.finish_time - (t.created_at + INTERVAL '1 minute' * COALESCE(t.coverage_time, 0))))/60
        ELSE
            EXTRACT(EPOCH FROM (NOW() - (t.created_at + INTERVAL '1 minute' * COALESCE(t.coverage_time, 0))))/60
    END as sla_breach_minutes,
    
    -- ===== REQUESTER (USER) =====
    u.name as requester_name,
    
    -- ===== DIMENSÕES DE TEMPO (para facilitar filtros no Power BI) =====
    EXTRACT(YEAR FROM t.created_at) as created_year,
    EXTRACT(MONTH FROM t.created_at) as created_month,
    EXTRACT(DAY FROM t.created_at) as created_day,
    EXTRACT(DOW FROM t.created_at) as created_day_of_week, -- 0=Sunday, 6=Saturday
    EXTRACT(HOUR FROM t.created_at) as created_hour,
    TO_CHAR(t.created_at, 'YYYY-MM') as created_year_month,
    TO_CHAR(t.created_at, 'YYYY-MM-DD') as created_date,
    TO_CHAR(t.created_at, 'Day') as created_day_name,
    TO_CHAR(t.created_at, 'Month') as created_month_name,
    
    -- ===== DIMENSÕES DE TEMPO PARA FINISH =====
    EXTRACT(YEAR FROM t.finish_time) as finish_year,
    EXTRACT(MONTH FROM t.finish_time) as finish_month,
    TO_CHAR(t.finish_time, 'YYYY-MM-DD') as finish_date

FROM tickets t
LEFT JOIN users u ON t.requester_id = u.id;

-- =============================================================================
-- ÍNDICES PARA PERFORMANCE
-- =============================================================================

-- Índice único para refresh CONCURRENT
CREATE UNIQUE INDEX idx_bi_tickets_flat_id ON bi_tickets_flat(id);

-- Índices para filtros comuns
CREATE INDEX idx_bi_tickets_flat_tenant_date ON bi_tickets_flat(tenant_id, created_at DESC);
-- Índices específicos de queue/location removidos para modelo genérico
CREATE INDEX idx_bi_tickets_flat_status ON bi_tickets_flat(ticket_status);
CREATE INDEX idx_bi_tickets_flat_sla_status ON bi_tickets_flat(sla_status);
CREATE INDEX idx_bi_tickets_flat_created_at ON bi_tickets_flat(created_at DESC);

-- Índice BRIN para queries por período (muito eficiente para dados ordenados por tempo)
CREATE INDEX idx_bi_tickets_flat_created_at_brin ON bi_tickets_flat USING BRIN(created_at);

-- =============================================================================
-- COMENTÁRIOS PARA DOCUMENTAÇÃO
-- =============================================================================

COMMENT ON MATERIALIZED VIEW bi_tickets_flat IS 'View materializada com dados denormalizados de tickets (simplificada - ~60% menos campos). Exportada para Azure Data Lake a cada 15 minutos pelo Airflow para consumo no Power BI com isolamento multi-tenant.';
COMMENT ON COLUMN bi_tickets_flat.sla_status IS 'Status do SLA: Within SLA (finalizado no prazo), SLA Breached (finalizado fora do prazo), SLA At Risk (ainda aberto e fora do prazo), On Track (ainda aberto e dentro do prazo)';
COMMENT ON COLUMN bi_tickets_flat.sla_breach_minutes IS 'Minutos de violação do SLA (negativo = dentro do prazo, positivo = fora do prazo)';
