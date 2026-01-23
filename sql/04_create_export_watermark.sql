-- ==============================================================================
-- Tabela de Controle de Exportação Incremental para Azure Data Lake
-- ==============================================================================
-- 
-- ⚠️ IMPORTANTE: Este script deve ser executado no BANCO DE PRODUÇÃO
--    (onde estão as views bi_tickets_flat, tenants, tickets),
--    NÃO no banco interno do Airflow!
--
-- Como executar:
--   Opção 1 (Recomendado): ./run_sql_on_prod.sh
--   Opção 2: psql -h $POSTGRES_HOST -U $POSTGRES_USER -d $POSTGRES_DB -f sql/04_create_export_watermark.sql
--
-- Esta tabela rastreia a última exportação bem-sucedida de dados para o Azure
-- Data Lake por tenant (tenant_id). Permite exportação incremental eficiente,
-- evitando duplicação de dados.
--

-- ==============================================================================

-- Configurar search_path (ajustar se necessário)
-- SET search_path TO public;

-- Criar tabela de controle de watermark
CREATE TABLE IF NOT EXISTS export_watermark (
    tenant_id TEXT NOT NULL,
    last_exported_created_at TIMESTAMPTZ NOT NULL,
    last_export_success_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    rows_exported BIGINT DEFAULT 0,
    export_type VARCHAR(50) DEFAULT 'INCREMENTAL', -- 'FULL' ou 'INCREMENTAL'
    notes TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    
    CONSTRAINT pk_export_watermark PRIMARY KEY (tenant_id),
    CONSTRAINT fk_export_watermark_tenant FOREIGN KEY (tenant_id) 
        REFERENCES tenants(id) ON DELETE CASCADE
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_export_watermark_last_export 
    ON export_watermark(last_export_success_at DESC);

CREATE INDEX IF NOT EXISTS idx_export_watermark_created_at 
    ON export_watermark(last_exported_created_at DESC);

-- Trigger para atualizar updated_at automaticamente
CREATE OR REPLACE FUNCTION update_export_watermark_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_update_export_watermark_timestamp
BEFORE UPDATE ON export_watermark
FOR EACH ROW
EXECUTE FUNCTION update_export_watermark_timestamp();

-- Comentários para documentação
COMMENT ON TABLE export_watermark IS 'Controla última exportação de dados para Azure Data Lake por tenant';
COMMENT ON COLUMN export_watermark.tenant_id IS 'ID do tenant (UUID)';
COMMENT ON COLUMN export_watermark.last_exported_created_at IS 'Último created_at exportado - usado como watermark para próxima exportação';
COMMENT ON COLUMN export_watermark.last_export_success_at IS 'Timestamp da última exportação bem-sucedida';
COMMENT ON COLUMN export_watermark.rows_exported IS 'Quantidade de registros exportados na última execução';
COMMENT ON COLUMN export_watermark.export_type IS 'Tipo da última exportação: FULL (completa) ou INCREMENTAL';
COMMENT ON COLUMN export_watermark.notes IS 'Notas sobre a exportação (ex: erros recuperados, backfill)';

-- ==============================================================================
-- Inicialização: Popular watermark para tenants existentes
-- ==============================================================================
-- 
-- OPÇÃO A: Usar MAX(created_at) atual por tenant (evita re-exportação)
-- OPÇÃO B: Usar data antiga para forçar re-exportação completa
--
-- Executar APENAS UMA das opções abaixo:
-- ==============================================================================

-- OPÇÃO A: Usar dados atuais (recomendado para produção)
-- Inicializa watermark com o último ticket existente de cada tenant
INSERT INTO export_watermark (tenant_id, last_exported_created_at, rows_exported, export_type, notes)
SELECT 
    c.id as tenant_id,
    COALESCE(MAX(t.created_at), NOW() - INTERVAL '7 days') as last_exported_created_at,
    COUNT(t.id) as rows_exported,
    'FULL' as export_type,
    'Inicialização automática - usando MAX(created_at) existente' as notes
FROM tenants c
LEFT JOIN tickets t ON c.id = t.tenant_id
WHERE NOT EXISTS (
    SELECT 1 FROM export_watermark ew WHERE ew.tenant_id = c.id
)
GROUP BY c.id;

-- OPÇÃO B (comentada): Forçar re-exportação dos últimos 7 dias
-- Descomente se quiser re-exportar dados recentes
/*
INSERT INTO export_watermark (tenant_id, last_exported_created_at, rows_exported, export_type, notes)
SELECT 
    id as tenant_id,
    NOW() - INTERVAL '7 days' as last_exported_created_at,
    0 as rows_exported,
    'FULL' as export_type,
    'Inicialização manual - forçando re-exportação dos últimos 7 dias' as notes
FROM tenants
WHERE NOT EXISTS (
    SELECT 1 FROM export_watermark ew WHERE ew.tenant_id = tenants.id
);
*/

-- ==============================================================================
-- View para monitoramento
-- ==============================================================================

CREATE OR REPLACE VIEW vw_export_watermark_status AS
SELECT 
    ew.tenant_id,
    c.name as tenant_name,
    ew.last_exported_created_at,
    ew.last_export_success_at,
    ew.rows_exported,
    ew.export_type,
    EXTRACT(EPOCH FROM (NOW() - ew.last_export_success_at))/3600 as hours_since_last_export,
    COUNT(t.id) as pending_tickets,
    ew.notes
FROM export_watermark ew
JOIN tenants c ON ew.tenant_id = c.id
LEFT JOIN tickets t ON t.tenant_id = ew.tenant_id 
    AND t.created_at > ew.last_exported_created_at
GROUP BY 
    ew.tenant_id, c.name, ew.last_exported_created_at, 
    ew.last_export_success_at, ew.rows_exported, ew.export_type, ew.notes
ORDER BY ew.last_export_success_at DESC;

COMMENT ON VIEW vw_export_watermark_status IS 'Visão do status de exportação por tenant com tickets pendentes';

-- ==============================================================================
-- Queries úteis para monitoramento
-- ==============================================================================

-- Ver status de todas as tenants
-- SELECT * FROM vw_export_watermark_status;

-- Tenants com exportação atrasada (> 1 hora)
-- SELECT * FROM vw_export_watermark_status WHERE hours_since_last_export > 1;

-- Resetar watermark de uma tenant específica (forçar re-exportação)
-- UPDATE export_watermark 
-- SET last_exported_created_at = NOW() - INTERVAL '7 days',
--     export_type = 'FULL',
--     notes = 'Reset manual - re-exportação forçada'
-- WHERE tenant_id = '<uuid>';

COMMIT;
