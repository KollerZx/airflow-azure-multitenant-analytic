-- ============================================================================
-- Script: Criação de Tabela de Configuração de Formatos de Exportação
-- Arquivo: 06_create_tenant_export_settings.sql
-- Descrição: Configuração multi-formato (Parquet + CSV) por tenant

-- ============================================================================

-- Configurar search_path (ajustar se necessário)
-- SET search_path TO public;

-- -----------------------------------------------------------------------------
-- 1. CRIAÇÃO DA TABELA tenant_export_settings
-- -----------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS tenant_export_settings (
    tenant_id TEXT NOT NULL PRIMARY KEY,
    
    -- Formatos habilitados para exportação
    export_formats TEXT[] NOT NULL DEFAULT ARRAY['parquet'],
    -- Valores possíveis: 'parquet', 'csv', 'json' (futuro)
    -- Exemplo: ARRAY['parquet', 'csv'] exporta ambos
    
    -- Configurações CSV
    csv_delimiter VARCHAR(1) DEFAULT ',',
    csv_encoding VARCHAR(20) DEFAULT 'utf-8',
    csv_quoting VARCHAR(20) DEFAULT 'minimal',  -- 'minimal', 'all', 'nonnumeric'
    csv_date_format VARCHAR(50) DEFAULT '%Y-%m-%d %H:%M:%S',  -- Formato Python strftime
    
    -- Configurações Parquet
    parquet_compression VARCHAR(20) DEFAULT 'snappy',  -- 'snappy', 'gzip', 'brotli', 'none'
    parquet_engine VARCHAR(20) DEFAULT 'pyarrow',      -- 'pyarrow', 'fastparquet'
    
    -- Controle de ativação
    active BOOLEAN DEFAULT true,
    
    -- Observações
    notes TEXT,
    
    -- Auditoria
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    created_by VARCHAR(255) DEFAULT CURRENT_USER,
    updated_by VARCHAR(255) DEFAULT CURRENT_USER,
    
    -- Constraints
    CONSTRAINT fk_tenant_export_settings_tenant 
        FOREIGN KEY (tenant_id) REFERENCES tenants(id) ON DELETE CASCADE,
    
    CONSTRAINT check_valid_formats 
        CHECK (export_formats <@ ARRAY['parquet', 'csv', 'json']),
    
    CONSTRAINT check_csv_delimiter 
        CHECK (csv_delimiter IN (',', ';', '|', '\t')),
    
    CONSTRAINT check_csv_encoding 
        CHECK (csv_encoding IN ('utf-8', 'utf-8-sig', 'latin1', 'iso-8859-1')),
    
    CONSTRAINT check_parquet_compression 
        CHECK (parquet_compression IN ('snappy', 'gzip', 'brotli', 'lz4', 'zstd', 'none'))
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_tenant_export_settings_active ON tenant_export_settings(active) WHERE active = true;
CREATE INDEX IF NOT EXISTS idx_tenant_export_settings_formats ON tenant_export_settings USING GIN(export_formats);

-- Comentários
COMMENT ON TABLE tenant_export_settings IS 'Configuração de formatos de exportação por tenant para Azure Data Lake';
COMMENT ON COLUMN tenant_export_settings.export_formats IS 'Array de formatos habilitados: parquet, csv, json';
COMMENT ON COLUMN tenant_export_settings.csv_delimiter IS 'Delimitador CSV: , (padrão) ou ; (Europa)';
COMMENT ON COLUMN tenant_export_settings.csv_encoding IS 'Encoding do CSV: utf-8 (padrão) ou latin1';
COMMENT ON COLUMN tenant_export_settings.parquet_compression IS 'Compressão Parquet: snappy (padrão), gzip, brotli';
COMMENT ON COLUMN tenant_export_settings.active IS 'Se false, tenant usa configuração default (apenas Parquet)';

-- -----------------------------------------------------------------------------
-- 2. TRIGGER PARA ATUALIZAR updated_at AUTOMATICAMENTE
-- -----------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION update_tenant_export_settings_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    NEW.updated_by = CURRENT_USER;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_update_tenant_export_settings_timestamp ON tenant_export_settings;

CREATE TRIGGER trg_update_tenant_export_settings_timestamp
    BEFORE UPDATE ON tenant_export_settings
    FOR EACH ROW
    EXECUTE FUNCTION update_tenant_export_settings_timestamp();

-- -----------------------------------------------------------------------------
-- 3. POPULAR CONFIGURAÇÕES PADRÃO PARA EMPRESAS EXISTENTES
-- -----------------------------------------------------------------------------

-- Inserir configuração padrão (apenas Parquet) para tenants que já existem
INSERT INTO tenant_export_settings (tenant_id, export_formats, notes)
SELECT 
    id,
    ARRAY['parquet'],  -- Mantém comportamento atual
    'Configuração padrão - migração automática'
FROM tenants
ON CONFLICT (tenant_id) DO NOTHING;

-- -----------------------------------------------------------------------------
-- 4. VIEWS AUXILIARES
-- -----------------------------------------------------------------------------

-- View: Tenants com exportação CSV habilitada
CREATE OR REPLACE VIEW vw_tenants_csv_enabled AS
SELECT 
    ces.tenant_id,
    c.name as tenant_name,
    ces.export_formats,
    ces.csv_delimiter,
    ces.csv_encoding,
    ces.active,
    ces.created_at,
    ces.updated_at
FROM tenant_export_settings ces
JOIN tenants c ON ces.tenant_id = c.id
WHERE ces.active = true
  AND 'csv' = ANY(ces.export_formats)
ORDER BY c.name;

COMMENT ON VIEW vw_tenants_csv_enabled IS 'Lista tenants com exportação CSV ativa';

-- View: Sumário de formatos por tenant
CREATE OR REPLACE VIEW vw_export_formats_summary AS
SELECT 
    CASE 
        WHEN ces.export_formats IS NULL THEN 'default'
        WHEN array_length(ces.export_formats, 1) = 1 THEN 'single'
        ELSE 'multi'
    END as format_strategy,
    COUNT(DISTINCT c.id) as tenants_count,
    ARRAY_AGG(DISTINCT unnest_format) as formats_used
FROM tenants c
LEFT JOIN tenant_export_settings ces ON c.id = ces.tenant_id
LEFT JOIN LATERAL unnest(COALESCE(ces.export_formats, ARRAY['parquet'])) as unnest_format ON true
WHERE ces.active = true OR ces.active IS NULL
GROUP BY format_strategy;

COMMENT ON VIEW vw_export_formats_summary IS 'Estatísticas de uso de formatos de exportação';

-- -----------------------------------------------------------------------------
-- 5. ATUALIZAR bi_refresh_log PARA RASTREAR FORMATO
-- -----------------------------------------------------------------------------

-- Adicionar coluna file_format se não existir
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_schema = 'public' 
          AND table_name = 'bi_refresh_log' 
          AND column_name = 'file_format'
    ) THEN
        ALTER TABLE bi_refresh_log 
        ADD COLUMN file_format VARCHAR(20) DEFAULT 'parquet';
        
        COMMENT ON COLUMN bi_refresh_log.file_format IS 'Formato do arquivo exportado: parquet, csv, json';
    END IF;
END $$;

-- Atualizar view vw_export_stats_by_tenant para incluir formato
-- Drop necessário pois estamos mudando a estrutura (adicionando file_format)
DROP VIEW IF EXISTS vw_export_stats_by_tenant;

CREATE VIEW vw_export_stats_by_tenant AS
SELECT 
    brl.tenant_id,
    c.name as tenant_name,
    brl.file_format,
    COUNT(*) as total_exports,
    SUM(brl.rows_affected) as total_rows_exported,
    SUM(brl.files_created) as total_files_created,
    MAX(brl.refresh_timestamp) as last_export_date,
    AVG(brl.duration_seconds) as avg_duration_seconds,
    COUNT(*) FILTER (WHERE brl.status = 'SUCCESS') as successful_exports,
    COUNT(*) FILTER (WHERE brl.status = 'ERROR') as failed_exports
FROM bi_refresh_log brl
LEFT JOIN tenants c ON brl.tenant_id = c.id
WHERE brl.export_destination = 'AZURE_DATALAKE'
GROUP BY brl.tenant_id, c.name, brl.file_format
ORDER BY c.name, brl.file_format;

-- -----------------------------------------------------------------------------
-- 6. FUNÇÕES AUXILIARES
-- -----------------------------------------------------------------------------

-- Função: Obter configuração de exportação para uma tenant
CREATE OR REPLACE FUNCTION get_tenant_export_config(p_tenant_id TEXT)
RETURNS TABLE (
    export_formats TEXT[],
    csv_delimiter VARCHAR(1),
    csv_encoding VARCHAR(20),
    csv_date_format VARCHAR(50),
    parquet_compression VARCHAR(20)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        COALESCE(ces.export_formats, ARRAY['parquet']),
        COALESCE(ces.csv_delimiter, ','),
        COALESCE(ces.csv_encoding, 'utf-8'),
        COALESCE(ces.csv_date_format, 'YYYY-MM-DD HH24:MI:SS'),
        COALESCE(ces.parquet_compression, 'snappy')
    FROM tenants c
    LEFT JOIN tenant_export_settings ces ON c.id = ces.tenant_id AND ces.active = true
    WHERE c.id = p_tenant_id;
    
    -- Se tenant não existe, retornar configuração padrão
    IF NOT FOUND THEN
        RETURN QUERY SELECT 
            ARRAY['parquet']::TEXT[], 
            ','::VARCHAR(1), 
            'utf-8'::VARCHAR(20),
            'YYYY-MM-DD HH24:MI:SS'::VARCHAR(50),
            'snappy'::VARCHAR(20);
    END IF;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION get_tenant_export_config IS 'Retorna configuração de exportação para uma tenant (ou padrão se não configurada)';

-- Função: Habilitar CSV para uma tenant
CREATE OR REPLACE FUNCTION enable_csv_export(
    p_tenant_id TEXT,
    p_delimiter VARCHAR(1) DEFAULT ',',
    p_encoding VARCHAR(20) DEFAULT 'utf-8'
)
RETURNS BOOLEAN AS $$
BEGIN
    INSERT INTO tenant_export_settings (
        tenant_id, 
        export_formats, 
        csv_delimiter, 
        csv_encoding,
        notes
    )
    VALUES (
        p_tenant_id,
        ARRAY['parquet', 'csv'],
        p_delimiter,
        p_encoding,
        'CSV habilitado manualmente via função enable_csv_export()'
    )
    ON CONFLICT (tenant_id) DO UPDATE SET
        export_formats = CASE 
            WHEN 'csv' = ANY(tenant_export_settings.export_formats) 
            THEN tenant_export_settings.export_formats
            ELSE array_append(tenant_export_settings.export_formats, 'csv')
        END,
        csv_delimiter = p_delimiter,
        csv_encoding = p_encoding,
        active = true,
        notes = COALESCE(tenant_export_settings.notes, '') || ' | CSV habilitado em ' || NOW()::TEXT;
    
    RETURN true;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Erro ao habilitar CSV para tenant %: %', p_tenant_id, SQLERRM;
        RETURN false;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION enable_csv_export IS 'Habilita exportação CSV para uma tenant específica';

-- Função: Desabilitar CSV para uma tenant (mantém apenas Parquet)
CREATE OR REPLACE FUNCTION disable_csv_export(p_tenant_id TEXT)
RETURNS BOOLEAN AS $$
BEGIN
    UPDATE tenant_export_settings
    SET export_formats = array_remove(export_formats, 'csv'),
        notes = COALESCE(notes, '') || ' | CSV desabilitado em ' || NOW()::TEXT
    WHERE tenant_id = p_tenant_id;
    
    RETURN FOUND;
EXCEPTION
    WHEN OTHERS THEN
        RAISE NOTICE 'Erro ao desabilitar CSV para tenant %: %', p_tenant_id, SQLERRM;
        RETURN false;
END;
$$ LANGUAGE plpgsql;

COMMENT ON FUNCTION disable_csv_export IS 'Desabilita exportação CSV para uma tenant (mantém apenas Parquet)';

-- -----------------------------------------------------------------------------
-- 7. VALIDAÇÕES E TESTES
-- -----------------------------------------------------------------------------

-- Verificar criação da tabela
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'tenant_export_settings') THEN
        RAISE NOTICE '✅ Tabela tenant_export_settings criada com sucesso';
    ELSE
        RAISE EXCEPTION '❌ Falha ao criar tabela tenant_export_settings';
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_views WHERE schemaname = 'public' AND viewname = 'vw_tenants_csv_enabled') THEN
        RAISE NOTICE '✅ View vw_tenants_csv_enabled criada com sucesso';
    END IF;
    
    IF EXISTS (SELECT 1 FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public') AND proname = 'get_tenant_export_config') THEN
        RAISE NOTICE '✅ Função get_tenant_export_config() criada com sucesso';
    END IF;
END $$;

-- Contar configurações inseridas
SELECT 
    COUNT(*) as total_tenants_configured,
    COUNT(*) FILTER (WHERE 'csv' = ANY(export_formats)) as with_csv_enabled,
    COUNT(*) FILTER (WHERE 'parquet' = ANY(export_formats)) as with_parquet_enabled
FROM tenant_export_settings;

-- -----------------------------------------------------------------------------
-- Script concluído
-- Execute: psql -h localhost -p 5432 -U postgres -d <database> -f 06_create_tenant_export_settings.sql
-- Ou via helper: ./run_sql_on_prod.sh sql/06_create_tenant_export_settings.sql
-- -----------------------------------------------------------------------------
