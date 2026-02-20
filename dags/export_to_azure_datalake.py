"""
DAG para exportar dados do PostgreSQL para Azure Data Lake Storage Gen2
com isolamento por tenant_id.

Cada tenant tem sua própria pasta: tenant_<uuid>/
Estrutura: tenant_<uuid>/table_name/year=YYYY/month=MM/day=DD/*.parquet


NOTA: Esta DAG é disparada automaticamente (dataset-aware scheduling) sempre que
a view materializada bi_tickets_flat é atualizada pela DAG tickets_powerbi_etl.
Nenhum schedule_interval fixo é necessário - a exportação ocorre imediatamente
após o refresh da view ser concluído, garantindo que dados novos são sempre exportados.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import io


# Import dataset para criar dependência com DAG de refresh
from datasets import VIEW_TICKETS_DATASET

# Nome da conexão PostgreSQL (configurável via variável de ambiente)
POSTGRES_CONN_ID = os.getenv('POSTGRES_CONN_ID', 'postgres_prod')

# Configurações Azure Data Lake
AZURE_STORAGE_ACCOUNT = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
AZURE_CONTAINER = os.getenv('AZURE_CONTAINER_NAME', 'tickets-data')
AZURE_TENANT_ID = os.getenv('AZURE_TENANT_ID')
AZURE_CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
AZURE_CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')

# Configuração do schema PostgreSQL
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(datetime.now().year, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'export_tickets_to_azure_datalake',
    default_args=default_args,
    description='Exporta dados do PostgreSQL para Azure Data Lake com isolamento físico por tenant_id',
    # Dataset-aware scheduling: executa sempre que VIEW_TICKETS_DATASET é atualizado
    # Substitui schedule_interval fixo por disparo reativo quando bi_tickets_flat é refreshed
    schedule=[VIEW_TICKETS_DATASET],
    catchup=False,
    is_paused_upon_creation=False,  # DAG ativa por padrão
    default_view='grid',  # Visualização padrão na UI
    tags=['azure', 'datalake', 'export', 'multi-tenant', 'parquet', 'dataset-aware'],
    max_active_runs=1,  # Apenas uma execução por vez para evitar conflitos
)


def get_datalake_client():
    """
    Cria cliente Azure Data Lake com autenticação via Service Principal.
    
    Returns:
        DataLakeServiceClient: Cliente autenticado para o Data Lake
    """
    if not all([AZURE_STORAGE_ACCOUNT, AZURE_TENANT_ID, AZURE_CLIENT_ID, AZURE_CLIENT_SECRET]):
        raise ValueError("❌ Variáveis Azure não configuradas. Verifique .env")
    
    credential = ClientSecretCredential(
        tenant_id=AZURE_TENANT_ID,
        client_id=AZURE_CLIENT_ID,
        client_secret=AZURE_CLIENT_SECRET
    )
    
    account_url = f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    
    print(f"✅ Cliente Data Lake conectado: {account_url}")
    return service_client


def get_tenants_with_directories():
    """
    Lista tenants (tenant_id) que já têm diretório criado no Azure Data Lake.
    Filtra apenas diretórios raiz no padrão tenant_<UUID>.
    
    Returns:
        set: Conjunto de UUIDs de tenants com diretórios
    """
    try:
        service_client = get_datalake_client()
        file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
        
        # Listar apenas diretórios raiz (depth=1)
        paths = file_system_client.get_paths(path="", recursive=False)
        
        tenants = set()
        for path in paths:
            # Filtrar apenas diretórios com padrão tenant_<uuid>
            if path.is_directory and path.name.startswith('tenant_'):
                # Extrair UUID do nome do diretório (remove 'tenant_' prefix)
                tenant_uuid = path.name.replace('tenant_', '')
                tenants.add(tenant_uuid)
        
        print(f"✅ Encontrados {len(tenants)} diretórios no Azure Data Lake")
        return tenants
        
    except Exception as e:
        print(f"⚠️ Erro ao listar diretórios do Azure: {e}")
        print("⚠️ Exportação será feita para TODAS as tenants do banco")
        return set()


def get_tenant_export_config(hook, tenant_id):
    """
    Obtém configuração de formatos de exportação para uma tenant.
    Retorna formatos habilitados, delimitador CSV, encoding, etc.
    
    Args:
        hook (PostgresHook): Conexão PostgreSQL
        tenant_id (str): UUID da tenant
    
    Returns:
        dict: Configuração de exportação {formats: [], csv_delimiter, csv_encoding, parquet_compression}
    """
    query = f"""
        SELECT * FROM {POSTGRES_SCHEMA}.get_tenant_export_config('{tenant_id}')
    """
    result = hook.get_first(query)
    
    if result:
        return {
            'formats': result[0] if result[0] else ['parquet'],
            'csv_delimiter': result[1] or ',',
            'csv_encoding': result[2] or 'utf-8',
            'csv_date_format': result[3] or '%Y-%m-%d %H:%M:%S',
            'parquet_compression': result[4] or 'snappy'
        }
    else:
        # Configuração padrão se tenant não existe
        return {
            'formats': ['parquet'],
            'csv_delimiter': ',',
            'csv_encoding': 'utf-8',
            'csv_date_format': '%Y-%m-%d %H:%M:%S',
            'parquet_compression': 'snappy'
        }


def get_tenant_watermark(hook, tenant_id):
    """
    Obtém o último created_at exportado para uma tenant (watermark).
    Se não existir, retorna None (primeira exportação).
    
    Args:
        hook (PostgresHook): Conexão PostgreSQL
        tenant_id (str): UUID da tenant
    
    Returns:
        datetime or None: Último created_at exportado ou None
    """
    query = f"""
        SELECT last_exported_created_at 
        FROM {POSTGRES_SCHEMA}.export_watermark 
        WHERE tenant_id = '{tenant_id}'
    """
    result = hook.get_first(query)
    return result[0] if result else None


def update_tenant_watermark(hook, tenant_id, last_created_at, rows_exported, export_type='INCREMENTAL'):
    """
    Atualiza o watermark após exportação bem-sucedida.
    OBS: Watermark é único por tenant (não duplicado por formato).
    
    Args:
        hook (PostgresHook): Conexão PostgreSQL
        tenant_id (str): UUID da tenant
        last_created_at (datetime): Último created_at exportado
        rows_exported (int): Quantidade de registros exportados
        export_type (str): Tipo de exportação ('FULL' ou 'INCREMENTAL')
    """
    query = f"""
        INSERT INTO {POSTGRES_SCHEMA}.export_watermark 
            (tenant_id, last_exported_created_at, rows_exported, export_type)
        VALUES ('{tenant_id}', '{last_created_at}', {rows_exported}, '{export_type}')
        ON CONFLICT (tenant_id) DO UPDATE SET
            last_exported_created_at = EXCLUDED.last_exported_created_at,
            last_export_success_at = NOW(),
            rows_exported = EXCLUDED.rows_exported,
            export_type = EXCLUDED.export_type,
            updated_at = NOW()
    """
    hook.run(query)
    print(f"✅ Watermark atualizado: {tenant_id} -> {last_created_at} ({rows_exported} rows)")


def log_export_to_database(hook, tenant_id, file_format, rows_exported, file_path, export_type, duration_seconds=0, status='SUCCESS'):
    """
    Registra exportação no bi_refresh_log para auditoria.
    
    Args:
        hook (PostgresHook): Conexão PostgreSQL
        tenant_id (str): UUID da tenant
        file_format (str): Formato do arquivo ('parquet' ou 'csv')
        rows_exported (int): Quantidade de registros
        file_path (str): Path completo no Azure
        export_type (str): 'FULL' ou 'INCREMENTAL'
        duration_seconds (float): Tempo de execução
        status (str): 'SUCCESS' ou 'ERROR'
    """
    query = f"""
        INSERT INTO {POSTGRES_SCHEMA}.bi_refresh_log (
            view_name, status, rows_affected, duration_seconds,
            airflow_dag_id, tenant_id, export_type, file_format,
            files_created, export_destination, azure_storage_account,
            azure_container, azure_file_path
        )
        VALUES (
            'export_tickets_to_azure_datalake',
            '{status}',
            {rows_exported},
            {duration_seconds},
            'export_tickets_to_azure_datalake',
            '{tenant_id}',
            '{export_type}',
            '{file_format}',
            1,
            'AZURE_DATALAKE',
            '{AZURE_STORAGE_ACCOUNT}',
            '{AZURE_CONTAINER}',
            '{file_path}'
        )
    """
    hook.run(query)


def upload_dataframe_to_datalake(df, file_path, file_format='parquet', config=None):
    """
    Faz upload de DataFrame para o Azure Data Lake em formato especificado.
    
    Args:
        df (pd.DataFrame): DataFrame com os dados a serem exportados
        file_path (str): Caminho completo no Data Lake
        file_format (str): Formato do arquivo ('parquet' ou 'csv')
        config (dict): Configurações específicas do formato (compressão, delimiter, etc)
    """
    service_client = get_datalake_client()
    file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
    
    config = config or {}
    buffer = io.BytesIO()
    
    # Converter DataFrame para o formato especificado
    if file_format == 'parquet':
        df.to_parquet(
            buffer,
            engine='pyarrow',
            compression=config.get('parquet_compression', 'snappy'),
            index=False
        )
    elif file_format == 'csv':
        df.to_csv(
            buffer,
            sep=config.get('csv_delimiter', ','),
            encoding=config.get('csv_encoding', 'utf-8'),
            index=False,
            date_format=config.get('csv_date_format', '%Y-%m-%d %H:%M:%S')
        )
    else:
        raise ValueError(f"Formato não suportado: {file_format}")
    
    buffer.seek(0)
    
    # Upload para Data Lake (sobrescreve se já existir)
    file_client = file_system_client.get_file_client(file_path)
    file_client.upload_data(buffer.read(), overwrite=True)
    
    file_size_kb = len(buffer.getvalue()) / 1024
    print(f"✅ Uploaded {len(df):,} rows ({file_size_kb:.2f} KB) to {file_path} [{file_format.upper()}]")


def export_tickets_to_datalake(**context):
    """
    Exporta bi_tickets_flat para Azure Data Lake com exportação incremental baseada em watermark.
    
    Estratégia OTIMIZADA:
    - Usa watermark (último created_at exportado) por tenant para evitar duplicatas
    - Fallback: janela de 30 minutos se watermark não existir
    - Agrupa por tenant_id para garantir isolamento físico
    - Exporta APENAS para tenants que têm diretório no Azure
    - Particionamento Hive: year=YYYY/month=MM/day=DD/
    - Formato: Parquet com compressão Snappy
    - Atualiza watermark após sucesso
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Listar tenants que têm diretório no Azure (com Service Principal configurado)
    azure_tenants = get_tenants_with_directories()
    
    if not azure_tenants:
        print("⚠️ Nenhum diretório tenant_* encontrado no Azure Data Lake")
        print("⚠️ Execute o script generate_azure_service_principals.py primeiro")
        return
    
    print(f"📂 Tenants com diretórios no Azure: {len(azure_tenants)}")
    
    # Informações de particionamento baseadas na execution_date
    execution_date = context['execution_date']
    year = execution_date.strftime('%Y')
    month = execution_date.strftime('%m')
    day = execution_date.strftime('%d')
    timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
    
    # Contadores para estatísticas
    total_exported = 0
    total_tenants_exported = 0
    tenants_skipped = 0
    tenants_no_new_data = 0
    
    # Processar cada tenant individualmente (permite watermark por tenant)
    for tenant_id in azure_tenants:
        print(f"\n{'='*80}")
        print(f"Processando tenant: {tenant_id}")
        print(f"{'='*80}")
        
        # Obter watermark da tenant
        watermark = get_tenant_watermark(hook, tenant_id)
        
        if watermark:
            # Exportação incremental: apenas tickets APÓS o último exportado
            print(f"✅ Watermark encontrado: {watermark}")
            query = f"""
                SELECT *
                FROM {POSTGRES_SCHEMA}.bi_tickets_flat
                WHERE tenant_id = '{tenant_id}'
                  AND created_at > '{watermark}'
                ORDER BY created_at
            """
            export_type = 'INCREMENTAL'
        else:
            # Primeira exportação: usar fallback de 30 minutos (2x a frequência do DAG)
            print("⚠️ Watermark não encontrado (primeira exportação)")
            print("📊 Usando fallback: últimos 30 minutos")
            query = f"""
                SELECT *
                FROM {POSTGRES_SCHEMA}.bi_tickets_flat
                WHERE tenant_id = '{tenant_id}'
                  AND (created_at >= NOW() - INTERVAL '30 minutes' 
                       OR finish_time >= NOW() - INTERVAL '30 minutes')
                ORDER BY created_at
            """
            export_type = 'FULL'
        
        # Buscar dados
        df = hook.get_pandas_df(query)
        
        if df.empty:
            print(f"ℹ️ Nenhum dado novo para exportar (tenant já está atualizada)")
            tenants_no_new_data += 1
            continue
        
        print(f"📊 Encontrados {len(df):,} tickets novos para exportar")
        
        # Obter configuração de formatos para a tenant
        export_config = get_tenant_export_config(hook, tenant_id)
        formats_to_export = export_config['formats']
        
        print(f"📋 Formatos habilitados: {', '.join(formats_to_export)}")
        
        # Obter último created_at exportado (para watermark)
        last_created_at = df['created_at'].max()
        
        formats_success = 0
        formats_failed = 0
        
        # Processar cada formato habilitado
        for file_format in formats_to_export:
            extension = file_format  # 'parquet' ou 'csv'
            
            # Preparar caminho do arquivo com particionamento por formato
            file_path = (
                f"tenant_{tenant_id}/tickets/"
                f"format={file_format}/"
                f"year={year}/month={month}/day={day}/"
                f"tickets_{timestamp}.{extension}"
            )
            
            try:
                import time
                start_time = time.time()
                
                # Upload para Azure Data Lake no formato especificado
                upload_dataframe_to_datalake(
                    df=df,
                    file_path=file_path,
                    file_format=file_format,
                    config=export_config
                )
                
                duration = time.time() - start_time
                
                # Registrar exportação no log
                log_export_to_database(
                    hook=hook,
                    tenant_id=tenant_id,
                    file_format=file_format,
                    rows_exported=len(df),
                    file_path=file_path,
                    export_type=export_type,
                    duration_seconds=round(duration, 2),
                    status='SUCCESS'
                )
                
                formats_success += 1
                print(f"   ✅ {file_format.upper()}: {len(df):,} tickets exportados")
                
            except Exception as e:
                print(f"   ❌ Erro ao exportar {file_format.upper()}: {e}")
                formats_failed += 1
                
                # Registrar erro no log
                try:
                    log_export_to_database(
                        hook=hook,
                        tenant_id=tenant_id,
                        file_format=file_format,
                        rows_exported=0,
                        file_path=file_path,
                        export_type=export_type,
                        status='ERROR'
                    )
                except:
                    pass  # Ignora erro ao registrar erro
                
                continue
        
        # Atualizar watermark apenas se pelo menos 1 formato teve sucesso
        if formats_success > 0:
            try:
                update_tenant_watermark(
                    hook=hook,
                    tenant_id=tenant_id,
                    last_created_at=last_created_at,
                    rows_exported=len(df),
                    export_type=export_type
                )
                
                total_exported += len(df)
                total_tenants_exported += 1
                
                print(f"✅ Tenant {tenant_id}: {formats_success}/{len(formats_to_export)} formatos exportados")
                print(f"   Último created_at: {last_created_at}")
                
            except Exception as e:
                print(f"⚠️ Erro ao atualizar watermark: {e}")
        else:
            print(f"❌ Todos os formatos falharam para tenant {tenant_id}")
            tenants_skipped += 1
    
    # Resumo final
    print(f"\n{'='*80}")
    print("📊 RESUMO DA EXPORTAÇÃO")
    print(f"{'='*80}")
    print(f"✅ Total exportado: {total_exported:,} registros")
    print(f"✅ Tenants exportados: {total_tenants_exported}/{len(azure_tenants)}")
    print(f"ℹ️ Tenants sem dados novos: {tenants_no_new_data}")
    if tenants_skipped > 0:
        print(f"⚠️ Tenants com erro: {tenants_skipped}")
    print(f"{'='*80}\n")


# Definir task
export_tickets = PythonOperator(
    task_id='export_tickets_to_datalake',
    python_callable=export_tickets_to_datalake,
    dag=dag,
)

export_tickets