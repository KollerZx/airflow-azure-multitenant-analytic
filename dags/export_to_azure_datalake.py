"""
DAG para exportar dados do PostgreSQL para Azure Data Lake Storage Gen2
com isolamento por tenant_id.

Cada tenant tem sua própria pasta: tenant_<uuid>/
Estrutura: tenant_<uuid>/table_name/year=YYYY/month=MM/day=DD/*.parquet

Autor: Airflow Team
Data: 2026-01-14
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
    schedule_interval='*/15 * * * *',  # A cada 15 minutos
    catchup=False,
    is_paused_upon_creation=False,  # DAG ativa por padrão
    default_view='grid',  # Visualização padrão na UI
    tags=['azure', 'datalake', 'export', 'multi-tenant', 'parquet'],
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
        print("⚠️ Exportação será feita para TODOS os tenants do banco")
        return set()


def upload_dataframe_to_datalake(df, file_path):
    """
    Faz upload de DataFrame como Parquet para o Azure Data Lake.
    
    Args:
        df (pd.DataFrame): DataFrame com os dados a serem exportados
        file_path (str): Caminho completo no Data Lake 
                        (ex: tenant_xxx/tickets/year=2026/month=01/day=14/data_timestamp.parquet)
    """
    service_client = get_datalake_client()
    file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
    
    # Converter DataFrame para Parquet em memória
    parquet_buffer = io.BytesIO()
    df.to_parquet(
        parquet_buffer, 
        engine='pyarrow', 
        compression='snappy',  # Compressão otimizada para analytics
        index=False
    )
    parquet_buffer.seek(0)
    
    # Upload para Data Lake (sobrescreve se já existir)
    file_client = file_system_client.get_file_client(file_path)
    file_client.upload_data(parquet_buffer.read(), overwrite=True)
    
    file_size_kb = len(parquet_buffer.getvalue()) / 1024
    print(f"✅ Uploaded {len(df):,} rows ({file_size_kb:.2f} KB) to {file_path}")


def export_tickets_to_datalake(**context):
    """
    Exporta bi_tickets_flat para Azure Data Lake, particionado por tenant_id e data.
    
    Estratégia:
    - Query incremental: apenas tickets criados/finalizados nas últimas 24h
    - Agrupa por tenant_id para garantir isolamento físico
    - Exporta APENAS para tenants que têm diretório no Azure (com Service Principal configurado)
    - Particionamento Hive: year=YYYY/month=MM/day=DD/
    - Formato: Parquet com compressão Snappy
    """
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Listar tenants que têm diretório no Azure (com Service Principal configurado)
    azure_tenants = get_tenants_with_directories()
    
    if not azure_tenants:
        print("⚠️ Nenhum diretório tenant_* encontrado no Azure Data Lake")
        print("⚠️ Execute o script generate_azure_service_principals.py primeiro")
        return
    
    print(f"📂 Tenants com diretórios no Azure: {len(azure_tenants)}")
    
    # Query incremental: últimas 24h (usando created_at e finish_time)
    query = f"""
        SELECT 
            *
        FROM {POSTGRES_SCHEMA}.bi_tickets_flat
        WHERE created_at >= NOW() - INTERVAL '1 day' 
           OR finish_time >= NOW() - INTERVAL '1 day'
    """
    
    print("🔍 Buscando tickets atualizados nas últimas 24h...")
    df = hook.get_pandas_df(query)
    
    if df.empty:
        print("⚠️ Nenhum ticket atualizado nas últimas 24h")
        return
    
    # Filtrar apenas tenants que têm diretório no Azure
    df_filtered = df[df['tenant_id'].isin(azure_tenants)]
    
    if df_filtered.empty:
        tenants_no_dir = df['tenant_id'].unique()
        print(f"⚠️ Tickets encontrados para {len(tenants_no_dir)} tenant(s), mas nenhuma tem diretório no Azure:")
        for tenant_id in tenants_no_dir:
            print(f"   - {tenant_id}")
        print("⚠️ Execute generate_azure_service_principals.py para criar Service Principals para estes tenants")
        return
    
    # Informações de particionamento baseadas na execution_date
    execution_date = context['execution_date']
    year = execution_date.strftime('%Y')
    month = execution_date.strftime('%m')
    day = execution_date.strftime('%d')
    timestamp = execution_date.strftime('%Y%m%d_%H%M%S')
    
    # Agrupar por tenant_id e exportar separadamente (isolamento físico)
    total_tenants = df_filtered['tenant_id'].nunique()
    tenants_skipped = df['tenant_id'].nunique() - total_tenants
    
    print(f"📊 Exportando para {total_tenants} tenant(s) com Service Principal configurado")
    if tenants_skipped > 0:
        print(f"⏭️  Ignorando {tenants_skipped} tenant(s) sem diretório no Azure")
    
    for idx, (tenant_id, group_df) in enumerate(df_filtered.groupby('tenant_id'), 1):
        file_path = (
            f"tenant_{tenant_id}/tickets/"
            f"year={year}/month={month}/day={day}/"
            f"tickets_{timestamp}.parquet"
        )
        
        upload_dataframe_to_datalake(group_df, file_path)
        print(f"[{idx}/{total_tenants}] Tenant {tenant_id}: {len(group_df):,} tickets")
    
    print(f"✅ Exportação concluída: {len(df_filtered):,} registros em {total_tenants} tenant(s)")




# Definir task
export_tickets = PythonOperator(
    task_id='export_tickets_to_datalake',
    python_callable=export_tickets_to_datalake,
    dag=dag,
)

export_tickets