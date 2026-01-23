"""
DAG para compactar arquivos Parquet e CSV diários no Azure Data Lake.

Este DAG consolida múltiplos arquivos Parquet e CSV gerados ao longo do dia
em um único arquivo por tenant/dia, reduzindo custos de storage e
melhorando performance de queries no Power BI.

Execução: Diariamente às 2:00 AM
Processo:
1. Lista tenants com diretórios no Azure
2. Para cada tenant, identifica arquivos do dia anterior (D-1)
3. Baixa e combina todos os arquivos em um único DataFrame
4. Remove duplicatas (se houver)
5. Upload do arquivo consolidado
6. Remove arquivos individuais após validação

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
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    'azure_datalake_compaction',
    default_args=default_args,
    description='Compacta arquivos Parquet diários no Azure Data Lake (consolidação D-1)',
    schedule_interval='0 2 * * *',  # Diariamente às 2:00 AM
    catchup=False,
    is_paused_upon_creation=False,
    default_view='grid',
    tags=['azure', 'datalake', 'compaction', 'maintenance', 'parquet'],
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
    
    return service_client


def get_tenants_with_directories():
    """
    Lista tenants (tenant_id) que têm diretório no Azure Data Lake.
    
    Returns:
        set: Conjunto de UUIDs de tenants com diretórios
    """
    try:
        service_client = get_datalake_client()
        file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
        
        paths = file_system_client.get_paths(path="", recursive=False)
        
        tenants = set()
        for path in paths:
            if path.is_directory and path.name.startswith('tenant_'):
                tenant_uuid = path.name.replace('tenant_', '')
                tenants.add(tenant_uuid)
        
        print(f"✅ Encontrados {len(tenants)} diretórios no Azure Data Lake")
        return tenants
        
    except Exception as e:
        print(f"❌ Erro ao listar diretórios do Azure: {e}")
        return set()


def list_files(service_client, tenant_id, file_format, year, month, day):
    """
    Lista arquivos de um formato específico em uma pasta no Azure Data Lake.
    
    Args:
        service_client: Cliente DataLakeServiceClient
        tenant_id (str): UUID da tenant
        file_format (str): Formato do arquivo ('parquet' ou 'csv')
        year (str): Ano (YYYY)
        month (str): Mês (MM)
        day (str): Dia (DD)
    
    Returns:
        list: Lista de caminhos de arquivos do formato especificado
    """
    file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
    folder_path = f"tenant_{tenant_id}/tickets/format={file_format}/year={year}/month={month}/day={day}"
    
    extension = f".{file_format}"
    
    try:
        paths = file_system_client.get_paths(path=folder_path, recursive=False)
        files = [
            path.name for path in paths 
            if not path.is_directory and path.name.endswith(extension)
        ]
        return files
    except Exception as e:
        print(f"⚠️ Erro ao listar arquivos em {folder_path}: {e}")
        return []


def download_file(service_client, file_path, file_format):
    """
    Baixa arquivo do Azure Data Lake e retorna como DataFrame.
    
    Args:
        service_client: Cliente DataLakeServiceClient
        file_path (str): Caminho completo do arquivo no Data Lake
        file_format (str): Formato do arquivo ('parquet' ou 'csv')
    
    Returns:
        pd.DataFrame: Dados do arquivo
    """
    file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
    file_client = file_system_client.get_file_client(file_path)
    
    download = file_client.download_file()
    file_data = download.readall()
    
    if file_format == 'parquet':
        df = pd.read_parquet(io.BytesIO(file_data))
    elif file_format == 'csv':
        df = pd.read_csv(io.BytesIO(file_data))
    else:
        raise ValueError(f"Formato não suportado: {file_format}")
    
    return df


def upload_file(service_client, df, file_path, file_format, compression='snappy'):
    """
    Faz upload de DataFrame para o Azure Data Lake no formato especificado.
    
    Args:
        service_client: Cliente DataLakeServiceClient
        df (pd.DataFrame): DataFrame com os dados
        file_path (str): Caminho completo no Data Lake
        file_format (str): Formato do arquivo ('parquet' ou 'csv')
        compression (str): Compressão (usado apenas para Parquet)
    """
    file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
    
    buffer = io.BytesIO()
    
    if file_format == 'parquet':
        df.to_parquet(
            buffer, 
            engine='pyarrow', 
            compression=compression,
            index=False
        )
    elif file_format == 'csv':
        df.to_csv(
            buffer,
            index=False,
            date_format='%Y-%m-%d %H:%M:%S'
        )
    else:
        raise ValueError(f"Formato não suportado: {file_format}")
    
    buffer.seek(0)
    
    file_client = file_system_client.get_file_client(file_path)
    file_client.upload_data(buffer.read(), overwrite=True)
    
    file_size_kb = len(buffer.getvalue()) / 1024
    print(f"✅ Uploaded {len(df):,} rows ({file_size_kb:.2f} KB) to {file_path} [{file_format.upper()}]")


def delete_file(service_client, file_path):
    """
    Remove arquivo do Azure Data Lake.
    
    Args:
        service_client: Cliente DataLakeServiceClient
        file_path (str): Caminho completo do arquivo
    """
    file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
    file_client = file_system_client.get_file_client(file_path)
    file_client.delete_file()


def compact_tenant_data(service_client, tenant_id, file_format, target_date):
    """
    Compacta arquivos de um formato específico de uma tenant em uma data.
    
    Args:
        service_client: Cliente DataLakeServiceClient
        tenant_id (str): UUID da tenant
        file_format (str): Formato dos arquivos ('parquet' ou 'csv')
        target_date (datetime): Data alvo para compactação (D-1)
    
    Returns:
        dict: Estatísticas da compactação
    """
    year = target_date.strftime('%Y')
    month = target_date.strftime('%m')
    day = target_date.strftime('%d')
    
    print(f"\n{'='*80}")
    print(f"Compactando [{file_format.upper()}]: {tenant_id} - {year}/{month}/{day}")
    print(f"{'='*80}")
    
    # Listar arquivos do formato especificado
    files = list_files(service_client, tenant_id, file_format, year, month, day)
    
    if not files:
        print(f"ℹ️ Nenhum arquivo {file_format.upper()} encontrado para compactar")
        return {'status': 'skipped', 'reason': 'no_files', 'format': file_format}
    
    if len(files) == 1:
        print(f"ℹ️ Apenas 1 arquivo encontrado (já consolidado)")
        return {'status': 'skipped', 'reason': 'single_file', 'format': file_format}
    
    print(f"📂 Encontrados {len(files)} arquivos para compactar")
    
    # Download e consolidação
    dataframes = []
    total_rows_before = 0
    
    for file_path in files:
        try:
            df = download_file(service_client, file_path, file_format)
            dataframes.append(df)
            total_rows_before += len(df)
            print(f"   ✅ {file_path}: {len(df):,} rows")
        except Exception as e:
            print(f"   ❌ Erro ao baixar {file_path}: {e}")
            return {'status': 'error', 'reason': str(e), 'format': file_format}
    
    # Combinar DataFrames
    print(f"\n🔄 Consolidando {len(dataframes)} DataFrames...")
    df_consolidated = pd.concat(dataframes, ignore_index=True)
    
    # Remover duplicatas (se houver)
    rows_before_dedup = len(df_consolidated)
    df_consolidated = df_consolidated.drop_duplicates()
    rows_after_dedup = len(df_consolidated)
    duplicates_removed = rows_before_dedup - rows_after_dedup
    
    if duplicates_removed > 0:
        print(f"⚠️ Removidas {duplicates_removed:,} linhas duplicadas")
    
    print(f"📊 Total consolidado: {rows_after_dedup:,} rows")
    
    # Upload do arquivo consolidado
    extension = file_format
    consolidated_filename = f"tickets_{year}{month}{day}_consolidated.{extension}"
    consolidated_path = f"tenant_{tenant_id}/tickets/format={file_format}/year={year}/month={month}/day={day}/{consolidated_filename}"
    
    try:
        upload_file(service_client, df_consolidated, consolidated_path, file_format)
    except Exception as e:
        print(f"❌ Erro ao fazer upload do arquivo consolidado: {e}")
        return {'status': 'error', 'reason': str(e), 'format': file_format}
    
    # Validação: verificar que arquivo consolidado existe e tem dados
    try:
        df_validation = download_file(service_client, consolidated_path, file_format)
        if len(df_validation) != rows_after_dedup:
            print(f"❌ Validação falhou: arquivo consolidado tem {len(df_validation)} rows, esperado {rows_after_dedup}")
            return {'status': 'error', 'reason': 'validation_failed', 'format': file_format}
    except Exception as e:
        print(f"❌ Erro na validação: {e}")
        return {'status': 'error', 'reason': str(e), 'format': file_format}
    
    print("✅ Validação bem-sucedida")
    
    # Remover arquivos individuais
    print(f"\n🗑️ Removendo {len(files)} arquivos individuais...")
    files_deleted = 0
    
    for file_path in files:
        try:
            delete_file(service_client, file_path)
            files_deleted += 1
            print(f"   ✅ Removido: {file_path}")
        except Exception as e:
            print(f"   ⚠️ Erro ao remover {file_path}: {e}")
    
    print(f"✅ Compactação concluída: {len(files)} arquivos → 1 arquivo consolidado")
    
    return {
        'status': 'success',
        'format': file_format,
        'files_before': len(files),
        'files_after': 1,
        'rows_before': total_rows_before,
        'rows_after': rows_after_dedup,
        'duplicates_removed': duplicates_removed,
        'files_deleted': files_deleted
    }


def compact_datalake_files(**context):
    """
    Função principal para compactar arquivos do dia anterior (D-1).
    Processa todas as tenants com diretórios no Azure.
    """
    service_client = get_datalake_client()
    
    # Data alvo: ontem (D-1)
    execution_date = context['execution_date']
    target_date = execution_date - timedelta(days=1)
    
    print(f"{'='*80}")
    print(f"COMPACTAÇÃO DE ARQUIVOS AZURE DATA LAKE")
    print(f"{'='*80}")
    print(f"Data alvo: {target_date.strftime('%Y-%m-%d')} (D-1)")
    print(f"Execução: {execution_date.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{'='*80}\n")
    
    # Listar tenants
    azure_tenants = get_tenants_with_directories()
    
    if not azure_tenants:
        print("⚠️ Nenhum diretório tenant_* encontrado no Azure Data Lake")
        return
    
    print(f"📂 Tenants a processar: {len(azure_tenants)}\n")
    
    # Conexão PostgreSQL para obter configurações de formato
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    # Estatísticas globais
    total_tenants = len(azure_tenants)
    tenants_compacted = 0
    tenants_skipped = 0
    tenants_error = 0
    total_files_before = 0
    total_files_after = 0
    total_duplicates_removed = 0
    
    # Formatos a processar (hardcoded: parquet e csv)
    formats_to_process = ['parquet', 'csv']
    
    # Processar cada tenant
    for idx, tenant_id in enumerate(azure_tenants, 1):
        print(f"\n[{idx}/{total_tenants}] Processando tenant: {tenant_id}")
        
        # Obter formatos habilitados para a tenant
        try:
            query = f"""
                SELECT export_formats FROM {POSTGRES_SCHEMA}.tenant_export_settings 
                WHERE tenant_id = '{tenant_id}' AND active = true
            """
            result = hook.get_first(query)
            tenant_formats = result[0] if result and result[0] else ['parquet']
        except Exception as e:
            print(f"⚠️ Erro ao obter formatos da tenant, usando padrão (parquet): {e}")
            tenant_formats = ['parquet']
        
        print(f"📋 Formatos habilitados: {', '.join(tenant_formats)}")
        
        tenant_had_success = False
        
        # Processar cada formato habilitado para a tenant
        for file_format in tenant_formats:
            try:
                result = compact_tenant_data(service_client, tenant_id, file_format, target_date)
                
                if result['status'] == 'success':
                    tenant_had_success = True
                    total_files_before += result['files_before']
                    total_files_after += result['files_after']
                    total_duplicates_removed += result['duplicates_removed']
                    
                    print(f"   ✅ {file_format.upper()}: {result['files_before']} → {result['files_after']} arquivos")
                    
                elif result['status'] == 'skipped':
                    print(f"   ℹ️ {file_format.upper()}: Ignorado ({result['reason']})")
                    
                else:
                    print(f"   ❌ {file_format.upper()}: Erro ({result.get('reason', 'unknown')})")
                    
            except Exception as e:
                print(f"   ❌ Erro ao compactar {file_format.upper()}: {e}")
        
        # Contabilizar tenant
        if tenant_had_success:
            tenants_compacted += 1
        else:
            tenants_skipped += 1
    
    # Resumo final
    print(f"\n{'='*80}")
    print("📊 RESUMO DA COMPACTAÇÃO")
    print(f"{'='*80}")
    print(f"✅ Tenants compactados: {tenants_compacted}/{total_tenants}")
    print(f"ℹ️ Tenants ignorados: {tenants_skipped}")
    print(f"📂 Total de arquivos: {total_files_before} → {total_files_after}")
    if total_duplicates_removed > 0:
        print(f"⚠️ Duplicatas removidas: {total_duplicates_removed:,}")
    
    space_saved_pct = ((total_files_before - total_files_after) / total_files_before * 100) if total_files_before > 0 else 0
    print(f"💾 Redução de arquivos: {space_saved_pct:.1f}%")
    print(f"{'='*80}\n")
    
    # Registrar no banco (opcional)
    if tenants_compacted > 0:
        try:
            log_query = f"""
                INSERT INTO {POSTGRES_SCHEMA}.bi_refresh_log 
                (view_name, status, rows_affected, airflow_dag_id, export_type)
                VALUES (
                    'azure_datalake_compaction',
                    'SUCCESS',
                    {total_files_before - total_files_after},
                    'azure_datalake_compaction',
                    'COMPACTION'
                )
            """
            hook.run(log_query)
            print("✅ Log registrado no banco de dados")
        except Exception as e:
            print(f"⚠️ Erro ao registrar log no banco: {e}")
            log_query = f"""
                INSERT INTO {POSTGRES_SCHEMA}.bi_refresh_log 
                (view_name, status, rows_affected, notes, airflow_dag_id)
                VALUES (
                    'azure_datalake_compaction',
                    'SUCCESS',
                    {total_files_before - total_files_after},
                    'Compactados {tenants_compacted} tenants. Arquivos: {total_files_before} → {total_files_after}',
                    'azure_datalake_compaction'
                )
            """
            hook.run(log_query)
            print("✅ Log registrado no banco de dados")
        except Exception as e:
            print(f"⚠️ Erro ao registrar log no banco: {e}")


# Definir task
compact_files = PythonOperator(
    task_id='compact_datalake_files',
    python_callable=compact_datalake_files,
    dag=dag,
)

compact_files
