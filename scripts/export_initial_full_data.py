#!/usr/bin/env python3
"""
Script para PRIMEIRA EXPORTAÇÃO COMPLETA de dados para Azure Data Lake.

Este script exporta TODOS os dados (sem filtro de data) para popular o Data Lake
pela primeira vez. Após a execução inicial, o DAG do Airflow mantém apenas
exportações incrementais (últimas 24h).

Uso:
    # Exportar como Parquet (padrão)
    python scripts/export_initial_full_data.py
    
    # Exportar como CSV
    python scripts/export_initial_full_data.py --format csv
    
    # Exportar como Parquet explicitamente
    python scripts/export_initial_full_data.py --format parquet

"""

import os
import sys
import argparse
from datetime import datetime
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import io

# Carregar variáveis do .env
load_dotenv()

# Configurações Azure Data Lake
AZURE_STORAGE_ACCOUNT = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
AZURE_CONTAINER = os.getenv('AZURE_CONTAINER_NAME', 'tickets-data')
AZURE_TENANT_ID = os.getenv('AZURE_TENANT_ID')
AZURE_CLIENT_ID = os.getenv('AZURE_CLIENT_ID')
AZURE_CLIENT_SECRET = os.getenv('AZURE_CLIENT_SECRET')

# Configurações PostgreSQL
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')

# Data de particionamento (hoje)
NOW = datetime.now()
YEAR = NOW.strftime('%Y')
MONTH = NOW.strftime('%m')
DAY = NOW.strftime('%d')
TIMESTAMP = NOW.strftime('%Y%m%d_%H%M%S')


def validate_config():
    """Valida se todas as variáveis de ambiente necessárias estão configuradas."""
    missing_vars = []
    
    # Azure
    if not AZURE_STORAGE_ACCOUNT:
        missing_vars.append('AZURE_STORAGE_ACCOUNT_NAME')
    if not AZURE_TENANT_ID:
        missing_vars.append('AZURE_TENANT_ID')
    if not AZURE_CLIENT_ID:
        missing_vars.append('AZURE_CLIENT_ID')
    if not AZURE_CLIENT_SECRET:
        missing_vars.append('AZURE_CLIENT_SECRET')
    
    # PostgreSQL
    if not POSTGRES_HOST:
        missing_vars.append('POSTGRES_HOST')
    if not POSTGRES_DB:
        missing_vars.append('POSTGRES_DB')
    if not POSTGRES_USER:
        missing_vars.append('POSTGRES_USER')
    if not POSTGRES_PASSWORD:
        missing_vars.append('POSTGRES_PASSWORD')
    
    if missing_vars:
        print("❌ ERRO: Variáveis de ambiente faltando no .env:")
        for var in missing_vars:
            print(f"   - {var}")
        sys.exit(1)
    
    print("✅ Configurações validadas")


def get_datalake_client():
    """Cria cliente Azure Data Lake com autenticação via Service Principal."""
    credential = ClientSecretCredential(
        tenant_id=AZURE_TENANT_ID,
        client_id=AZURE_CLIENT_ID,
        client_secret=AZURE_CLIENT_SECRET
    )
    
    account_url = f"https://{AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net"
    service_client = DataLakeServiceClient(account_url=account_url, credential=credential)
    
    print(f"✅ Cliente Data Lake conectado: {account_url}")
    return service_client


def get_postgres_connection():
    """
    Cria uma connection string SQLAlchemy para PostgreSQL.
    Evita warnings do pandas ao usar pd.read_sql().
    
    Returns:
        str: Connection string no formato SQLAlchemy
    """
    connection_string = (
        f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )
    print(f"✅ Conectado ao PostgreSQL: {POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}")
    return connection_string


def get_tenants_with_folders():
    """
    Lista tenants (tenant_id) que já têm diretório criado no Azure Data Lake.
    Filtra apenas diretórios raiz no padrão tenant_<UUID> (ignora subdiretórios).
    
    Returns:
        set: Conjunto de UUIDs de tenants com diretórios
    """
    try:
        service_client = get_datalake_client()
        file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
        
        print("\n🔍 Listando tenants com diretórios no Azure Data Lake...")
        paths = file_system_client.get_paths()
        
        tenants = set()
        for path in paths:
            # Filtrar apenas diretórios raiz tenant_<uuid> (sem subdireórios)
            if path.name.startswith('tenant_') and path.is_directory:
                # Extrair apenas o nome do diretório raiz (ignorar subpaths como tenant_xxx/tickets)
                path_parts = path.name.split('/')
                if len(path_parts) == 1:  # Apenas diretório raiz (sem barra adicional)
                    # Extrair UUID do nome do diretório
                    tenant_id = path_parts[0].replace('tenant_', '')
                    tenants.add(tenant_id)
        
        if tenants:
            print(f"✅ Encontradas {len(tenants)} tenants com diretórios no Azure:")
            for tenant_id in sorted(tenants):
                print(f"   - tenant_{tenant_id}")
        else:
            print("⚠️  Nenhum diretório tenant_* encontrado no Azure Data Lake")
            print("💡 Dica: Execute o script generate_azure_service_principals.py primeiro")
        
        return tenants
        
    except Exception as e:
        print(f"⚠️  Erro ao listar diretórios do Azure: {e}")
        print("💡 Retornando conjunto vazio - nenhuma tenant será exportada")
        return set()


def upload_dataframe_to_datalake(df, file_path, service_client, file_format='parquet'):
    """
    Faz upload de DataFrame para o Azure Data Lake no formato especificado.
    
    Args:
        df (pd.DataFrame): DataFrame com os dados
        file_path (str): Caminho no Data Lake
        service_client: Cliente do Data Lake
        file_format (str): Formato do arquivo ('parquet' ou 'csv')
    """
    file_system_client = service_client.get_file_system_client(file_system=AZURE_CONTAINER)
    
    # Converter DataFrame para o formato especificado
    buffer = io.BytesIO()
    
    if file_format == 'parquet':
        df.to_parquet(
            buffer, 
            engine='pyarrow', 
            compression='snappy',
            index=False
        )
    elif file_format == 'csv':
        df.to_csv(
            buffer,
            sep=',',
            encoding='utf-8',
            index=False,
            date_format='%Y-%m-%d %H:%M:%S'
        )
    else:
        raise ValueError(f"Formato não suportado: {file_format}. Use 'parquet' ou 'csv'.")
    
    buffer.seek(0)
    
    # Upload para Data Lake
    file_client = file_system_client.get_file_client(file_path)
    file_client.upload_data(buffer.read(), overwrite=True)
    
    file_size_kb = len(buffer.getvalue()) / 1024
    print(f"   ✅ {len(df):,} rows ({file_size_kb:.2f} KB) [{file_format.upper()}] -> {file_path}")


def export_tickets_full(azure_tenants, file_format='parquet'):
    """Exporta TODOS os tickets (sem filtro de data) apenas para tenants com diretório no Azure.
    
    Args:
        azure_tenants (set): Conjunto de UUIDs de tenants com diretórios no Azure
        file_format (str): Formato do arquivo ('parquet' ou 'csv')
    """
    print("\n" + "="*80)
    print(f"📊 EXPORTANDO TICKETS (bi_tickets_flat) - TODOS OS DADOS - Formato: {file_format.upper()}")
    print("="*80)
    
    if not azure_tenants:
        print("⚠️  Nenhuma tenant com diretório no Azure - pulando exportação")
        return
    
    conn = get_postgres_connection()
    service_client = get_datalake_client()
    
    # Query SEM filtro de data (exportação completa)
    # Usando apenas campos disponíveis na view simplificada
    query = f"""
        SELECT 
            *
        FROM {POSTGRES_SCHEMA}.bi_tickets_flat
        ORDER BY tenant_id, id
    """
    
    print("🔍 Buscando TODOS os tickets do banco...")
    df = pd.read_sql(query, conn)
    
    if df.empty:
        print("⚠️ Nenhum ticket encontrado")
        return
    
    print(f"✅ Total de tickets: {len(df):,}")
    
    # Identificar tenants no banco vs Azure
    tenants_db = set(df['tenant_id'].unique())
    tenants_only_azure = azure_tenants - tenants_db
    tenants_only_db = tenants_db - azure_tenants
    tenants_both = tenants_db & azure_tenants
    
    # Relatório de correspondência
    print(f"\n📊 Relatório de Tenants:")
    print(f"   🔵 No Azure E no Banco: {len(tenants_both)}")
    print(f"   🟡 Apenas no Azure (SEM dados locais): {len(tenants_only_azure)}")
    print(f"   🟠 Apenas no Banco (SEM diretório Azure): {len(tenants_only_db)}")
    
    # Avisar sobre tenants no Azure sem dados locais (provável outro ambiente)
    if tenants_only_azure:
        print(f"\n⚠️  {len(tenants_only_azure)} tenant(s) com diretório no Azure mas SEM dados no banco local:")
        print("   (Provavelmente de outro ambiente - serão ignoradas)")
        for tenant_id in sorted(tenants_only_azure):
            print(f"   - tenant_{tenant_id}/")
    
    # Avisar sobre tenants no banco sem diretório Azure
    if tenants_only_db:
        print(f"\n⚠️  {len(tenants_only_db)} tenant(s) no banco mas SEM diretório no Azure:")
        print("   (Execute generate_azure_service_principals.py para criar diretórios)")
        for tenant_id in sorted(tenants_only_db):
            count = len(df[df['tenant_id'] == tenant_id])
            print(f"   - {tenant_id}: {count:,} tickets")
    
    # Filtrar apenas tenants que têm diretório no Azure E dados no banco
    df_filtered = df[df['tenant_id'].isin(azure_tenants)]
    
    if df_filtered.empty:
        print("\n❌ Nenhum ticket para exportar (tenants no Azure não têm dados no banco local)")
        return
    
    # Agrupar por tenant_id e exportar separadamente
    total_tenants = df_filtered['tenant_id'].nunique()
    print(f"\n📊 Total de tenants a exportar: {total_tenants}")
    
    for idx, (tenant_id, group_df) in enumerate(df_filtered.groupby('tenant_id'), 1):
        file_path = (
            f"tenant_{tenant_id}/tickets/"
            f"format={file_format}/"  # Particionamento por formato
            f"year={YEAR}/month={MONTH}/day={DAY}/"
            f"tickets_full_{TIMESTAMP}.{file_format}"
        )
        
        print(f"[{idx}/{total_tenants}] Tenant {tenant_id}: {len(group_df):,} tickets")
        upload_dataframe_to_datalake(group_df, file_path, service_client, file_format)
    
    print(f"\n✅ TICKETS: {len(df_filtered):,} registros exportados para {total_tenants} tenants")


def main():
    """Função principal - executa exportação completa de todas as tabelas."""
    # Parser de argumentos CLI
    parser = argparse.ArgumentParser(
        description='Exportação inicial completa de dados para Azure Data Lake',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  %(prog)s                      # Exportar como Parquet (padrão)
  %(prog)s --format csv         # Exportar como CSV
  %(prog)s --format parquet     # Exportar como Parquet explicitamente
        """
    )
    parser.add_argument(
        '--format',
        type=str,
        choices=['parquet', 'csv'],
        default='parquet',
        help='Formato de exportação: parquet (padrão, mais rápido) ou csv (compatível com Excel)'
    )
    
    args = parser.parse_args()
    file_format = args.format
    
    print("\n" + "🚀"*40)
    print("EXPORTAÇÃO INICIAL COMPLETA PARA AZURE DATA LAKE")
    print("🚀"*40)
    print(f"\n📅 Data de execução: {NOW.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📁 Particionamento: year={YEAR}/month={MONTH}/day={DAY}/")
    print(f"📦 Formato de exportação: {file_format.upper()}")
    print(f"\n⚠️  ATENÇÃO: Esta exportação inclui TODOS os dados históricos!")
    print("⚠️  Após esta execução, o DAG do Airflow manterá apenas dados incrementais.")
    print("\n📊 Views exportadas: bi_tickets_flat (simplificada - ~60% menos campos)\n")
    
    # Validar configurações
    validate_config()
    
    try:
        # Listar tenants com diretórios no Azure Data Lake
        azure_tenants = get_tenants_with_folders()
        
        if not azure_tenants:
            print("\n❌ Nenhuma tenant com diretório no Azure Data Lake encontrada.")
            print("💡 Execute o script generate_azure_service_principals.py primeiro para criar os diretórios.")
            sys.exit(1)
        
        # Exportar tickets (apenas para tenants com diretório no Azure)
        export_tickets_full(azure_tenants, file_format)
        
        print("\n" + "="*80)
        print(f"✅ EXPORTAÇÃO COMPLETA FINALIZADA COM SUCESSO! (Formato: {file_format.upper()})")
        print("="*80)
        print("\n💡 Próximos passos:")
        print("   1. Verificar dados no Azure Portal")
        print("   2. Testar acesso com Service Principal do cliente")
        print("   3. Ativar DAG 'export_tickets_to_azure_datalake' no Airflow")
        print("   4. DAG manterá exportações incrementais (últimas 24h) a cada 15 min\n")
        
    except Exception as e:
        print(f"\n❌ ERRO durante exportação: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
