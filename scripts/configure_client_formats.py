#!/usr/bin/env python3
"""
Script: Configuração de Formatos de Exportação por Cliente
Arquivo: configure_client_formats.py
Descrição: Interface interativa para habilitar/desabilitar CSV por tenant

"""

import os
import sys
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import RealDictCursor

# Carregar variáveis de ambiente
load_dotenv()

# Configurações PostgreSQL
POSTGRES_HOST = os.getenv('POSTGRES_HOST', 'localhost')
POSTGRES_PORT = os.getenv('POSTGRES_PORT', '5432')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'postgres')
POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')

# Cores para output
class Colors:
    GREEN = '\033[0;32m'
    BLUE = '\033[0;34m'
    YELLOW = '\033[1;33m'
    RED = '\033[0;31m'
    CYAN = '\033[0;36m'
    NC = '\033[0m'  # No Color


def get_db_connection():
    """Cria conexão com PostgreSQL."""
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        return conn
    except Exception as e:
        print(f"{Colors.RED}❌ Erro ao conectar ao PostgreSQL: {e}{Colors.NC}")
        sys.exit(1)


def list_tenants():
    """Lista todas as tenants com suas configurações de formato."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = f"""
        SELECT 
            c.id as tenant_id,
            c.name as tenant_name,
            COALESCE(ces.export_formats, ARRAY['parquet']) as formats,
            ces.csv_delimiter,
            ces.csv_encoding,
            ces.active,
            ces.created_at,
            ces.updated_at
        FROM {POSTGRES_SCHEMA}.tenants c
        LEFT JOIN {POSTGRES_SCHEMA}.tenant_export_settings ces ON c.id = ces.tenant_id
        ORDER BY c.name
    """
    
    cursor.execute(query)
    tenants = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    return tenants


def display_tenants(tenants):
    """Exibe lista de tenants com formatação."""
    print(f"\n{Colors.CYAN}{'='*100}{Colors.NC}")
    print(f"{Colors.CYAN}TENANTS E CONFIGURAÇÕES DE FORMATO{Colors.NC}")
    print(f"{Colors.CYAN}{'='*100}{Colors.NC}\n")
    
    print(f"{'#':<4} {'Nome do Tenant':<30} {'Formatos Habilitados':<25} {'CSV Config':<20} {'Status':<10}")
    print(f"{'-'*100}")
    
    for idx, tenant in enumerate(tenants, 1):
        formats_str = ', '.join(tenant['formats'])
        
        if 'csv' in tenant['formats']:
            csv_config = f"{tenant['csv_delimiter'] or ','} | {tenant['csv_encoding'] or 'utf-8'}"
            format_color = Colors.GREEN
        else:
            csv_config = "-"
            format_color = Colors.YELLOW
        
        status = "✅ Ativo" if tenant['active'] or tenant['active'] is None else "❌ Inativo"
        
        print(f"{idx:<4} {tenant['tenant_name'][:28]:<30} {format_color}{formats_str:<25}{Colors.NC} {csv_config:<20} {status:<10}")
    
    print(f"{'-'*100}\n")


def enable_csv_for_tenant(tenant_id, delimiter=',', encoding='utf-8'):
    """Habilita exportação CSV para uma tenant."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = f"""
        SELECT {POSTGRES_SCHEMA}.enable_csv_export(
            '{tenant_id}'::TEXT,
            '{delimiter}'::VARCHAR(1),
            '{encoding}'::VARCHAR(20)
        )
    """
    
    try:
        cursor.execute(query)
        conn.commit()
        result = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return result
    except Exception as e:
        print(f"{Colors.RED}❌ Erro ao habilitar CSV: {e}{Colors.NC}")
        conn.rollback()
        cursor.close()
        conn.close()
        return False


def disable_csv_for_tenant(tenant_id):
    """Desabilita exportação CSV para uma tenant."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    query = f"""
        SELECT {POSTGRES_SCHEMA}.disable_csv_export('{tenant_id}'::TEXT)
    """
    
    try:
        cursor.execute(query)
        conn.commit()
        result = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return result
    except Exception as e:
        print(f"{Colors.RED}❌ Erro ao desabilitar CSV: {e}{Colors.NC}")
        conn.rollback()
        cursor.close()
        conn.close()
        return False


def show_tenant_details(tenant_id):
    """Exibe detalhes de configuração de uma tenant."""
    conn = get_db_connection()
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    
    query = f"""
        SELECT 
            c.id,
            c.name,
            COALESCE(ces.export_formats, ARRAY['parquet']) as formats,
            ces.csv_delimiter,
            ces.csv_encoding,
            ces.csv_quoting,
            ces.csv_date_format,
            ces.parquet_compression,
            ces.parquet_engine,
            ces.active,
            ces.notes,
            ces.created_at,
            ces.updated_at
        FROM {POSTGRES_SCHEMA}.tenants c
        LEFT JOIN {POSTGRES_SCHEMA}.tenant_export_settings ces ON c.id = ces.tenant_id
        WHERE c.id = '{tenant_id}'
    """
    
    cursor.execute(query)
    tenant = cursor.fetchone()
    
    cursor.close()
    conn.close()
    
    if not tenant:
        print(f"{Colors.RED}❌ Tenant não encontrado{Colors.NC}")
        return
    
    print(f"\n{Colors.CYAN}{'='*80}{Colors.NC}")
    print(f"{Colors.CYAN}DETALHES DA TENANT{Colors.NC}")
    print(f"{Colors.CYAN}{'='*80}{Colors.NC}\n")
    
    print(f"{Colors.BLUE}Nome:{Colors.NC} {tenant['name']}")
    print(f"{Colors.BLUE}ID:{Colors.NC} {tenant['id']}")
    print(f"{Colors.BLUE}Status:{Colors.NC} {'✅ Ativo' if tenant['active'] or tenant['active'] is None else '❌ Inativo'}")
    
    print(f"\n{Colors.YELLOW}📋 FORMATOS HABILITADOS:{Colors.NC}")
    for fmt in tenant['formats']:
        print(f"   • {fmt.upper()}")
    
    if 'csv' in tenant['formats']:
        print(f"\n{Colors.YELLOW}📄 CONFIGURAÇÕES CSV:{Colors.NC}")
        print(f"   Delimitador: {tenant['csv_delimiter'] or ','}")
        print(f"   Encoding: {tenant['csv_encoding'] or 'utf-8'}")
        print(f"   Quoting: {tenant['csv_quoting'] or 'minimal'}")
        print(f"   Formato de Data: {tenant['csv_date_format'] or 'YYYY-MM-DD HH24:MI:SS'}")
    
    if 'parquet' in tenant['formats']:
        print(f"\n{Colors.YELLOW}📦 CONFIGURAÇÕES PARQUET:{Colors.NC}")
        print(f"   Compressão: {tenant['parquet_compression'] or 'snappy'}")
        print(f"   Engine: {tenant['parquet_engine'] or 'pyarrow'}")
    
    if tenant['notes']:
        print(f"\n{Colors.YELLOW}📝 OBSERVAÇÕES:{Colors.NC}")
        print(f"   {tenant['notes']}")
    
    print(f"\n{Colors.CYAN}{'='*80}{Colors.NC}\n")


def interactive_menu():
    """Menu interativo principal."""
    while True:
        print(f"\n{Colors.BLUE}{'='*80}{Colors.NC}")
        print(f"{Colors.BLUE}CONFIGURAÇÃO DE FORMATOS DE EXPORTAÇÃO{Colors.NC}")
        print(f"{Colors.BLUE}{'='*80}{Colors.NC}\n")
        
        print("1. Listar todos os tenants")
        print("2. Habilitar CSV para um tenant")
        print("3. Desabilitar CSV para um tenant")
        print("4. Ver detalhes de um tenant")
        print("5. Habilitar CSV para TODOS os tenants (massa)")
        print("0. Sair")
        
        choice = input(f"\n{Colors.CYAN}Escolha uma opção: {Colors.NC}").strip()
        
        if choice == '1':
            tenants = list_tenants()
            display_tenants(tenants)
            
        elif choice == '2':
            tenants = list_tenants()
            display_tenants(tenants)
            
            tenant_num = input(f"\n{Colors.CYAN}Digite o número do tenant (ou ID): {Colors.NC}").strip()
            
            # Tentar como número ou UUID
            try:
                tenant_idx = int(tenant_num) - 1
                if 0 <= tenant_idx < len(tenants):
                    tenant_id = tenants[tenant_idx]['tenant_id']
                else:
                    print(f"{Colors.RED}❌ Número inválido{Colors.NC}")
                    continue
            except ValueError:
                tenant_id = tenant_num
            
            # Perguntar delimitador
            delimiter = input(f"{Colors.CYAN}Delimitador CSV (, ou ;) [,]: {Colors.NC}").strip() or ','
            if delimiter not in [',', ';', '|', '\t']:
                print(f"{Colors.RED}❌ Delimitador inválido. Usando '{','}{Colors.NC}")
                delimiter = ','
            
            # Perguntar encoding
            encoding = input(f"{Colors.CYAN}Encoding (utf-8 ou latin1) [utf-8]: {Colors.NC}").strip() or 'utf-8'
            if encoding not in ['utf-8', 'utf-8-sig', 'latin1', 'iso-8859-1']:
                print(f"{Colors.RED}❌ Encoding inválido. Usando 'utf-8'{Colors.NC}")
                encoding = 'utf-8'
            
            # Confirmar
            confirm = input(f"{Colors.YELLOW}Confirmar habilitação de CSV? (s/n): {Colors.NC}").strip().lower()
            if confirm == 's':
                if enable_csv_for_tenant(tenant_id, delimiter, encoding):
                    print(f"{Colors.GREEN}✅ CSV habilitado com sucesso!{Colors.NC}")
                else:
                    print(f"{Colors.RED}❌ Erro ao habilitar CSV{Colors.NC}")
            else:
                print(f"{Colors.YELLOW}Operação cancelada{Colors.NC}")
        
        elif choice == '3':
            tenants = list_tenants()
            display_tenants(tenants)
            
            tenant_num = input(f"\n{Colors.CYAN}Digite o número do tenant (ou ID): {Colors.NC}").strip()
            
            # Tentar como número ou UUID
            try:
                tenant_idx = int(tenant_num) - 1
                if 0 <= tenant_idx < len(tenants):
                    tenant_id = tenants[tenant_idx]['tenant_id']
                else:
                    print(f"{Colors.RED}❌ Número inválido{Colors.NC}")
                    continue
            except ValueError:
                tenant_id = tenant_num
            
            # Confirmar
            confirm = input(f"{Colors.YELLOW}Confirmar desabilitação de CSV? (s/n): {Colors.NC}").strip().lower()
            if confirm == 's':
                if disable_csv_for_tenant(tenant_id):
                    print(f"{Colors.GREEN}✅ CSV desabilitado com sucesso!{Colors.NC}")
                else:
                    print(f"{Colors.RED}❌ Erro ao desabilitar CSV{Colors.NC}")
            else:
                print(f"{Colors.YELLOW}Operação cancelada{Colors.NC}")
        
        elif choice == '4':
            tenants = list_tenants()
            display_tenants(tenants)
            
            tenant_num = input(f"\n{Colors.CYAN}Digite o número do tenant (ou ID): {Colors.NC}").strip()
            
            # Tentar como número ou UUID
            try:
                tenant_idx = int(tenant_num) - 1
                if 0 <= tenant_idx < len(tenants):
                    tenant_id = tenants[tenant_idx]['tenant_id']
                else:
                    print(f"{Colors.RED}❌ Número inválido{Colors.NC}")
                    continue
            except ValueError:
                tenant_id = tenant_num
            
            show_tenant_details(tenant_id)
        
        elif choice == '5':
            tenants = list_tenants()
            
            print(f"\n{Colors.YELLOW}⚠️ ATENÇÃO: Esta operação habilitará CSV para TODAS os {len(tenants)} tenants!{Colors.NC}")
            confirm = input(f"{Colors.RED}Tem certeza? (digite 'SIM' para confirmar): {Colors.NC}").strip()
            
            if confirm == 'SIM':
                success_count = 0
                error_count = 0
                
                for tenant in tenants:
                    if enable_csv_for_tenant(tenant['tenant_id']):
                        success_count += 1
                        print(f"   ✅ {tenant['tenant_name']}")
                    else:
                        error_count += 1
                        print(f"   ❌ {tenant['tenant_name']}")
                
                print(f"\n{Colors.GREEN}✅ CSV habilitado para {success_count} tenants{Colors.NC}")
                if error_count > 0:
                    print(f"{Colors.RED}❌ Erro em {error_count} tenants{Colors.NC}")
            else:
                print(f"{Colors.YELLOW}Operação cancelada{Colors.NC}")
        
        elif choice == '0':
            print(f"\n{Colors.GREEN}👋 Até logo!{Colors.NC}\n")
            break
        
        else:
            print(f"{Colors.RED}❌ Opção inválida{Colors.NC}")


def main():
    """Função principal."""
    print(f"{Colors.BLUE}")
    print("=" * 80)
    print("   CONFIGURADOR DE FORMATOS DE EXPORTAÇÃO")
    print("   Airflow → PostgreSQL → Azure Data Lake")
    print("=" * 80)
    print(f"{Colors.NC}")
    
    # Verificar conexão
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()[0]
        print(f"{Colors.GREEN}✅ Conectado ao PostgreSQL: {version.split(',')[0]}{Colors.NC}")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"{Colors.RED}❌ Erro de conexão: {e}{Colors.NC}")
        sys.exit(1)
    
    # Menu interativo
    interactive_menu()


if __name__ == "__main__":
    main()
