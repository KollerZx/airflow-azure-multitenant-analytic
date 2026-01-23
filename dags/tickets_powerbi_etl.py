"""
DAG: tickets_powerbi_etl
Descrição: Pipeline ETL para carregar dados de tickets no Power BI
Frequência: A cada 15 minutos (configurável)
"""

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import logging
import os

# Nome da conexão PostgreSQL (configurável via variável de ambiente)
POSTGRES_CONN_ID = os.getenv('POSTGRES_CONN_ID', 'postgres_prod')

# Obter schema do ambiente (padrão: public)
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')

# Configurações padrão para todas as tasks
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['your-email@example.com'],  # Configurar email para alertas
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(minutes=30),
}

def log_view_stats(view_name: str, **context):
    """
    Função Python para registrar estatísticas do refresh da view
    """
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    schema = os.getenv('POSTGRES_SCHEMA', 'public')
    
    try:
        # Contar registros na view
        count_query = f"SELECT COUNT(*) as count FROM {schema}.{view_name};"
        result = postgres_hook.get_first(count_query)
        row_count = result[0] if result else 0
        
        # Pegar informações da execução do Airflow
        execution_date = context.get('execution_date')
        dag_run = context.get('dag_run')
        task_instance = context.get('task_instance')
        
        # Calcular duração (estimativa)
        start_time = task_instance.start_date
        duration = (datetime.now(start_time.tzinfo) - start_time).total_seconds()
        
        # Inserir log
        log_query = f"""
            INSERT INTO {schema}.bi_refresh_log 
            (view_name, status, rows_affected, duration_seconds, airflow_dag_id, airflow_task_id, airflow_execution_date)
            VALUES (
                '{view_name}',
                'SUCCESS',
                {row_count},
                {duration},
                '{dag_run.dag_id}',
                '{task_instance.task_id}',
                '{execution_date}'
            );
        """
        postgres_hook.run(log_query)
        
        logging.info(f"✅ View {view_name} refreshed successfully: {row_count} rows in {duration:.2f}s")
        
    except Exception as e:
        logging.error(f"❌ Error logging stats for {view_name}: {str(e)}")
        raise

def check_tables_exist(**context):
    """
    Verifica se as views materializadas existem antes de tentar refresh
    """
    postgres_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    schema = os.getenv('POSTGRES_SCHEMA', 'public')
    
    views = ['bi_tickets_flat']
    missing_views = []
    
    for view_name in views:
        check_query = f"""
            SELECT EXISTS (
                SELECT FROM pg_matviews 
                WHERE schemaname = '{schema}' AND matviewname = '{view_name}'
            );
        """
        result = postgres_hook.get_first(check_query)
        exists = result[0] if result else False
        
        if not exists:
            missing_views.append(view_name)
            logging.warning(f"⚠️ View materializada {schema}.{view_name} não existe!")
    
    if missing_views:
        raise ValueError(
            f"Views materializadas ausentes no schema '{schema}': {', '.join(missing_views)}. "
            "Execute os scripts SQL de criação primeiro!"
        )
    
    logging.info("✅ Todas as views materializadas existem")

# Definição da DAG
with DAG(
    dag_id='tickets_powerbi_etl',
    default_args=default_args,
    description='ETL para carregar dados de tickets no Power BI via views materializadas',
    schedule_interval='*/15 * * * *',  # A cada 15 minutos
    start_date=datetime(datetime.now().year, 1, 1),
    catchup=False,  # Não executar backfill de datas passadas
    is_paused_upon_creation=False,  # DAG ativa por padrão
    default_view='grid',  # Visualização padrão na UI
    tags=['powerbi', 'tickets', 'bi', 'etl'],
    max_active_runs=1,  # Apenas uma execução por vez
) as dag:

    # ==========================================================================
    # TASK 0: Verificar se as views existem
    # ==========================================================================
    check_views = PythonOperator(
        task_id='check_views_exist',
        python_callable=check_tables_exist,
        provide_context=True,
    )

    # ==========================================================================
    # TASK 1: Refresh da view principal de tickets
    # ==========================================================================
    refresh_tickets = PostgresOperator(
        task_id='refresh_bi_tickets_flat',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            -- Configurar search_path
            SET search_path TO {POSTGRES_SCHEMA}, public;
            
            -- Refresh CONCURRENTLY permite leituras durante a atualização
            REFRESH MATERIALIZED VIEW CONCURRENTLY {POSTGRES_SCHEMA}.bi_tickets_flat;
        """,
    )
    
    log_tickets_stats = PythonOperator(
        task_id='log_tickets_stats',
        python_callable=log_view_stats,
        op_kwargs={'view_name': 'bi_tickets_flat'},
        provide_context=True,
    )

    # ==========================================================================
    # TASK 2: Analyze tables para otimizar planos de query
    # ==========================================================================
    analyze_tables = PostgresOperator(
        task_id='analyze_bi_tables',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            SET search_path TO {POSTGRES_SCHEMA}, public;
            
            -- Atualiza estatísticas para o query planner
            ANALYZE {POSTGRES_SCHEMA}.bi_tickets_flat;
        """,
    )

    # ==========================================================================
    # TASK 3: Log de conclusão geral
    # ==========================================================================
    log_completion = PostgresOperator(
        task_id='log_etl_completion',
        postgres_conn_id=POSTGRES_CONN_ID,
        sql=f"""
            SET search_path TO {POSTGRES_SCHEMA}, public;
            
            INSERT INTO {POSTGRES_SCHEMA}.bi_refresh_log (view_name, status, airflow_dag_id)
            VALUES ('ALL_VIEWS', 'SUCCESS', 'tickets_powerbi_etl');
        """,
    )

    # ==========================================================================
    # DEFINIÇÃO DE DEPENDÊNCIAS (ordem de execução)
    # ==========================================================================
    
    # Primeiro verifica se as views existem
    check_views >> [refresh_tickets]
    
    # Cada refresh é seguido pelo seu respectivo log
    refresh_tickets >> log_tickets_stats
    
    # Após todos os logs, executa o analyze
    [log_tickets_stats] >> analyze_tables
    
    # Finalmente, registra conclusão geral
    analyze_tables >> log_completion
    refresh_tickets
    
    # Cada refresh é seguido pelo seu respectivo log
    refresh_tickets >> log_tickets_stats
    
    # Após o log, executa o analyze
    log_tickets_stats