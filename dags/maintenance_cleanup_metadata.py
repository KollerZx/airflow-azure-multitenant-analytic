"""
DAG de Manutenção: Limpeza de Metadados Antigos do Airflow

Este DAG executa limpeza periódica dos metadados do banco de dados do Airflow,
removendo registros antigos de execuções, logs, jobs e outras tabelas.

Complementa o serviço airflow-log-cleaner que remove logs físicos.

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text

# Configuração padrão para todas as tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='maintenance_cleanup_metadata',
    default_args=default_args,
    description='Limpeza automática de metadados antigos do Airflow (database)',
    schedule='@weekly',  # Executa semanalmente aos domingos à meia-noite
    start_date=datetime(2026, 1, 1),
    catchup=False,
    is_paused_upon_creation=False, # DAG ativa por padrão
    tags=['maintenance', 'cleanup', 'database'],
    doc_md=__doc__,
) as dag:

    # Task 1: Limpar metadados do banco (logs, DAG runs, task instances, jobs)
    cleanup_database_metadata = BashOperator(
        task_id='cleanup_database_metadata',
        bash_command="""
        echo "🧹 Iniciando limpeza de metadados do Airflow..."
        
        # Calcular timestamp de 30 dias atrás
        CLEAN_TIMESTAMP=$(date -u -d '30 days ago' '+%Y-%m-%dT%H:%M:%S')
        
        echo "📅 Removendo registros anteriores a: ${CLEAN_TIMESTAMP}"
        
        # Executar limpeza do banco de dados
        airflow db clean \
          --tables log,job,dag_run,task_instance,task_fail,sla_miss,import_error,task_reschedule \
          --clean-before-timestamp "${CLEAN_TIMESTAMP}" \
          --verbose \
          --yes
        
        EXIT_CODE=$?
        
        if [ $EXIT_CODE -eq 0 ]; then
            echo "✅ Limpeza de metadados concluída com sucesso!"
        else
            echo "❌ Erro durante limpeza de metadados (código: ${EXIT_CODE})"
            exit $EXIT_CODE
        fi
        """,
        doc_md="""
        ### Limpeza de Metadados do Banco de Dados
        
        Remove registros antigos das seguintes tabelas:
        - **log**: Logs de tarefas armazenados no banco
        - **job**: Registros de jobs do scheduler/webserver
        - **dag_run**: Execuções de DAGs
        - **task_instance**: Instâncias de tarefas
        - **task_fail**: Histórico de falhas
        - **sla_miss**: Violações de SLA
        - **import_error**: Erros de importação de DAGs
        - **task_reschedule**: Tarefas reagendadas (sensores)
        
        **Retenção:** 30 dias (configurável)
        """,
    )

    # Task 2: Verificar uso de disco da pasta de logs
    check_logs_disk_usage = BashOperator(
        task_id='check_logs_disk_usage',
        bash_command="""
        echo "💾 Verificando uso de disco da pasta de logs..."
        echo ""
        
        LOGS_DIR="/opt/airflow/logs"
        
        # Tamanho total da pasta de logs
        TOTAL_SIZE=$(du -sh ${LOGS_DIR} | cut -f1)
        echo "📁 Tamanho total: ${TOTAL_SIZE}"
        echo ""
        
        # Top 10 DAGs que mais ocupam espaço
        echo "📊 Top 10 DAGs com mais logs:"
        du -sh ${LOGS_DIR}/dag_id=*/ 2>/dev/null | sort -rh | head -10 || echo "Nenhuma DAG encontrada"
        echo ""
        
        # Contagem de arquivos de log
        LOG_COUNT=$(find ${LOGS_DIR} -name "*.log" -type f | wc -l)
        echo "📄 Total de arquivos .log: ${LOG_COUNT}"
        echo ""
        
        # Verificar uso do filesystem
        echo "🖥️  Uso do filesystem:"
        df -h ${LOGS_DIR}
        echo ""
        
        # Alerta se uso > 80%
        USAGE_PERCENT=$(df ${LOGS_DIR} | tail -1 | awk '{print $5}' | sed 's/%//')
        if [ ${USAGE_PERCENT} -gt 80 ]; then
            echo "⚠️  ALERTA: Uso de disco acima de 80% (${USAGE_PERCENT}%)"
            echo "   Considere reduzir o período de retenção de logs"
        else
            echo "✅ Uso de disco OK (${USAGE_PERCENT}%)"
        fi
        """,
        doc_md="""
        ### Verificação de Uso de Disco
        
        Monitora o espaço ocupado pelos logs do Airflow:
        - Tamanho total da pasta `/opt/airflow/logs`
        - Top 10 DAGs com maior consumo de espaço
        - Contagem total de arquivos `.log`
        - Percentual de uso do filesystem
        - Alerta se uso > 80%
        """,
    )

    # Task 3: Gerar relatório de manutenção
    def generate_maintenance_report_func():
        """
        Gera relatório de manutenção com estatísticas do banco de dados.
        Usa SQLAlchemy para consultar o banco do Airflow diretamente.
        """
        from datetime import datetime
        
        print("📋 Gerando relatório de manutenção...")
        print("")
        print("==========================================")
        print("  RELATÓRIO DE MANUTENÇÃO DO AIRFLOW")
        print(f"  Data: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")
        print("==========================================")
        print("")
        
        try:
            # Obter conexão do banco de dados do Airflow
            airflow_db_conn = BaseHook.get_connection('airflow_db')
            
            # Se a conexão não existir, usar a conexão padrão do Airflow
            # (configurada em AIRFLOW__DATABASE__SQL_ALCHEMY_CONN)
            if not airflow_db_conn:
                from airflow.settings import Session
                engine = Session.get_bind()
            else:
                # Construir URI de conexão
                db_uri = airflow_db_conn.get_uri()
                engine = create_engine(db_uri)
        except Exception:
            # Fallback: usar engine do Airflow
            from airflow.settings import Session
            engine = Session.get_bind()
        
        print("📊 ESTATÍSTICAS DO BANCO DE DADOS:")
        print("")
        
        try:
            with engine.connect() as conn:
                # Query 1: DAG Runs dos últimos 30 dias
                result = conn.execute(text("""
                    SELECT COUNT(*) as count
                    FROM dag_run 
                    WHERE start_date >= NOW() - INTERVAL '30 days'
                """))
                dag_runs_count = result.fetchone()[0]
                print(f"  • DAG Runs (últimos 30 dias): {dag_runs_count}")
                
                # Query 2: Task Instances dos últimos 30 dias
                result = conn.execute(text("""
                    SELECT COUNT(*) as count
                    FROM task_instance 
                    WHERE start_date >= NOW() - INTERVAL '30 days'
                """))
                task_instances_count = result.fetchone()[0]
                print(f"  • Task Instances (últimos 30 dias): {task_instances_count}")
                
                # Query 3: Jobs Ativos
                result = conn.execute(text("""
                    SELECT COUNT(*) as count
                    FROM job 
                    WHERE end_date IS NULL OR end_date >= NOW() - INTERVAL '1 day'
                """))
                active_jobs_count = result.fetchone()[0]
                print(f"  • Jobs Ativos: {active_jobs_count}")
                
            print("")
            print("✅ Manutenção concluída com sucesso!")
            
        except Exception as e:
            print(f"⚠️  Erro ao consultar estatísticas: {str(e)}")
            print("✅ Manutenção de limpeza concluída (estatísticas indisponíveis)")
        
        print("")

    generate_maintenance_report = PythonOperator(
        task_id='generate_maintenance_report',
        python_callable=generate_maintenance_report_func,
        doc_md="""
        ### Relatório de Manutenção
        
        Gera resumo executivo da manutenção realizada:
        - Estatísticas de registros mantidos no banco (últimos 30 dias)
        - Status de jobs ativos do scheduler/webserver
        - Confirmação de conclusão
        
        Usa SQLAlchemy para consultar o banco do Airflow diretamente via Python.
        """,
    )

    # Definir dependências: execução sequencial
    cleanup_database_metadata >> check_logs_disk_usage >> generate_maintenance_report
