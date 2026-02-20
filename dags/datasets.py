"""
Definições de Datasets para dependências entre DAGs.

Dataset-aware Scheduling (Airflow 2.4+) permite criar dependências declarativas
entre DAGs através de datasets. Uma DAG que produz (outlets) um dataset dispara
automaticamente outras DAGs que consomem (schedule) esse dataset.

"""

from airflow.datasets import Dataset
import os

# Obter schema do PostgreSQL do ambiente
POSTGRES_SCHEMA = os.getenv('POSTGRES_SCHEMA', 'public')

# Dataset representando a view materializada bi_tickets_flat
# URI Pattern: postgres://<schema>/<view_name>
VIEW_TICKETS_DATASET = Dataset(f"postgres://{POSTGRES_SCHEMA}/bi_tickets_flat")

"""
Como usar:

1. DAG que ATUALIZA a view (Producer):
   from datasets import VIEW_TICKETS_DATASET
   
   refresh_task = PostgresOperator(
       task_id='refresh_view',
       sql='REFRESH MATERIALIZED VIEW bi_tickets_flat',
       outlets=[VIEW_TICKETS_DATASET],  # Declara que produz o dataset
   )

2. DAG que DEPENDE da view atualizada (Consumer):
   from datasets import VIEW_TICKETS_DATASET
   
   with DAG(
       'my_export_dag',
       schedule=[VIEW_TICKETS_DATASET],  # Executa quando dataset é atualizado
       ...
   ):
       export_task = PythonOperator(...)

Benefícios:
- Ordem de execução garantida (consumer só executa após producer)
- Disparo imediato (não espera próximo schedule_interval)
- Desacoplamento (DAGs permanecem independentes)
- Visibilidade na UI (Airflow mostra grafo de dependências)
"""
