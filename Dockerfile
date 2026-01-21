FROM apache/airflow:slim-2.8.1-python3.11

# Instalar ferramentas sistema (como root)
USER root
RUN apt-get update && apt-get install -y postgresql-client && apt-get clean

# Voltar para usuário airflow
USER airflow

# Instalar dependências Python para Azure Data Lake, Parquet e PostgreSQL
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres==5.14.0 \
    apache-airflow-providers-common-sql==1.20.0 \
    apache-airflow-providers-microsoft-azure==11.1.0 \
    azure-storage-file-datalake==12.23.0 \
    psycopg2-binary==2.9.11 \
    azure-identity==1.25.1 \
    "numpy>=2.0.0,<3.0.0" \
    pyarrow==17.0.0 \
    pandas==2.3.3
