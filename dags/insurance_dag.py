# dags/insurance_dag.py
"""
ETL DAG для insurance.csv (Kaggle download)
"""

from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import os
import pandas as pd
import numpy as np
from zipfile import ZipFile

default_args = {
    'owner': 'student',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 0,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'insurance_analysis',
    default_args=default_args,
    description='ETL для страховых выплат (скачивание с Kaggle)',
    schedule_interval=None,
    catchup=False,
    tags=['etl', 'kaggle', 'insurance']
)


def extract(**context):
    """Скачиваем insurance.csv с Kaggle"""
    from kaggle import api

    dataset = "mirichoi0218/insurance"
    data_dir = os.path.join(os.path.dirname(__file__), 'data')
    os.makedirs(data_dir, exist_ok=True)

    # Проверяем kaggle.json
    kaggle_path = os.path.expanduser("~/.kaggle/kaggle.json")
    if not os.path.exists(kaggle_path):
        raise FileNotFoundError(
            f"Файл {kaggle_path} не найден. "
            f"Скачай свой API-ключ с Kaggle и помести его в ~/.kaggle/kaggle.json"
        )

    # Скачиваем датасет
    print("Downloading dataset from Kaggle...")
    api.dataset_download_files(dataset, path=data_dir, unzip=True)
    csv_path = os.path.join(data_dir, 'insurance.csv')

    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Не найден файл {csv_path} после распаковки")

    context['task_instance'].xcom_push(key='csv_path', value=csv_path)
    print(f"CSV успешно загружен: {csv_path}")


def load_raw(**context):
    """Загружаем CSV в таблицу raw_insurance"""
    csv_path = context['task_instance'].xcom_pull(key='csv_path', task_ids='extract')
    df = pd.read_csv(csv_path)

    hook = PostgresHook(postgres_conn_id='analytics_postgres')
    hook.run("DROP TABLE IF EXISTS raw_insurance;")
    hook.run("""
        CREATE TABLE raw_insurance (
            id SERIAL PRIMARY KEY,
            age INTEGER,
            sex TEXT,
            bmi FLOAT,
            children INTEGER,
            smoker TEXT,
            region TEXT,
            charges FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    rows = [tuple(x) for x in df[['age','sex','bmi','children','smoker','region','charges']].to_numpy()]
    hook.insert_rows('raw_insurance', rows, target_fields=['age','sex','bmi','children','smoker','region','charges'])
    print(f"Loaded {len(rows)} rows into raw_insurance")


def transform(**context):
    """Создаём stg_insurance"""
    hook = PostgresHook(postgres_conn_id='analytics_postgres')
    df = hook.get_pandas_df("SELECT age, sex, bmi, children, smoker, region, charges FROM raw_insurance;")

    df['age_group'] = pd.cut(df['age'], bins=[0,30,50,200], labels=['до 30','30-50','50+'], right=False)
    df['bmi_category'] = pd.cut(df['bmi'], bins=[0,18.5,24.9,29.9,100], labels=['Underweight','Normal','Overweight','Obese'], right=False)
    df['is_smoker'] = np.where(df['smoker'].str.lower() == 'yes', 1, 0)

    df_stage = df[['age_group','sex','bmi_category','is_smoker','charges']].copy()

    hook.run("DROP TABLE IF EXISTS stg_insurance;")
    hook.run("""
        CREATE TABLE stg_insurance (
            age_group TEXT,
            sex TEXT,
            bmi_category TEXT,
            is_smoker INTEGER,
            charges FLOAT
        );
    """)
    rows = [tuple(x) for x in df_stage.to_numpy()]
    hook.insert_rows('stg_insurance', rows, target_fields=['age_group','sex','bmi_category','is_smoker','charges'])
    print(f"Loaded {len(rows)} rows into stg_insurance")


t_extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

t_load_raw = PythonOperator(
    task_id='load_raw',
    python_callable=load_raw,
    dag=dag
)

t_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

t_create_datamart = PostgresOperator(
    task_id='create_datamart',
    postgres_conn_id='analytics_postgres',
    sql='medical_variant_10.sql',
    dag=dag
)

t_extract >> t_load_raw >> t_transform >> t_create_datamart
