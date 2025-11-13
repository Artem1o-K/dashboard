"""
DAG-тестер для проверки соединения с Kaggle.
Скачивает один датасет для проверки аутентификации.
"""
from __future__ import annotations

from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG

def simple_kaggle_download():
    """
    Минимальная функция для проверки скачивания с помощью kagglehub.
    Все импорты и логика находятся внутри, чтобы избежать проблем с парсингом DAG.
    """
    import kagglehub
    import os
    
    print("--- ТЕСТ KAGGLE: Внутри Python-функции. ---")
    print("--- ТЕСТ KAGGLE: Импортируем kagglehub. ---")
    
    dataset_name = "harshitagpt/us-presidents"
    print(f"--- ТЕСТ KAGGLE: Пытаемся скачать датасет '{dataset_name}'... ---")
    
    try:
        # Эта строка вызовет аутентификацию
        path = kagglehub.dataset_download(dataset_name)
        
        print("--- ТЕСТ KAGGLE: УСПЕХ! ---")
        print(f"--- ТЕСТ KAGGLE: Датасет скачан по пути: {path} ---")
        
        # Проверим содержимое скачанной папки
        contents = os.listdir(path)
        print(f"--- ТЕСТ KAGGLE: Содержимое папки: {contents} ---")
        
    except Exception as e:
        print("--- ТЕСТ KAGGLE: ОШИБКА! Произошло исключение. ---")
        print(f"--- ТЕСТ KAGGLE: Детали ошибки: {e} ---")
        # Перевыбрасываем исключение, чтобы задача в Airflow была помечена как FAILED
        raise e

# Определение DAG
with DAG(
    dag_id='kaggle_simple_download_test',
    start_date=days_ago(1),
    schedule=None,  # Запускается только вручную
    catchup=False,
    tags=['test', 'kaggle'],
    doc_md="Простой DAG для проверки соединения и аутентификации с Kaggle API.",
) as dag:
    
    test_download_task = PythonOperator(
        task_id='test_kaggle_download_task',
        python_callable=simple_kaggle_download,
    )