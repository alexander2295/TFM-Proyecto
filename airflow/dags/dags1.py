from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Función de prueba
def funcion_prueba():
    print("¡Hola! Esta es una tarea de prueba en Airflow.")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 18),  # Cambia la fecha según sea necesario
    'retries': 1,
}

dag = DAG(
    'test_dag',  # Nombre del DAG
    default_args=default_args,
    description='Un DAG de prueba para verificar la configuración de Airflow',
    schedule_interval='@once',  # Ejecutar una vez
)

# Definición de la tarea
tarea_prueba = PythonOperator(
    task_id='tarea_prueba',  # ID único de la tarea
    python_callable=funcion_prueba,  # Llama a la función de prueba
    dag=dag,
)

# No hay dependencias en este caso, ya que solo hay una tarea