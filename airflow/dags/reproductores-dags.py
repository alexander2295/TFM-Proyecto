from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import subprocess
import os

# Ruta base de los scripts
BASE_PATH = r"C:\Users\Usuario\Desktop\TFM_Poryecto\scripts1\reproductores"

# Funciones para ejecutar los scripts
def ejecutar_script(script_name):
    script_path = os.path.join(BASE_PATH, script_name)
    subprocess.run(['python', script_path], check=True)

# Argumentos por defecto
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 16),  # Fecha en el pasado
    'retries': 1,
}

# Definir el DAG
dag = DAG(
    'reproductores_dag',  # Nombre del DAG
    default_args=default_args,
    description='DAG para ejecutar los reproductores',
    schedule_interval='@daily',  # Ejecutar diariamente
)

# Definición de tareas
tarea1 = PythonOperator(
    task_id='ejecutar_reproductor1',
    python_callable=ejecutar_script,
    op_args=['reproductor1.py'],
    dag=dag,
)

tarea2 = PythonOperator(
    task_id='ejecutar_reproductor2',
    python_callable=ejecutar_script,
    op_args=['reproductor2.py'],
    dag=dag,
)

tarea3 = PythonOperator(
    task_id='ejecutar_reproductor3',
    python_callable=ejecutar_script,
    op_args=['reproductor3.py'],
    dag=dag,
)

# Establecer dependencias
tarea1 >> tarea2 >> tarea3  # Ejecutar en secuencia
