import os
import time
import subprocess
import logging
import sys

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Función para ejecutar un script
def run_script(script_name, venv_python):
    """Ejecuta un script de Python en el sistema."""
    try:
        logger.info(f"Ejecutando {script_name}...")
        
        # Verificar si el script existe antes de ejecutarlo
        if not os.path.isfile(script_name):
            logger.error(f"El script no existe: {script_name}")
            return -1

        # Ejecutar el script con el intérprete del entorno virtual
        result = subprocess.run([venv_python, script_name], capture_output=True, text=True)

        if result.returncode == 0:
            logger.info(f"Éxito: {script_name} ejecutado correctamente.")
            logger.debug(f"Salida de {script_name}: {result.stdout}")
            return 0
        else:
            logger.error(f"Error en {script_name}: {result.stderr}")
            return result.returncode
    except Exception as e:
        logger.error(f"Ocurrió un error al ejecutar {script_name}: {e}")
        return -1

def wait_for_data_to_be_ready(wait_time=10):
    """Espera un tiempo configurable para simular la preparación de datos."""
    logger.info(f"Esperando {wait_time} segundos para asegurar que los datos estén listos en Kafka...")
    time.sleep(wait_time)

# Función principal del pipeline
def run_pipeline(venv_python, wait_time=10):
    logger.info("Iniciando el pipeline de datos...")

    # Verificar la ruta base y construir rutas absolutas
    base_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'scripts1')  # Ruta corregida
    if not os.path.exists(base_path):
        logger.error(f"La ruta base {base_path} no existe. Verifica el directorio.")
        return

    # Lista de scripts a ejecutar del productor
    producer_scripts = [
        os.path.join(base_path, 'reproductores', 'reproductor1.py'),
        os.path.join(base_path, 'reproductores', 'reproductor2.py'),
        os.path.join(base_path, 'reproductores', 'reproductor3.py')
    ]

    # Lista de scripts a ejecutar del consumidor
    consumer_scripts = [
        os.path.join(base_path, 'consumidor', 'consumidor1-y.py'),
        os.path.join(base_path, 'consumidor', 'consumidor2-y.py'),
        os.path.join(base_path, 'consumidor', 'consumidor3-y.py')
    ]

    # Verificar la existencia de los scripts del productor
    for script in producer_scripts:
        if not os.path.isfile(script):
            logger.error(f"El script del productor no se encontró: {script}")
            return

    # Ejecutar todos los scripts del productor
    for script in producer_scripts:
        if run_script(script, venv_python) != 0:
            logger.error("Fallo en el productor. Abortando pipeline.")
            return

    # Esperar a que los datos estén listos
    wait_for_data_to_be_ready(wait_time)

    # Verificar la existencia de los scripts del consumidor
    for script in consumer_scripts:
        if not os.path.isfile(script):
            logger.error(f"El script del consumidor no se encontró: {script}")
            return

    # Ejecutar todos los scripts del consumidor
    for script in consumer_scripts:
        if run_script(script, venv_python) != 0:
            logger.error("Fallo en el consumidor. Abortando pipeline.")
            return

    logger.info("Pipeline de datos completado exitosamente.")

# Ejecutar el pipeline
if __name__ == '__main__':
    # Definir la ruta del intérprete de Python dentro del entorno virtual
    if sys.platform == "win32":
        venv_python = r'C:\Users\Usuario\Desktop\TFM_Poryecto\venv\Scripts\python.exe'  # Cambiado a 'Scripts'
    else:
        venv_python = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'venv', 'bin', 'python')

    # Verificar que el intérprete de Python del entorno virtual existe
    if not os.path.isfile(venv_python):
        logger.error(f"El intérprete de Python especificado no existe: {venv_python}")
        sys.exit(1)

    # Llamar a la función del pipeline con parámetros configurables
    run_pipeline(venv_python, wait_time=10)