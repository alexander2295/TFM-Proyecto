import os
import json
import logging
import mysql.connector
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(level=logging.INFO)

# Cargar archivo .env
load_dotenv()

# Variables de entorno
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT', 3306)
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPICO = os.getenv('TOPICO', 'artistasgenero1')

# Nombre de la tabla específica para este consumidor
TABLA_MYSQL = "canciones_ecuador"  # Cambia esto si se requiere otra tabla

# Validar que las variables de entorno estén configuradas
if not all([MYSQL_HOST, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DB, KAFKA_BROKER]):
    raise ValueError("Faltan variables de entorno en el archivo .env")

# Conectar a la base de datos MySQL
def conectar_mysql():
    try:
        conexion = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        logging.info("Conexión a MySQL exitosa.")
        return conexion
    except mysql.connector.Error as err:
        logging.error(f"Error al conectar a MySQL: {err}")
        raise

# Crear la tabla si no existe
def crear_tabla(conexion):
    try:
        cursor = conexion.cursor()
        query = f"""
        CREATE TABLE IF NOT EXISTS {TABLA_MYSQL} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cancion VARCHAR(255) NOT NULL,
            artista VARCHAR(255) NOT NULL,
            oyentes INT NOT NULL,
            enlace VARCHAR(255),
            generos TEXT,
            fecha_procesamiento TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(query)
        conexion.commit()
        logging.info(f"Tabla '{TABLA_MYSQL}' verificada/creada en MySQL.")
    except mysql.connector.Error as err:
        logging.error(f"Error al crear/verificar la tabla en MySQL: {err}")

# Insertar datos en MySQL
def insertar_en_mysql(conexion, datos):
    try:
        cursor = conexion.cursor()
        query = f"""
        INSERT INTO {TABLA_MYSQL} (cancion, artista, oyentes, enlace, generos)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, datos)
        conexion.commit()
        logging.info(f"Datos insertados en MySQL en la tabla '{TABLA_MYSQL}': {datos}")
    except mysql.connector.Error as err:
        logging.error(f"Error al insertar datos en MySQL: {err}")

# Consumir mensajes de Kafka y procesarlos
def consumir_mensajes():
    try:
        # Crear el consumidor de Kafka
        consumer = KafkaConsumer(
            TOPICO,
            bootstrap_servers=[KAFKA_BROKER],
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        logging.info(f"Conectado al tópico de Kafka: {TOPICO}")

        # Conectar a MySQL
        conexion = conectar_mysql()
        crear_tabla(conexion)  # Asegurar que la tabla existe

        for mensaje in consumer:
            datos = mensaje.value

            # Limpieza y validación de datos
            cancion = datos.get('cancion', '').strip()
            artista = datos.get('artista', '').strip()
            oyentes = datos.get('oyentes', 0)
            enlace = datos.get('enlace', '').strip()
            generos = ', '.join(datos.get('generos', []))

            # Verificar que los campos obligatorios no estén vacíos
            if cancion and artista and oyentes:
                # Insertar datos en la base de datos
                insertar_en_mysql(conexion, (cancion, artista, oyentes, enlace, generos))
            else:
                logging.warning(f"Datos incompletos: {datos}")

    except Exception as e:
        logging.error(f"Error al consumir mensajes de Kafka: {e}")

if __name__ == "__main__":
    consumir_mensajes()
