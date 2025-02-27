from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error
import pandas as pd
import json
import logging

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# **Configuración de MySQL**
MYSQL_HOST = 'localhost'
MYSQL_PORT = 3306
MYSQL_USER = 'dary'
MYSQL_PASSWORD = 'Barcelona-22'
MYSQL_DB = 'youtube_data'

# **Configuración de Kafka**
KAFKA_BROKER = 'localhost:29092'
TOPICO = 'artistasgenero'

# Función para conectar a MySQL
def conectar_mysql():
    try:
        conexion = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        if conexion.is_connected():
            logger.info("Conexión a MySQL exitosa.")
            return conexion
    except Error as e:
        logger.error(f"Error al conectar a MySQL: {e}")
        return None

# Función para crear la tabla en MySQL
def crear_tabla(conexion):
    query = """
    CREATE TABLE IF NOT EXISTS artistas_genero (
        id INT AUTO_INCREMENT PRIMARY KEY,
        genero VARCHAR(50),
        cantidad_videos INT,
        vistas BIGINT,
        likes BIGINT,
        titulos TEXT
    );
    """
    try:
        cursor = conexion.cursor()
        cursor.execute(query)
        conexion.commit()
        logger.info("Tabla 'artistas_genero' creada o ya existe.")
    except Error as e:
        logger.error(f"Error al crear la tabla: {e}")
        conexion.rollback()

# Función para insertar datos limpios en MySQL
def insertar_datos(conexion, datos):
    query = """
    INSERT INTO artistas_genero (
        genero, cantidad_videos, vistas, likes, titulos
    ) VALUES (%s, %s, %s, %s, %s)
    """
    try:
        cursor = conexion.cursor()
        cursor.executemany(query, datos)
        conexion.commit()
        logger.info(f"Se insertaron {cursor.rowcount} registros en MySQL.")
    except Error as e:
        logger.error(f"Error al insertar datos en MySQL: {e}")
        conexion.rollback()

# Función para limpiar y transformar datos
def clean_and_transform_data(mensaje):
    """
    Limpia y transforma los datos de un mensaje del tópico `artistasgenero`.
    """
    try:
        genero = mensaje.get('genero', '').capitalize()
        cantidad_videos = mensaje.get('cantidad_videos', 0)
        vistas = mensaje.get('vistas', 0)
        likes = mensaje.get('likes', 0)
        titulos = ', '.join(mensaje.get('titulos', []))  # Convertir la lista de títulos en una cadena separada por comas

        return (genero, cantidad_videos, vistas, likes, titulos)
    except Exception as e:
        logger.error(f"Error al limpiar datos: {e}")
        return None

# Consumir mensajes de Kafka
def consumir_kafka():
    consumer = KafkaConsumer(
        TOPICO,
        bootstrap_servers=KAFKA_BROKER,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode('utf-8')),
        consumer_timeout_ms=10000  # Salir si no hay mensajes después de 10 segundos
    )

    logger.info(f"Conectado al tópico {TOPICO}. Esperando mensajes...")

    # Conectar a MySQL
    conexion = conectar_mysql()
    if not conexion:
        logger.error("No se pudo establecer conexión con MySQL. Saliendo...")
        return

    # Crear tabla si no existe
    crear_tabla(conexion)

    mensajes_limpios = []
    total_mensajes_procesados = 0

    # Procesar mensajes
    for mensaje in consumer:
        data = mensaje.value
        logger.info(f"Mensaje recibido: {data}")

        datos_limpios = clean_and_transform_data(data)
        if datos_limpios:
            mensajes_limpios.append(datos_limpios)
            total_mensajes_procesados += 1

        # Insertar datos en MySQL cada 50 mensajes
        if len(mensajes_limpios) >= 50:
            insertar_datos(conexion, mensajes_limpios)
            mensajes_limpios = []

    # Insertar los mensajes restantes
    if mensajes_limpios:
        insertar_datos(conexion, mensajes_limpios)

    logger.info(f"Proceso completado. Total de géneros procesados y limpiados: {total_mensajes_procesados}")

    # Cerrar conexión MySQL
    if conexion.is_connected():
        conexion.close()
        logger.info("Conexión a MySQL cerrada.")

    # Cerrar Kafka Consumer
    consumer.close()

# Ejecutar consumidor
if __name__ == '__main__':
    consumir_kafka()
