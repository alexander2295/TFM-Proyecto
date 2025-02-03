import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
import mysql.connector
from mysql.connector import Error
import json
import logging
import re
from datetime import datetime

# Cargar las variables de entorno desde el archivo .env
load_dotenv(dotenv_path='C:/Users/Usuario/Desktop/TFM_Poryecto/.env')

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# **Configuración de MySQL**
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = int(os.getenv('MYSQL_PORT'))
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')

# **Configuración de Kafka**
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPICO = 'youtube_music_playlists'  # Tópico definido directamente en el código

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
        raise

# Función para crear la tabla en MySQL
def crear_tabla(conexion):
    query = """
    CREATE TABLE IF NOT EXISTS youtube_music_playlists (
        id INT AUTO_INCREMENT PRIMARY KEY,
        titulo VARCHAR(255),
        descripcion TEXT,
        video_id VARCHAR(50) UNIQUE,
        playlist_id VARCHAR(50),
        playlist_titulo VARCHAR(255),
        view_count INT DEFAULT 0,
        like_count INT DEFAULT 0,
        comment_count INT DEFAULT 0,
        duration_seconds INT DEFAULT 0,
        fecha_publicacion DATETIME
    );
    """
    try:
        cursor = conexion.cursor()
        cursor.execute(query)
        conexion.commit()
        logger.info("Tabla 'youtube_music_playlists' creada o ya existe.")
    except Error as e:
        logger.error(f"Error al crear la tabla: {e}")
        conexion.rollback()

# Función para insertar datos limpios en MySQL
def insertar_datos(conexion, datos):
    query = """
    INSERT INTO youtube_music_playlists (
        titulo, descripcion, video_id, playlist_id, playlist_titulo,
        view_count, like_count, comment_count, duration_seconds, fecha_publicacion
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        titulo = VALUES(titulo),
        descripcion = VALUES(descripcion),
        playlist_titulo = VALUES(playlist_titulo),
        view_count = VALUES(view_count),
        like_count = VALUES(like_count),
        comment_count = VALUES(comment_count),
        duration_seconds = VALUES(duration_seconds),
        fecha_publicacion = VALUES(fecha_publicacion);
    """
    try:
        cursor = conexion.cursor()
        cursor.executemany(query, datos)
        conexion.commit()
        logger.info(f"Se insertaron o actualizaron {cursor.rowcount} registros en MySQL.")
    except Error as e:
        logger.error(f"Error al insertar datos en MySQL: {e}")
        conexion.rollback()

# Función para limpiar las descripciones
def limpiar_descripcion(descripcion):
    """Limpia enlaces, hashtags y caracteres innecesarios de las descripciones."""
    try:
        descripcion = re.sub(r'http\S+|www\S+', '', descripcion)  # Eliminar URLs
        descripcion = re.sub(r'#\S+', '', descripcion)  # Eliminar hashtags
        descripcion = re.sub(r'\s+', ' ', descripcion).strip()  # Eliminar espacios múltiples
        return descripcion
    except Exception as e:
        logger.error(f"Error al limpiar la descripción: {e}")
        return descripcion

# Función para limpiar y transformar los datos
def limpiar_y_transformar_datos(data):
    """Limpia y transforma un mensaje del tópico de Kafka."""
    try:
        titulo = data.get('titulo', '').title()
        descripcion = limpiar_descripcion(data.get('descripcion', '').strip())
        video_id = data.get('video_id', '').strip()
        playlist_id = data.get('playlist_id', '').strip()
        playlist_titulo = data.get('playlist_titulo', '').title()
        view_count = int(data.get('viewCount', 0))
        like_count = int(data.get('likeCount', 0))
        comment_count = int(data.get('commentCount', 0))
        duration_seconds = int(data.get('duration_seconds', 0))

        # Convertir la fecha de publicación al formato 'YYYY-MM-DD HH:MM:SS'
        fecha_publicacion_iso = data.get('fecha_publicacion', None)
        fecha_publicacion = None
        if fecha_publicacion_iso:
            try:
                fecha_publicacion = datetime.strptime(fecha_publicacion_iso, "%Y-%m-%dT%H:%M:%SZ")
            except ValueError as e:
                logger.error(f"Formato de fecha incorrecto: {fecha_publicacion_iso}. Error: {e}")

        # Validar campos obligatorios
        if not video_id or not playlist_id or not titulo:
            logger.warning("Datos incompletos. Saltando mensaje.")
            return None

        return (
            titulo, descripcion, video_id, playlist_id, playlist_titulo,
            view_count, like_count, comment_count, duration_seconds, fecha_publicacion
        )
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

        datos_limpios = limpiar_y_transformar_datos(data)
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

    logger.info(f"Proceso completado. Total de mensajes procesados: {total_mensajes_procesados}")

    # Cerrar conexión MySQL
    if conexion.is_connected():
        conexion.close()
        logger.info("Conexión a MySQL cerrada.")

    # Cerrar Kafka Consumer
    consumer.close()

# Ejecutar consumidor
if __name__ == '__main__':
    consumir_kafka()
