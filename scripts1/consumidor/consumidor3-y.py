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
TOPICO = 'youtube_data1'

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
    CREATE TABLE IF NOT EXISTS youtube_videos (
        id INT AUTO_INCREMENT PRIMARY KEY,
        titulo VARCHAR(255),
        descripcion TEXT,
        etiquetas TEXT,
        canal VARCHAR(255),
        fecha_publicacion DATE,
        vistas BIGINT,
        likes BIGINT,
        video_id VARCHAR(100),
        genero VARCHAR(50)
    );
    """
    try:
        cursor = conexion.cursor()
        cursor.execute(query)
        conexion.commit()
        logger.info("Tabla 'youtube_videos' creada o ya existe.")
    except Error as e:
        logger.error(f"Error al crear la tabla: {e}")
        conexion.rollback()

# Función para insertar datos limpios en MySQL
def insertar_datos(conexion, datos):
    query = """
    INSERT INTO youtube_videos (
        titulo, descripcion, etiquetas, canal, fecha_publicacion, vistas, likes, video_id, genero
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    try:
        cursor = conexion.cursor()
        cursor.executemany(query, datos)
        conexion.commit()
        logger.info(f"Se insertaron {cursor.rowcount} registros en MySQL.")
    except Error as e:
        logger.error(f"Error al insertar datos en MySQL: {e}")
        conexion.rollback()

# Función para inferir género basado en palabras clave
def inferir_genero(titulo, descripcion, etiquetas):
    """Detecta el género de un video basado en palabras clave."""
    texto_completo = f"{titulo} {descripcion} {' '.join(etiquetas)}".lower()
    generos = {
        'reguetón': ['regueton', 'reguetón', 'bad bunny', 'karol g'],
        'pop': ['pop', 'taylor swift', 'dua lipa', 'bruno mars'],
        'rock': ['rock', 'arctic monkeys', 'imagine dragons', 'queen'],
        'trap': ['trap', 'anuel', 'ozuna'],
        'cumbia': ['cumbia', 'corazón serrano', 'grupo 5'],
        'balada': ['balada', 'alejandro sanz', 'luis miguel'],
        'funk': ['funk', 'funky', 'bruno mars'],
        'jazz': ['jazz', 'smooth jazz', 'louis armstrong'],
        'reggae': ['reggae', 'bob marley']
    }
    for genero, palabras_clave in generos.items():
        if any(palabra in texto_completo for palabra in palabras_clave):
            return genero
    return 'Desconocido'

# Función para limpiar y transformar datos de videos
def clean_and_transform_videos(video):
    """
    Limpieza y transformación de un solo video.
    """
    titulo = video.get('titulo', '').title()
    descripcion = video.get('descripcion', '')
    etiquetas = ', '.join(video.get('etiquetas', []))
    canal = video.get('canal', '')

    # Transformar la fecha_publicacion al formato 'YYYY-MM-DD'
    fecha_publicacion = video.get('fecha_publicacion', None)
    if fecha_publicacion:
        try:
            fecha_publicacion = pd.to_datetime(fecha_publicacion).strftime('%Y-%m-%d')
        except Exception as e:
            logger.warning(f"Fecha inválida: {fecha_publicacion}. Se asignará NULL. Error: {e}")
            fecha_publicacion = None

    vistas = int(video.get('vistas', 0))
    likes = int(video.get('likes', 0))
    video_id = video.get('video_id', '')
    genero = inferir_genero(titulo, descripcion, video.get('etiquetas', []))

    return (titulo, descripcion, etiquetas, canal, fecha_publicacion, vistas, likes, video_id, genero)

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
        video = mensaje.value
        logger.info(f"Mensaje recibido: {video}")

        datos_limpios = clean_and_transform_videos(video)
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

    logger.info(f"Proceso completado. Total de videos procesados y limpiados: {total_mensajes_procesados}")

    # Cerrar conexión MySQL
    if conexion.is_connected():
        conexion.close()
        logger.info("Conexión a MySQL cerrada.")

    # Cerrar Kafka Consumer
    consumer.close()

# Ejecutar consumidor
if __name__ == '__main__':
    consumir_kafka()
