from googleapiclient.discovery import build
from kafka import KafkaProducer
import pymysql
import json
import logging
from datetime import datetime, timezone

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración de la API de YouTube
api_key = 'AIzaSyD2ftkZZ9plLsJnQVDW2-lny0QRxOuOJyo'
youtube = build('youtube', 'v3', developerKey=api_key)

# Configuración de Kafka
KAFKA_BROKER = 'localhost:29092'
TOPICO = 'youtube_data1'

# Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Conexión a MySQL
def conectar_mysql():
    return pymysql.connect(
        host='localhost',
        user='dary',
        password='Barcelona-22',
        database='youtube_data',
        cursorclass=pymysql.cursors.DictCursor
    )

# Verificar o crear las tablas necesarias
def verificar_crear_tablas(cursor):
    query_config = """
    CREATE TABLE IF NOT EXISTS reproductor_config (
        nombre_reproductor VARCHAR(255) PRIMARY KEY,
        ultima_extraccion DATETIME
    );
    """
    query_videos = """
    CREATE TABLE IF NOT EXISTS videos_procesados (
        video_id VARCHAR(50) PRIMARY KEY
    );
    """
    cursor.execute(query_config)
    cursor.execute(query_videos)
    logger.info("Tablas `reproductor_config` y `videos_procesados` verificadas o creadas exitosamente.")

# Obtener la última fecha de extracción
def obtener_ultima_extraccion(cursor, nombre_reproductor):
    query_select = "SELECT ultima_extraccion FROM reproductor_config WHERE nombre_reproductor = %s;"
    cursor.execute(query_select, (nombre_reproductor,))
    resultado = cursor.fetchone()

    if resultado:
        return resultado['ultima_extraccion']
    else:
        # Insertar un registro inicial si no existe
        query_insert = "INSERT INTO reproductor_config (nombre_reproductor, ultima_extraccion) VALUES (%s, %s);"
        cursor.execute(query_insert, (nombre_reproductor, None))
        return None

# Actualizar la última fecha de extracción
def actualizar_ultima_extraccion(cursor, nombre_reproductor, fecha):
    query_update = "UPDATE reproductor_config SET ultima_extraccion = %s WHERE nombre_reproductor = %s;"
    cursor.execute(query_update, (fecha, nombre_reproductor))

# Buscar videos más populares en Ecuador
def buscar_tendencias_musicales(region_code='EC', max_resultados=50):
    try:
        request = youtube.videos().list(
            part='snippet,statistics',
            chart='mostPopular',
            regionCode=region_code,
            maxResults=max_resultados,
            videoCategoryId='10'  # Categoría de música
        )
        response = request.execute()
        return response.get('items', [])
    except Exception as e:
        logger.error(f"Error al buscar tendencias musicales: {e}")
        return []

# Verificar si un video ya fue procesado
def video_ya_procesado(cursor, video_id):
    query = "SELECT video_id FROM videos_procesados WHERE video_id = %s;"
    cursor.execute(query, (video_id,))
    return cursor.fetchone() is not None

# Registrar un video como procesado
def registrar_video_procesado(cursor, video_id):
    query = "INSERT INTO videos_procesados (video_id) VALUES (%s);"
    cursor.execute(query, (video_id,))

# Procesar videos y publicar en Kafka
def procesar_videos(videos, cursor):
    procesados = 0
    for video in videos:
        video_id = video['id']
        if video_ya_procesado(cursor, video_id):
            logger.info(f"Video ya procesado: {video_id}")
            continue

        try:
            # Extraer detalles del video
            datos_video = {
                'titulo': video['snippet']['title'],
                'descripcion': video['snippet']['description'],
                'etiquetas': video['snippet'].get('tags', []),
                'canal': video['snippet']['channelTitle'],
                'fecha_publicacion': video['snippet']['publishedAt'],
                'vistas': int(video['statistics'].get('viewCount', 0)),
                'likes': int(video['statistics'].get('likeCount', 0)),
                'video_id': video_id
            }
            # Publicar en Kafka
            producer.send(TOPICO, datos_video)
            # Registrar como procesado
            registrar_video_procesado(cursor, video_id)
            procesados += 1
        except Exception as e:
            logger.error(f"Error al procesar video {video_id}: {e}")
    return procesados

# Reproductor principal
def main():
    reproductor_nombre = "youtube_trending_videos"
    conexion = conectar_mysql()
    cursor = conexion.cursor()

    try:
        # Verificar o crear tablas necesarias
        verificar_crear_tablas(cursor)

        # Obtener última fecha de extracción
        ultima_extraccion = obtener_ultima_extraccion(cursor, reproductor_nombre)
        logger.info(f"Última extracción realizada el: {ultima_extraccion}")

        # Buscar tendencias musicales
        logger.info("Buscando tendencias musicales...")
        tendencias = buscar_tendencias_musicales(region_code='EC')

        if tendencias:
            # Procesar videos
            procesados = procesar_videos(tendencias, cursor)
            conexion.commit()

            # Actualizar última extracción
            nueva_fecha_extraccion = datetime.now(timezone.utc)
            actualizar_ultima_extraccion(cursor, reproductor_nombre, nueva_fecha_extraccion)
            conexion.commit()

            logger.info(f"Total de videos procesados: {procesados}")
            logger.info(f"Fecha de última extracción actualizada: {nueva_fecha_extraccion}")
        else:
            logger.info("No se encontraron nuevos videos.")
    except Exception as e:
        logger.error(f"Error en el proceso principal: {e}")
    finally:
        cursor.close()
        conexion.close()
        producer.close()
        logger.info("Recursos cerrados correctamente.")
        logger.info("Proceso completado.")

if __name__ == "__main__":
    main()

