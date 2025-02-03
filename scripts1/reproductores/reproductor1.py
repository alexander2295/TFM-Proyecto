import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from kafka import KafkaProducer
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import pymysql
import logging
import time
from concurrent.futures import ThreadPoolExecutor

# Cargar variables de entorno
load_dotenv(dotenv_path='C:/Users/Usuario/Desktop/TFM_Poryecto/.env')

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración de API de YouTube
api_key = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=api_key)

# Configuración de Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPICO = 'artistasgenero'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    request_timeout_ms=30000
)

# Configuración de MySQL
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = int(os.getenv('MYSQL_PORT'))
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')

# Palabras clave para géneros musicales (ahora cargadas desde base de datos)
def obtener_generos_desde_db():
    """Obtiene los géneros y sus palabras clave desde MySQL"""
    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
                           password=MYSQL_PASSWORD, database=MYSQL_DB, cursorclass=pymysql.cursors.DictCursor)
    generos = {}
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT genero, palabras_clave FROM generos_musicales")
            for row in cursor.fetchall():
                generos[row['genero']] = row['palabras_clave'].split(',')
    except Exception as e:
        logger.error(f"Error obteniendo géneros desde la base de datos: {e}")
    finally:
        conn.close()
    return generos

generos = obtener_generos_desde_db()

# Función para manejar la paginación de YouTube
def obtener_videos_youtube(inicio, fin):
    """Extrae videos desde la API de YouTube con paginación"""
    videos = []
    next_page_token = None
    while True:
        try:
            request = youtube.search().list(
                part='snippet',
                type='video',
                regionCode='EC',
                maxResults=50,
                publishedAfter=inicio.isoformat(),
                publishedBefore=fin.isoformat(),
                videoCategoryId='10',
                pageToken=next_page_token
            )
            response = request.execute()
            videos.extend(response.get('items', []))
            next_page_token = response.get('nextPageToken')

            if not next_page_token:
                break

        except Exception as e:
            logger.error(f"Error al obtener videos: {e}")
            time.sleep(5)  # Esperar antes de reintentar

    return videos

# Función para analizar géneros
def analizar_generos(videos):
    """Clasifica videos en géneros según palabras clave"""
    conteo_generos = defaultdict(lambda: {'anios': set(), 'cantidad_videos': 0, 'vistas': 0, 'likes': 0, 'titulos': []})
    for video in videos:
        try:
            if 'statistics' not in video:
                continue

            titulo = video['snippet']['title'].lower()
            descripcion = video['snippet']['description'].lower()
            etiquetas = video['snippet'].get('tags', [])
            texto_completo = f"{titulo} {descripcion} {' '.join(etiquetas)}"

            for genero, palabras_clave in generos.items():
                if any(palabra in texto_completo for palabra in palabras_clave):
                    anio = datetime.strptime(video['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").year
                    vistas = int(video['statistics'].get('viewCount', 0))
                    likes = int(video['statistics'].get('likeCount', 0))

                    conteo_generos[genero]['anios'].add(anio)
                    conteo_generos[genero]['cantidad_videos'] += 1
                    conteo_generos[genero]['vistas'] += vistas
                    conteo_generos[genero]['likes'] += likes
                    conteo_generos[genero]['titulos'].append(video['snippet']['title'])

        except Exception as e:
            logger.error(f"Error inesperado al procesar video: {e}")

    return conteo_generos

# Función principal
def main():
    reproductor_nombre = "reproductor_3"

    conn = pymysql.connect(host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
                           password=MYSQL_PASSWORD, database=MYSQL_DB, cursorclass=pymysql.cursors.DictCursor)

    # Obtener última extracción
    with conn.cursor() as cursor:
        cursor.execute("SELECT ultima_extraccion FROM reproductor_config WHERE nombre_reproductor = %s;", (reproductor_nombre,))
        ultima_extraccion = cursor.fetchone()
    
    fecha_inicio = ultima_extraccion['ultima_extraccion'].replace(tzinfo=timezone.utc) if ultima_extraccion else datetime(2023, 1, 1, tzinfo=timezone.utc)
    fecha_fin = datetime.now(timezone.utc)

    logger.info(f"Extrayendo videos desde {fecha_inicio} hasta {fecha_fin}...")

    # Paralelizar extracción de datos en bloques de 30 días
    videos_totales = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futuros = [executor.submit(obtener_videos_youtube, inicio, min(inicio + timedelta(days=30), fecha_fin))
                   for inicio in range(int((fecha_fin - fecha_inicio).days / 30))]
        for futuro in futuros:
            videos_totales.extend(futuro.result())

    # Analizar géneros y enviar a Kafka
    resultados_generos = analizar_generos(videos_totales)
    for genero, datos in resultados_generos.items():
        mensaje = {
            'genero': genero,
            'cantidad_videos': datos['cantidad_videos'],
            'vistas': datos['vistas'],
            'likes': datos['likes'],
            'anios': list(datos['anios']),
            'titulos': datos['titulos'][:5]
        }
        producer.send(TOPICO, mensaje)
        logger.info(f"Publicado en Kafka: {mensaje}")

    # Actualizar última extracción
    with conn.cursor() as cursor:
        cursor.execute("UPDATE reproductor_config SET ultima_extraccion = %s WHERE nombre_reproductor = %s;",
                       (fecha_fin.strftime("%Y-%m-%d %H:%M:%S"), reproductor_nombre))
        conn.commit()

    producer.close()
    conn.close()

if __name__ == "__main__":
    main()
