import os
import json
import logging
import pymysql
from dotenv import load_dotenv
from googleapiclient.discovery import build
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
from collections import defaultdict
from datetime import datetime, timezone, timedelta

# Cargar variables de entorno
load_dotenv(dotenv_path='C:/Users/Usuario/Desktop/TFM_Poryecto/.env')

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración de API YouTube
api_key = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=api_key)

# Configuración de Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'localhost:9092')
TOPICO = 'artistasgenero'

# Configuración de MySQL
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = int(os.getenv('MYSQL_PORT'))
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')

# Palabras clave por género musical
generos = {
    'reguetón': ['reguetón', 'reggaeton', 'bad bunny', 'karol g', 'feid', 'j balvin'],
    'rock': ['rock', 'queen', 'imagine dragons', 'nirvana', 'metallica'],
    'pop': ['pop', 'taylor swift', 'dua lipa', 'bruno mars', 'shakira'],
    'trap': ['trap', 'anuel', 'ozuna', 'arcangel'],
    'cumbia': ['cumbia', 'corazón serrano', 'grupo 5'],
    'balada': ['balada', 'luis miguel', 'alejandro sanz'],
    'salsa': ['salsa', 'marc anthony', 'gilberto santa rosa'],
}

# Función para conectar a MySQL
def conectar_mysql():
    return pymysql.connect(
        host=MYSQL_HOST, port=MYSQL_PORT, user=MYSQL_USER,
        password=MYSQL_PASSWORD, database=MYSQL_DB,
        cursorclass=pymysql.cursors.DictCursor
    )

# Verificar y crear el topic en Kafka
def verificar_o_crear_topic():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
        topics_existentes = admin_client.list_topics()

        if TOPICO not in topics_existentes:
            logger.info(f"El topic '{TOPICO}' no existe. Creándolo...")
            topic = NewTopic(name=TOPICO, num_partitions=3, replication_factor=1)
            admin_client.create_topics([topic])
            logger.info(f"Topic '{TOPICO}' creado exitosamente.")
        else:
            logger.info(f"El topic '{TOPICO}' ya existe.")

        admin_client.close()
    except Exception as e:
        logger.error(f"Error al verificar/crear el topic en Kafka: {e}")

# Crear el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    request_timeout_ms=30000
)

# Obtener la última fecha de extracción desde MySQL
def obtener_ultima_extraccion(conn, nombre_reproductor):
    query = "SELECT ultima_extraccion FROM reproductor_config WHERE nombre_reproductor = %s;"
    with conn.cursor() as cursor:
        cursor.execute(query, (nombre_reproductor,))
        resultado = cursor.fetchone()
        return resultado['ultima_extraccion'] if resultado else None

# Actualizar la última fecha de extracción en MySQL
def actualizar_ultima_extraccion(conn, nombre_reproductor, fecha):
    query = "UPDATE reproductor_config SET ultima_extraccion = %s WHERE nombre_reproductor = %s;"
    with conn.cursor() as cursor:
        cursor.execute(query, (fecha.strftime("%Y-%m-%d %H:%M:%S"), nombre_reproductor))
        conn.commit()

# Función para obtener videos de YouTube API con paginación
def obtener_videos_youtube(inicio, fin):
    videos_totales = []
    next_page_token = None

    while True:
        respuesta = youtube.search().list(
            part='snippet',
            type='video',
            regionCode='EC',
            maxResults=50,
            publishedAfter=inicio.isoformat(),
            publishedBefore=fin.isoformat(),
            videoCategoryId='10',
            pageToken=next_page_token
        ).execute()

        videos_totales.extend(respuesta.get('items', []))
        next_page_token = respuesta.get('nextPageToken')

        if not next_page_token:
            break  # No hay más páginas

    return videos_totales

# Función para analizar géneros musicales
def analizar_generos(videos):
    conteo_generos = defaultdict(lambda: {'anios': set(), 'cantidad_videos': 0, 'vistas': 0, 'likes': 0, 'titulos': []})
    for video in videos:
        titulo = video['snippet']['title'].lower()
        descripcion = video['snippet']['description'].lower()
        etiquetas = video['snippet'].get('tags', [])
        texto_completo = f"{titulo} {descripcion} {' '.join(etiquetas)}"

        for genero, palabras_clave in generos.items():
            if any(palabra in texto_completo for palabra in palabras_clave):
                anio = datetime.strptime(video['snippet']['publishedAt'], "%Y-%m-%dT%H:%M:%SZ").year
                conteo_generos[genero]['anios'].add(anio)
                conteo_generos[genero]['cantidad_videos'] += 1
                conteo_generos[genero]['titulos'].append(video['snippet']['title'])

    return conteo_generos

# Enviar datos a Kafka
def enviar_a_kafka(resultados_generos):
    for genero, datos in resultados_generos.items():
        mensaje = {
            'genero': genero,
            'cantidad_videos': datos['cantidad_videos'],
            'anios': list(datos['anios']),
            'titulos': datos['titulos'][:5]
        }
        producer.send(TOPICO, mensaje)
        logger.info(f"Publicado en Kafka: {mensaje}")

# Función principal
def main():
    reproductor_nombre = "reproductor_3"
    conn = conectar_mysql()

    verificar_o_crear_topic()  # Verificar/crear el topic en Kafka

    ultima_extraccion = obtener_ultima_extraccion(conn, reproductor_nombre)
    fecha_inicio = ultima_extraccion.replace(tzinfo=timezone.utc) if ultima_extraccion else datetime(2023, 1, 1, tzinfo=timezone.utc)
    fecha_fin = datetime.now(timezone.utc)

    logger.info(f"Extrayendo videos desde {fecha_inicio} hasta {fecha_fin}...")

    videos = obtener_videos_youtube(fecha_inicio, fecha_fin)
    resultados_generos = analizar_generos(videos)
    enviar_a_kafka(resultados_generos)

    actualizar_ultima_extraccion(conn, reproductor_nombre, fecha_fin)
    producer.close()
    conn.close()

if __name__ == "__main__":
    main()
