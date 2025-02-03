import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from kafka import KafkaProducer
import json
from collections import defaultdict
from datetime import datetime, timezone, timedelta
import pymysql
import logging

# Cargar las variables de entorno desde el archivo .env
load_dotenv(dotenv_path='C:/Users/Usuario/Desktop/TFM_Poryecto/.env')

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de la API de YouTube
api_key = os.getenv('YOUTUBE_API_KEY')
youtube = build('youtube', 'v3', developerKey=api_key)

# Configuración de Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPICO = 'artistasgenero'  # Definido directamente en el código

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

# Palabras clave para géneros musicales
generos = {
    'reguetón': ['reguetón', 'reggaeton', 'bad bunny', 'karol g', 'feid', 'j balvin', 'daddy yankee'],
    'rock': ['rock', 'queen', 'imagine dragons', 'arctic monkeys', 'nirvana', 'metallica', 'the beatles'],
    'pop': ['pop', 'taylor swift', 'dua lipa', 'bruno mars', 'shakira', 'billie eilish'],
    'trap': ['trap', 'anuel', 'ozuna', 'mora', 'arcangel'],
    'cumbia': ['cumbia', 'corazón serrano', 'grupo 5', 'los angeles azules'],
    'balada': ['balada', 'luis miguel', 'alejandro sanz', 'ricardo arjona'],
    'salsa': ['salsa', 'marc anthony', 'gilberto santa rosa', 'carlos vives'],
    'bomba': ['bomba', 'bomba del chota', 'afroecuatoriano', 'ritmos afro'],
    'pasillo': ['pasillo', 'julio jaramillo', 'juan fernando velasco', 'melancolía'],
    'jazz': ['jazz', 'smooth jazz', 'miles davis', 'louis armstrong']
}

# Función para conectar a MySQL
def conectar_mysql():
    return pymysql.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DB,
        cursorclass=pymysql.cursors.DictCursor
    )

# Función para verificar/crear la tabla de configuración
def verificar_crear_tabla_config(conn):
    query = """
    CREATE TABLE IF NOT EXISTS reproductor_config (
        nombre_reproductor VARCHAR(255) PRIMARY KEY,
        ultima_extraccion DATETIME
    );
    """
    with conn.cursor() as cursor:
        cursor.execute(query)
        conn.commit()
    logger.info("Tabla `reproductor_config` verificada o creada exitosamente.")

# Función para obtener la última fecha de extracción
def obtener_ultima_extraccion(conn, nombre_reproductor):
    query_select = "SELECT ultima_extraccion FROM reproductor_config WHERE nombre_reproductor = %s;"
    query_insert = "INSERT IGNORE INTO reproductor_config (nombre_reproductor, ultima_extraccion) VALUES (%s, NULL);"
    with conn.cursor() as cursor:
        cursor.execute(query_select, (nombre_reproductor,))
        resultado = cursor.fetchone()
        if resultado:
            return resultado['ultima_extraccion']
        else:
            cursor.execute(query_insert, (nombre_reproductor,))
            conn.commit()
            return None

# Función para actualizar la última fecha de extracción
def actualizar_ultima_extraccion(conn, nombre_reproductor, fecha):
    query_update = "UPDATE reproductor_config SET ultima_extraccion = %s WHERE nombre_reproductor = %s;"
    with conn.cursor() as cursor:
        cursor.execute(query_update, (fecha.strftime("%Y-%m-%d %H:%M:%S"), nombre_reproductor))
        conn.commit()
    logger.info(f"Fecha de última extracción actualizada a {fecha}.")

# Función para dividir un rango de fechas en bloques
def dividir_rango_fechas(inicio, fin, delta=timedelta(days=30)):
    while inicio < fin:
        siguiente = min(inicio + delta, fin)
        yield inicio, siguiente
        inicio = siguiente

# Función para analizar géneros musicales
def analizar_generos(videos):
    conteo_generos = defaultdict(lambda: {'anios': set(), 'cantidad_videos': 0, 'vistas': 0, 'likes': 0, 'titulos': []})
    for video in videos:
        try:
            if 'statistics' not in video:
                logger.warning(f"El video {video.get('id', {}).get('videoId', 'desconocido')} no tiene estadísticas. Omitiendo...")
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

        except KeyError as e:
            logger.error(f"Error al procesar video: Falta la clave {e}. Datos del video: {video}")
        except Exception as e:
            logger.error(f"Error inesperado al procesar video: {e}")

    return conteo_generos

# Función principal
def main():
    reproductor_nombre = "reproductor_3"
    conn = conectar_mysql()
    verificar_crear_tabla_config(conn)

    ultima_extraccion = obtener_ultima_extraccion(conn, reproductor_nombre)
    fecha_inicio = ultima_extraccion.replace(tzinfo=timezone.utc) if ultima_extraccion else datetime(2023, 1, 1, tzinfo=timezone.utc)
    fecha_fin = datetime.now(timezone.utc)

    logger.info(f"Extrayendo videos desde {fecha_inicio} hasta {fecha_fin}...")

    videos_totales = []
    for inicio, fin in dividir_rango_fechas(fecha_inicio, fecha_fin):
        logger.info(f"Consultando videos entre {inicio} y {fin}...")
        videos = youtube.search().list(
            part='snippet',
            type='video',
            regionCode='EC',
            maxResults=50,
            publishedAfter=inicio.isoformat(),
            publishedBefore=fin.isoformat(),
            videoCategoryId='10'
        ).execute().get('items', [])
        videos_totales.extend(videos)

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

    actualizar_ultima_extraccion(conn, reproductor_nombre, fecha_fin)
    producer.close()
    conn.close()


if __name__ == "__main__":
    main()
