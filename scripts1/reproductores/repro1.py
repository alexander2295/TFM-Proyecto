import os
from dotenv import load_dotenv
from googleapiclient.discovery import build
from kafka import KafkaProducer
import json
import logging
from datetime import datetime
import isodate

# Cargar las variables de entorno desde el archivo .env
load_dotenv(dotenv_path='C:/Users/Usuario/Desktop/TFM_Poryecto/.env')

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# **Configuración de la API de YouTube**
api_key = os.getenv('YOUTUBE_API_KEY')  # Leer la clave de API desde el archivo .env
youtube = build('youtube', 'v3', developerKey=api_key)

# **Configuración de Kafka**
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPICO = 'youtube_music_playlists'  # Nombre del tópico definido directamente en el código

# Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    linger_ms=500,
    request_timeout_ms=30000,
    retries=5
)

# Configuración del reproductor
FECHA_INICIO = '2024-01-01T00:00:00Z'  # Desde el 1 de enero de 2024
FECHA_FIN = '2025-12-31T23:59:59Z'    # Hasta el 31 de diciembre de 2025


def convertir_a_segundos(duration_iso):
    """Convierte una duración en formato ISO 8601 (ej: PT5M30S) a segundos."""
    if not duration_iso:
        return 0
    try:
        duration = isodate.parse_duration(duration_iso)
        return int(duration.total_seconds())
    except Exception as e:
        logger.error(f"Error al convertir duración {duration_iso} a segundos: {e}")
        return 0


def obtener_metrica_video(video_id):
    """Obtiene métricas detalladas de un video (reproducciones, likes, comentarios, duración)."""
    try:
        request = youtube.videos().list(
            part='statistics,contentDetails',
            id=video_id
        )
        response = request.execute()

        if response['items']:
            video_data = response['items'][0]
            return {
                'viewCount': int(video_data['statistics'].get('viewCount', 0)),
                'likeCount': int(video_data['statistics'].get('likeCount', 0)),
                'commentCount': int(video_data['statistics'].get('commentCount', 0)),
                'duration': video_data['contentDetails'].get('duration', ''),
            }
        else:
            logger.warning(f"No se encontraron métricas para el video ID: {video_id}")
            return {'viewCount': 0, 'likeCount': 0, 'commentCount': 0, 'duration': ''}
    except Exception as e:
        logger.error(f"Error al obtener métricas del video {video_id}: {e}")
        return {'viewCount': 0, 'likeCount': 0, 'commentCount': 0, 'duration': ''}


def buscar_listas_reproduccion(region_code='EC', max_resultados=10):
    """Busca listas de reproducción relacionadas con música y sus métricas básicas."""
    try:
        logger.info(f"Buscando listas de reproducción publicadas entre {FECHA_INICIO} y {FECHA_FIN}...")
        request = youtube.search().list(
            part='snippet',
            type='playlist',
            regionCode=region_code,
            maxResults=max_resultados,
            q='YouTube Music Ecuador',
            publishedAfter=FECHA_INICIO,
            publishedBefore=FECHA_FIN
        )
        response = request.execute()
        playlists = []

        for item in response.get('items', []):
            playlist_id = item['id']['playlistId']
            titulo = item['snippet']['title']
            fecha_creacion = item['snippet']['publishedAt']
            channel_title = item['snippet']['channelTitle']

            # Obtener métricas adicionales de la playlist
            request_playlist = youtube.playlists().list(
                part='contentDetails',
                id=playlist_id
            )
            response_playlist = request_playlist.execute()
            item_count = response_playlist['items'][0]['contentDetails'].get('itemCount', 0)

            playlists.append({
                'playlist_id': playlist_id,
                'titulo': titulo,
                'fecha_creacion': fecha_creacion,
                'channelTitle': channel_title,
                'itemCount': item_count
            })

        return playlists
    except Exception as e:
        logger.error(f"Error al buscar listas de reproducción: {e}")
        return []


def obtener_videos_de_lista(playlist_id, max_resultados=50):
    """Extrae los videos de una lista de reproducción específica, junto con sus métricas detalladas."""
    videos = []
    next_page_token = None

    while True:
        try:
            request = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=max_resultados,
                pageToken=next_page_token
            )
            response = request.execute()

            for video in response.get('items', []):
                video_id = video['snippet']['resourceId']['videoId']
                fecha_publicacion = video['snippet']['publishedAt']

                # Filtrar videos por fecha (2024-2025)
                if FECHA_INICIO <= fecha_publicacion <= FECHA_FIN:
                    # Obtener métricas del video
                    metricas = obtener_metrica_video(video_id)
                    if metricas['viewCount'] == 0 and metricas['duration'] == '':
                        continue
                    metricas['duration_seconds'] = convertir_a_segundos(metricas.get('duration', ''))

                    video_data = {
                        'video_id': video_id,
                        'titulo': video['snippet']['title'],
                        'descripcion': video['snippet']['description'],
                        'playlist_id': playlist_id,
                        'fecha_publicacion': fecha_publicacion,
                        'viewCount': metricas['viewCount'],
                        'likeCount': metricas['likeCount'],
                        'commentCount': metricas['commentCount'],
                        'duration_seconds': metricas['duration_seconds'],
                    }

                    videos.append(video_data)

            next_page_token = response.get('nextPageToken')
            if not next_page_token:
                break
        except Exception as e:
            logger.error(f"Error al obtener videos de la lista {playlist_id}: {e}")
            break

    return videos


def main():
    try:
        listas = buscar_listas_reproduccion(region_code='EC', max_resultados=10)
        if listas:
            logger.info(f"Se encontraron {len(listas)} listas de reproducción.")
            total_videos = 0

            for lista in listas:
                playlist_id = lista['playlist_id']
                titulo_lista = lista['titulo']
                logger.info(f"Procesando lista de reproducción: {titulo_lista} (ID: {playlist_id})")
                videos = obtener_videos_de_lista(playlist_id)

                for video in videos:
                    producer.send(TOPICO, video)
                    total_videos += 1

            logger.info(f"Total de videos procesados: {total_videos}")
        else:
            logger.info("No se encontraron listas de reproducción nuevas.")
    except Exception as e:
        logger.error(f"Error durante la ejecución del reproductor: {e}")
    finally:
        producer.close()
        logger.info("Recursos cerrados correctamente.")
        logger.info("Proceso completado.")


if __name__ == "__main__":
    main()
