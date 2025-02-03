from googleapiclient.discovery import build
from kafka import KafkaProducer
import json
import logging
from collections import defaultdict

# Configuración de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuración de la API de YouTube
api_key = 'YOUR_API_KEY'  # Reemplaza con tu clave de API
youtube = build('youtube', 'v3', developerKey=api_key)

# Configuración de Kafka
KAFKA_BROKER = 'localhost:29092'
TOPICO = 'youtube_music_top10'

# Inicializar el productor de Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Géneros y sus palabras clave
generos = {
    'reguetón': ['reguetón', 'reggaeton', 'bad bunny', 'karol g', 'j balvin', 'daddy yankee'],
    'pop': ['pop', 'dua lipa', 'bruno mars', 'shakira', 'taylor swift'],
    'rock': ['rock', 'queen', 'nirvana', 'metallica'],
    'trap': ['trap', 'anuel', 'ozuna', 'arcangel'],
    'cumbia': ['cumbia', 'grupo 5', 'corazón serrano'],
    'salsa': ['salsa', 'marc anthony', 'gilberto santa rosa'],
    'balada': ['balada', 'luis miguel', 'alejandro sanz'],
    'jazz': ['jazz', 'smooth jazz', 'miles davis'],
}

# Fechas para filtrar los videos
fecha_inicio = '2024-01-01T00:00:00Z'  # Desde el 1 de enero de 2024
fecha_fin = '2025-01-15T23:59:59Z'     # Hasta el 15 de enero de 2025


# Función para buscar listas de reproducción relacionadas con YouTube Music
def buscar_listas_reproduccion(region_code='EC', max_resultados=10):
    try:
        request = youtube.search().list(
            part='snippet',
            type='playlist',
            q='YouTube Music',
            regionCode=region_code,
            maxResults=max_resultados
        )
        response = request.execute()
        return response.get('items', [])
    except Exception as e:
        logger.error(f"Error al buscar listas de reproducción: {e}")
        return []


# Función para obtener canciones de una lista de reproducción
def obtener_canciones_playlist(playlist_id):
    canciones = []
    next_page_token = None

    while True:
        try:
            request = youtube.playlistItems().list(
                part='snippet',
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            canciones.extend(response.get('items', []))
            next_page_token = response.get('nextPageToken')

            if not next_page_token:
                break
        except Exception as e:
            logger.error(f"Error al obtener canciones de la playlist {playlist_id}: {e}")
            break

    return canciones


# Función para inferir el género de una canción
def inferir_genero(titulo, descripcion):
    texto_completo = f"{titulo} {descripcion}".lower()
    for genero, palabras_clave in generos.items():
        if any(palabra in texto_completo for palabra in palabras_clave):
            return genero
    return 'Desconocido'


# Buscar listas de reproducción populares
logger.info("Buscando listas de reproducción relacionadas con YouTube Music en Ecuador...")
playlists = buscar_listas_reproduccion(region_code='EC')

if not playlists:
    logger.info("No se encontraron listas de reproducción.")
else:
    logger.info(f"Se encontraron {len(playlists)} listas de reproducción.")

    # Diccionario para almacenar los géneros y canciones
    conteo_generos = defaultdict(lambda: {'vistas': 0, 'cantidad_videos': 0, 'canciones': []})

    # Procesar cada lista de reproducción
    for playlist in playlists:
        playlist_id = playlist['id']['playlistId']
        playlist_titulo = playlist['snippet']['title']
        logger.info(f"Procesando playlist: {playlist_titulo} (ID: {playlist_id})")

        # Obtener canciones de la playlist
        canciones = obtener_canciones_playlist(playlist_id)

        for cancion in canciones:
            try:
                titulo = cancion['snippet']['title']
                descripcion = cancion['snippet']['description']
                video_id = cancion['snippet']['resourceId']['videoId']
                genero_detectado = inferir_genero(titulo, descripcion)

                # Simular datos de vistas y likes (la API no proporciona estos datos aquí)
                vistas_simuladas = 10000 + len(titulo) * 100  # Ejemplo: Generar vistas ficticias
                likes_simulados = 500 + len(titulo) * 10      # Ejemplo: Generar likes ficticios

                # Agregar información al conteo por género
                conteo_generos[genero_detectado]['vistas'] += vistas_simuladas
                conteo_generos[genero_detectado]['cantidad_videos'] += 1
                conteo_generos[genero_detectado]['canciones'].append({
                    'titulo': titulo,
                    'video_id': video_id,
                    'vistas': vistas_simuladas,
                    'likes': likes_simulados
                })
            except Exception as e:
                logger.error(f"Error al procesar canción: {e}")

    # Publicar los resultados en Kafka
    for genero, datos in conteo_generos.items():
        datos['canciones'] = sorted(datos['canciones'], key=lambda x: x['vistas'], reverse=True)[:10]  # Top 10 canciones
        mensaje = {'genero': genero, 'cantidad_videos': datos['cantidad_videos'], 'vistas': datos['vistas'], 'canciones': datos['canciones']}
        producer.send(TOPICO, mensaje)
        logger.info(f"Publicado en Kafka: {mensaje}")

logger.info("Extracción y publicación en Kafka completadas.")
