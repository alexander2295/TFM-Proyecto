import os
import json
import logging
import requests
from kafka import KafkaProducer
from dotenv import load_dotenv

# Cargar el archivo .env
load_dotenv()

# Configurar el logging
logging.basicConfig(level=logging.INFO)

# Obtener las variables de entorno
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
LAST_FM_API_KEY = os.getenv('LAST_FM_API_KEY')
TOPICO = os.getenv('TOPICO', 'generos_mas_escuchados')

# Verificar que las variables de entorno están configuradas
if not KAFKA_BROKER:
    raise ValueError("❌ La variable de entorno 'KAFKA_BROKER' no está configurada.")
if not LAST_FM_API_KEY:
    raise ValueError("❌ La variable de entorno 'LAST_FM_API_KEY' no está configurada.")

# Configurar el productor de Kafka
try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    logging.info("✅ Productor Kafka configurado correctamente.")
except Exception as e:
    logging.error(f"❌ Error al configurar el productor Kafka: {e}")
    raise

# Función para enviar mensajes a Kafka
def enviar_mensaje_a_kafka(topico, mensaje):
    try:
        producer.send(topico, mensaje)
        producer.flush()
        logging.info(f"📨 Mensaje enviado a Kafka: {mensaje}")
    except Exception as e:
        logging.error(f"❌ Error al enviar mensaje a Kafka: {e}")

# Función para obtener las canciones más escuchadas desde Last.fm
def extraer_canciones_mas_escuchadas(pais, mes, anio):
    """
    Extrae las canciones más escuchadas desde Last.fm para un país, mes y año específicos.
    """
    url = "http://ws.audioscrobbler.com/2.0/"
    params = {
        'method': 'geo.getTopTracks',
        'country': pais,
        'api_key': LAST_FM_API_KEY,
        'format': 'json',
        'limit': 50
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"
    }

    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()

        if 'tracks' in data and 'track' in data['tracks']:
            return data['tracks']['track']
        else:
            logging.warning(f"⚠️ No se encontraron datos para {pais} en {mes}-{anio}.")
            return []
    except requests.exceptions.HTTPError as e:
        logging.error(f"❌ Error HTTP {e.response.status_code}: {e}")
        return []
    except requests.exceptions.RequestException as e:
        logging.error(f"❌ Error de conexión: {e}")
        return []

# Procesar datos y enviarlos a Kafka
def procesar_y_enviar_datos(pais, anio, meses):
    """
    Procesa datos por país, año y una lista de meses, y los envía a Kafka.
    """
    for mes in meses:
        logging.info(f"🔍 Procesando datos para {mes} {anio} en {pais}...")
        canciones = extraer_canciones_mas_escuchadas(pais, mes, anio)
        
        for cancion in canciones:
            mensaje = {
                'cancion': cancion.get('name'),
                'artista': cancion.get('artist', {}).get('name'),
                'oyentes': int(cancion.get('listeners', 0)),
                'enlace': cancion.get('url'),
                'generos': []  # Se pueden completar con otra API si es necesario
            }
            enviar_mensaje_a_kafka(TOPICO, mensaje)

if __name__ == "__main__":
    PAIS = "Ecuador"
    ANIO = 2024
    MESES = ['January', 'February', 'March', 'April', 'May', 'June', 
             'July', 'August', 'September', 'October', 'November', 'December']
    
    logging.info(f"🎶 Iniciando extracción de géneros y canciones más escuchadas en {PAIS} para el año {ANIO}...")
    procesar_y_enviar_datos(PAIS, ANIO, MESES)
    logging.info("✅ Proceso completado exitosamente.")
