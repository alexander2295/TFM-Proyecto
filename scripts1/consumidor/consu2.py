import os
import json
import logging
import pymysql
import re
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Configuración de logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Cargar variables de entorno
load_dotenv(dotenv_path='C:/Users/Usuario/Desktop/TFM_Poryecto/.env')

# Configuración de Kafka
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPICO = 'artistasgenero'  # Asegurar que coincide con el tópico del reproductor

# Configuración de MySQL
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')

# Conectar a MySQL
def conectar_mysql():
    try:
        conexion = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            cursorclass=pymysql.cursors.DictCursor
        )
        logger.info("✅ Conectado a MySQL exitosamente.")
        return conexion
    except pymysql.MySQLError as e:
        logger.error(f"❌ Error al conectar a MySQL: {e}")
        return None

# Crear la tabla si no existe
def crear_tabla_generos(conexion):
    query = """
    CREATE TABLE IF NOT EXISTS generos_musicales (
        id INT AUTO_INCREMENT PRIMARY KEY,
        genero VARCHAR(100),
        cantidad_videos INT,
        vistas BIGINT,
        likes BIGINT,
        anios TEXT,
        titulos TEXT,
        fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """
    try:
        with conexion.cursor() as cursor:
            cursor.execute(query)
        conexion.commit()
        logger.info("✅ Tabla `generos_musicales` creada o verificada correctamente.")
    except pymysql.MySQLError as e:
        logger.error(f"❌ Error al crear la tabla en MySQL: {e}")

# Función de limpieza de datos
def limpiar_datos(datos):
    """
    Limpia y valida los datos recibidos de Kafka antes de insertarlos en MySQL.
    """
    try:
        # Validar que los campos esenciales existen y tienen valores válidos
        if not datos.get('genero') or not isinstance(datos['genero'], str):
            logger.warning(f"⚠️ Género inválido: {datos.get('genero')}")
            return None
        
        if not isinstance(datos.get('cantidad_videos'), int) or datos['cantidad_videos'] < 0:
            logger.warning(f"⚠️ Cantidad de videos inválida: {datos.get('cantidad_videos')}")
            return None
        
        if not isinstance(datos.get('vistas'), int) or datos['vistas'] < 0:
            logger.warning(f"⚠️ Número de vistas inválido: {datos.get('vistas')}")
            return None
        
        if not isinstance(datos.get('likes'), int) or datos['likes'] < 0:
            logger.warning(f"⚠️ Número de likes inválido: {datos.get('likes')}")
            return None
        
        # Limpiar títulos eliminando caracteres extraños
        datos['titulos'] = [re.sub(r'[^a-zA-Z0-9\s]', '', titulo) for titulo in datos.get('titulos', []) if isinstance(titulo, str)]

        # Verificar que la lista de años sea válida
        if not isinstance(datos.get('anios'), list) or not all(isinstance(a, int) for a in datos['anios']):
            logger.warning(f"⚠️ Lista de años inválida: {datos.get('anios')}")
            return None

        # Convertir listas a JSON para almacenamiento en MySQL
        datos['anios'] = json.dumps(datos['anios'])
        datos['titulos'] = json.dumps(datos['titulos'])

        return datos

    except Exception as e:
        logger.error(f"❌ Error en la limpieza de datos: {e}")
        return None

# Insertar datos limpios en MySQL
def insertar_datos_genero(conexion, datos):
    query = """
    INSERT INTO generos_musicales (genero, cantidad_videos, vistas, likes, anios, titulos)
    VALUES (%s, %s, %s, %s, %s, %s);
    """
    valores = (
        datos['genero'],
        datos['cantidad_videos'],
        datos['vistas'],
        datos['likes'],
        datos['anios'],  # Se guarda como JSON
        datos['titulos']  # Se guarda como JSON
    )
    try:
        with conexion.cursor() as cursor:
            cursor.execute(query, valores)
        conexion.commit()
        logger.info(f"✅ Datos insertados en MySQL: {datos}")
    except pymysql.MySQLError as e:
        logger.error(f"❌ Error al insertar datos en MySQL: {e}")

# Consumir datos de Kafka
def consumir_kafka():
    try:
        consumer = KafkaConsumer(
            TOPICO,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        logger.info("✅ Consumidor Kafka iniciado, esperando mensajes...")

        conexion = conectar_mysql()
        if conexion:
            crear_tabla_generos(conexion)

        for mensaje in consumer:
            datos = mensaje.value
            logger.info(f"📨 Mensaje recibido: {datos}")

            # Limpiar y validar datos antes de insertar
            datos_limpios = limpiar_datos(datos)
            if datos_limpios:
                insertar_datos_genero(conexion, datos_limpios)
            else:
                logger.warning(f"⚠️ Datos inválidos descartados: {datos}")

    except Exception as e:
        logger.error(f"❌ Error en el consumidor Kafka: {e}")

if __name__ == "__main__":
    consumir_kafka()
