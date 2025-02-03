import os
import json
import logging
import pymysql
from kafka import KafkaConsumer
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)

# Obtener variables de entorno
KAFKA_BROKER = os.getenv('KAFKA_BROKER')
TOPICO = os.getenv('TOPICO', 'generos_mas_escuchados')

MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = int(os.getenv('MYSQL_PORT', 3306))
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DB = os.getenv('MYSQL_DB')

# Validar configuración
if not KAFKA_BROKER:
    raise ValueError("❌ La variable de entorno 'KAFKA_BROKER' no está configurada.")
if not MYSQL_HOST or not MYSQL_USER or not MYSQL_PASSWORD or not MYSQL_DB:
    raise ValueError("❌ Variables de entorno de MySQL no configuradas correctamente.")

# Conectar a MySQL
def conectar_mysql():
    try:
        conexion = pymysql.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            charset='utf8mb4',
            cursorclass=pymysql.cursors.DictCursor
        )
        logging.info("✅ Conexión a MySQL establecida correctamente.")
        return conexion
    except pymysql.MySQLError as e:
        logging.error(f"❌ Error al conectar a MySQL: {e}")
        raise

# Crear la tabla si no existe
def crear_tabla():
    conexion = conectar_mysql()
    with conexion.cursor() as cursor:
        sql = """
        CREATE TABLE IF NOT EXISTS generos_mas_escuchados (
            id INT AUTO_INCREMENT PRIMARY KEY,
            cancion VARCHAR(255),
            artista VARCHAR(255),
            oyentes INT,
            enlace VARCHAR(500),
            generos TEXT,
            fecha TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        cursor.execute(sql)
        conexion.commit()
    conexion.close()
    logging.info("✅ Tabla 'generos_mas_escuchados' verificada/creada en MySQL.")

# Insertar datos en MySQL
def insertar_en_mysql(datos):
    conexion = conectar_mysql()
    with conexion.cursor() as cursor:
        sql = """
        INSERT INTO generos_mas_escuchados (cancion, artista, oyentes, enlace, generos)
        VALUES (%s, %s, %s, %s, %s);
        """
        try:
            cursor.execute(sql, (
                datos.get('cancion', 'Desconocido'),
                datos.get('artista', 'Desconocido'),
                datos.get('oyentes', 0),
                datos.get('enlace', ''),
                json.dumps(datos.get('generos', []))  # Guardar géneros como JSON
            ))
            conexion.commit()
            logging.info(f"✅ Datos insertados en MySQL: {datos}")
        except pymysql.MySQLError as e:
            logging.error(f"❌ Error al insertar en MySQL: {e}")
        finally:
            conexion.close()

# Configurar consumidor de Kafka
def iniciar_consumidor():
    try:
        consumer = KafkaConsumer(
            TOPICO,
            bootstrap_servers=KAFKA_BROKER,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            enable_auto_commit=True
        )
        logging.info(f"🔄 Consumidor Kafka suscrito al tópico: {TOPICO}")

        for mensaje in consumer:
            datos = mensaje.value
            logging.info(f"📥 Mensaje recibido de Kafka: {datos}")

            # Verificar que los datos sean válidos antes de insertarlos en MySQL
            if 'cancion' in datos and 'artista' in datos and 'oyentes' in datos:
                insertar_en_mysql(datos)
            else:
                logging.warning(f"⚠️ Datos inválidos recibidos: {datos}")

    except Exception as e:
        logging.error(f"❌ Error en el consumidor de Kafka: {e}")

if __name__ == "__main__":
    logging.info("🚀 Iniciando consumidor de géneros más escuchados...")
    crear_tabla()  # Asegurar que la tabla existe
    iniciar_consumidor()
