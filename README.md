📌 TFM-Proyecto
Proyecto de análisis de datos de YouTube con extracción, procesamiento y almacenamiento en MySQL utilizando Kafka.

📖 Índice
1. 🔧 Requisitos previos
2. 📂 Estructura del proyecto
3. ⚙️ Instalación
4. 🚀 Ejecución
5. 📡 Configuración de Kafka
6. 💾 Base de Datos MySQL
7. 📢 Pipeline de Datos

🔧 Requisitos previos
Antes de empezar, asegúrate de tener instalado:
•	✅ Python 3.10 o superior
•	✅ Docker y Docker Compose (para ejecutar Kafka)
•	✅ MySQL Server
•	✅ Git (para clonar el repositorio)



📂 Estructura del proyecto
TFM-Proyecto/
│── airflow/                # Configuración de Apache Airflow
│── data/                   # Datos procesados
│── prometheus/             # Configuración de Prometheus (Opcional)
│── reportes/               # Reportes y análisis de datos
│── scripts1/               # Código de los reproductores y consumidores
│   ├── reproductores/      # Scripts para extraer datos de YouTube
│   │   ├── reproductor1.py
│   │   ├── reproductor2.py
│   │   ├── reproductor3.py
│   ├── consumidor/         # Scripts para procesar y almacenar datos en MySQL
│   │   ├── consumidor1.py
│   │   ├── consumidor2.py
│   │   ├── consumidor3.py
│── docker-compose.yml      # Configuración de Kafka y MySQL en Docker
│── pipeline_data.py        # Pipeline de datos (ejecución automática)
│── requirements.txt        # Dependencias del proyecto
│── README.md               # Documentación del proyecto
│── .gitignore              # Archivos a ignorar en GitHub

⚙️ Instalación
1️⃣ Clonar el repositorio
```sh
git clone https://github.com/alexander2295/TFM-Proyecto.git
cd TFM-Proyecto
```
2️⃣ Crear y activar un entorno virtual
```sh
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate  # Windows
```
3️⃣ Instalar dependencias
```sh
pip install -r requirements.txt
```
🚀 Ejecución
1️⃣ **Iniciar Kafka y MySQL con Docker**
```sh
docker-compose up -d
```
2️⃣ **Ejecutar los scripts de extracción de datos**
```sh
python scripts1/reproductores/reproductor1.py
```
3️⃣ **Ejecutar el consumidor para procesar y almacenar los datos**
```sh
python scripts1/consumidor/consumidor1.py
```
4️⃣ **Ejecutar el pipeline de datos**
```sh
python pipeline_data.py
```

📡 Configuración de Kafka
Si necesitas crear un **tópico en Kafka**, usa:
```sh
docker exec -it kafka kafka-topics.sh --create --topic youtube_data --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```
Para listar tópicos:
```sh
docker exec -it kafka kafka-topics.sh --list --bootstrap-server kafka:9092
📡 Configuración de Kafka
Si necesitas crear un **tópico en Kafka**, usa:
```sh
docker exec -it kafka kafka-topics.sh --create --topic youtube_data --bootstrap-server kafka:9092 --partitions 1 --replication-factor 1
```
Para listar tópicos:
```sh
docker exec -it kafka kafka-topics.sh --list --bootstrap-server kafka:9092
```
💾 Base de Datos MySQL
Si MySQL está en **Docker**, accede con:
```sh
docker exec -it mysql mysql -u root -p
```
Para crear la base de datos:
```sql
CREATE DATABASE youtube_data;
```
Para verificar tablas:
```sql
USE youtube_data;
SHOW TABLES;
```
📢 Pipeline de Datos
Para automatizar la ejecución de los scripts de extracción y procesamiento de datos, ejecuta:
```sh
python pipeline_data.py


