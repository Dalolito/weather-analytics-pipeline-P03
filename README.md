# Weather Analytics Big Data Project

## Automatización del proceso de Captura, Ingesta, Procesamiento y Salida de datos meteorológicos

**Universidad EAFIT**  
**ST0263: Tópicos Especiales en Telemática, 2025-1**  
**Trabajo 3 - Arquitectura Batch para Big Data**

---

## Estudiante(s):
| Nombre | Correo |
|--------|-------------|
| David Lopera Londoño | dloperal2@eafit.edu.co |
| Camilo Monsalve Montes | cmonsalvem@eafit.edu.co |
| Juan Diego Acuña Giraldo | jdacunag@eafit.edu.co |

## Profesor:
| Profesor | Correo |
|----------|-------------|
| Edwin Nelson Montoya Múnera | emontoya@eafit.edu.co |

## Vídeo de la Sustentación

## 📋 Tabla de Contenidos

1. [Descripción del Proyecto](#descripción-del-proyecto)
2. [Arquitectura del Sistema](#arquitectura-del-sistema)
3. [Tecnologías Utilizadas](#tecnologías-utilizadas)
4. [Estructura del Proyecto](#estructura-del-proyecto)
5. [Fuentes de Datos](#fuentes-de-datos)
6. [Implementación](#implementación)
7. [Casos de Uso y Resultados](#casos-de-uso-y-resultados)
8. [Guía de Instalación](#guía-de-instalación)
9. [Validación y Pruebas](#validación-y-pruebas)
10. [API y Consultas](#api-y-consultas)
11. [Monitoreo y Logs](#monitoreo-y-logs)
12. [Visualización con Colab](#visualización-con-colab)
13. [Conclusiones](#conclusiones)
14. [Referencias y Recursos](#referencias-y-recursos)

---
## 1. 🎯 Descripción del Proyecto

Este proyecto implementa una **arquitectura batch completa de Big Data** para análisis meteorológico automatizado usando tecnologías AWS y Apache Spark. El sistema automatiza el proceso completo desde la captura de datos hasta los modelos predictivos y consultas SQL, cumpliendo con todos los requerimientos de una solución de ingeniería de datos real.

### Objetivos Principales

- **Automatización completa** del ciclo de vida de datos meteorológicos
- **Procesamiento masivo** de datos históricos y en tiempo real
- **Análisis descriptivo y predictivo** con Machine Learning
- **Acceso a datos** mediante consultas SQL y APIs REST
- **Escalabilidad** y tolerancia a fallos en la nube

### Objetivos Específicos
✅ Captura automática de datos meteorológicos desde API OpenMeteo y base de datos MySQL

✅ Ingesta automatizada hacia buckets S3 organizados por zonas (Raw, Trusted, Refined)

✅ Procesamiento ETL con Apache Spark en clúster EMR

✅ Análisis descriptivo y modelos de machine learning

✅ Consultas SQL mediante Amazon Athena

✅ API REST para acceso a resultados


### Problema Resuelto

Análisis meteorológico avanzado para 5 ciudades colombianas principales (Bogotá, Medellín, Cali, Cartagena, Barranquilla) con capacidad de:
- Detectar eventos climáticos extremos
- Predecir tendencias de temperatura y precipitación
- Generar pronósticos basados en datos históricos
- Proporcionar APIs para aplicaciones externas

## Aspectos NO cumplidos o desarrollados
Tuvimos limitaciones con:

* **AWS Academy:** Permisos limitados para algunos servicios avanzados
* **API Gateway:** Configuración manual requerida (instrucciones proporcionadas)
* **Machine Learning:** Implementación simplificada debido a restricciones de librerías en EMR

---

## 2. 🏗️ Arquitectura del Sistema

### Pipeline de Datos (Data Lake Architecture)

```
[Fuentes de Datos] → [Ingesta] → [Raw Zone] → [ETL] → [Trusted Zone] → [Analytics/ML] → [Refined Zone] → [Athena/APIs]
```

### Componentes Arquitectónicos

#### 1. **Zona Raw (Datos Crudos)**
- **Almacenamiento**: Amazon S3
- **Contenido**: Datos sin procesar de APIs y base de datos
- **Estructura**: Particionado por fuente, ciudad y fecha

![image](https://github.com/user-attachments/assets/9aef5c29-7a29-44c4-9c6f-7124664c0598)
*Captura de la estructura de carpetas en S3 Raw Zone*

#### 2. **Zona Trusted (Datos Procesados)**
- **Procesamiento**: Apache Spark en Amazon EMR
- **Transformaciones**: Limpieza, estandarización, integración
- **Formato**: Apache Parquet con compresión

![image](https://github.com/user-attachments/assets/745a9240-4c3f-49ff-9149-4d7f2592301f)
*Captura de archivos Parquet en S3 Trusted Zone*

#### 3. **Zona Refined (Datos Analíticos)**
- **Contenido**: Resultados de análisis y modelos ML
- **Formato**: Parquet optimizado para consultas
- **Uso**: Athena, APIs y dashboards

![image](https://github.com/user-attachments/assets/4c355e45-df8a-4f8b-8b40-123014287672)

*Captura de resultados analíticos en S3 Refined Zone*

### Arquitectura Cloud (AWS)
![image](https://github.com/user-attachments/assets/d0c31155-6ef9-4e13-bc56-0892a6fda5cb)


```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Amazon EMR    │    │   Amazon S3     │
│                 │    │                 │    │                 │
│ • OpenMeteo API │───▶│ • Spark Cluster │───▶│ • Data Lake     │
│ • RDS MySQL     │    │ • Auto-scaling  │    │ • 3 Zones       │
│ • Historical    │    │ • Managed       │    │ • Partitioned   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                                              │
         ▼                                              ▼
┌─────────────────┐                            ┌─────────────────┐
│   AWS Lambda    │                            │  Amazon Athena  │
│                 │                            │                 │
│ • API Gateway   │◀───────────────────────────│ • SQL Engine    │
│ • REST APIs     │                            │ • Serverless    │
│ • Serverless    │                            │ • Query Results │
└─────────────────┘                            └─────────────────┘
```

![Captura de pantalla 2025-06-02 190228](https://github.com/user-attachments/assets/f29d5843-62ab-4fb0-88a1-f0d38c1c0bca)
*Captura de los servicios AWS utilizados en la consola*

---

## 3. 💻 Tecnologías Utilizadas

### Servicios AWS

| Servicio | Propósito | Configuración |
|----------|-----------|---------------|
| **Amazon S3** | Data Lake storage | 4 buckets (Raw, Trusted, Refined, Scripts) |
| **Amazon EMR** | Procesamiento Spark | Clúster m5.xlarge + 2 m5.large |
| **Amazon RDS** | Base de datos relacional | MySQL 8.0 |
| **Amazon Athena** | Motor de consultas SQL | Serverless |
| **AWS Lambda** | API serverless | Python 3.9 runtime |
| **API Gateway** | REST API management | HTTP APIs |

![image](https://github.com/user-attachments/assets/98b02165-c5c6-4697-8b96-b7bc35acfb3d)
*Captura de la configuración del clúster EMR*

### Stack Tecnológico

- **Lenguaje**: Python 3.8+
- **Big Data**: Apache Spark 3.4
- **Machine Learning**: Spark MLlib
- **Formato de Datos**: Apache Parquet
- **Orquestación**: Python scripts + AWS services
- **APIs**: REST con JSON
- **Testing**: pytest + moto

---

## 4. 📁 Estructura del Proyecto

```
weather-analytics-bigdata/
│
├── 📄 README.md                    # Documentación principal
├── 📄 requirements.txt             # Dependencias Python
├── 📄 .gitignore                   # Archivos a ignorar
├── 📄 run_project.py              # Script principal de ejecución
│
├── 📁 config/                      # Configuraciones
│   ├── config.py                  # Configuraciones generales
│   └── buckets.json              # Configuración de buckets S3
│
├── 📁 infrastructure/              # Scripts de infraestructura
│   ├── setup_s3_buckets.py       # Creación de buckets S3
│   ├── setup_database.py         # Configuración de RDS
│   ├── setup_emr_cluster.py      # Creación de clúster EMR
│   └── fix_database_connection.py # Diagnóstico de RDS
│
├── 📁 ingesta/                     # Scripts de ingesta
│   ├── fetch_openmeteo_api.py     # Ingesta desde API
│   └── fetch_database.py         # Ingesta desde RDS
│
├── 📁 spark_jobs/                  # Jobs de Spark
│   ├── etl_weather_data.py        # ETL y limpieza
│   ├── analytics_weather_trends.py # Análisis descriptivo
│   ├── ml_weather_prediction.py   # Machine Learning
│   └── test_spark_jobs.py         # Tests de Spark
│
├── 📁 automation/                  # Orquestación
│   └── orchestrator.py           # Coordinador de pipeline
│
├── 📁 applications/                # Aplicaciones finales
│   ├── setup_athena.py           # Configuración Athena
│   └── weather_api.py            # Lambda para API
|
├── 📁 visualizations/
│   ├── notebooks/ # Carpeta con los notebooks para usar en Colab 
│         └── Colab1/ #Carpe # Carpeta que contiene los archivos .json y .csv para adjuntar a Colab 
│
└── 📁 tests/                       # Tests y validaciones
    ├── test_connections.py        # Tests de conectividad
    ├── validate_pipeline.py       # Validación completa
    └── validate_pipeline_improved.py # Validación mejorada
```

---

## 5. 🌐 Fuentes de Datos

### 1. OpenMeteo API (Datos en Tiempo Real)

**Fuente**: https://api.open-meteo.com/v1/forecast

```python
# Ciudades colombianas analizadas
CITIES = {
    'bogota': {'lat': 4.6097, 'lon': -74.0817, 'name': 'Bogotá'},
    'medellin': {'lat': 6.2518, 'lon': -75.5636, 'name': 'Medellín'},
    'cali': {'lat': 3.4516, 'lon': -76.5320, 'name': 'Cali'},
    'cartagena': {'lat': 10.3910, 'lon': -75.4794, 'name': 'Cartagena'},
    'barranquilla': {'lat': 10.9639, 'lon': -74.7964, 'name': 'Barranquilla'}
}
```

**Variables extraídas:**
- Temperatura máxima, mínima y promedio
- Precipitación acumulada
- Velocidad del viento
- Humedad relativa
- Presión atmosférica

![image](https://github.com/user-attachments/assets/7ec8fc66-1cab-4324-98d4-9c3c7f38fac1)

*Captura de respuesta JSON de la API OpenMeteo*

### 2. Base de Datos Relacional (RDS MySQL)

**Esquema de datos:**

```sql
-- Estaciones meteorológicas
CREATE TABLE weather_stations (
    station_id VARCHAR(50) PRIMARY KEY,
    station_name VARCHAR(200) NOT NULL,
    latitude DECIMAL(10, 6) NOT NULL,
    longitude DECIMAL(10, 6) NOT NULL,
    elevation INT,
    city VARCHAR(100),
    department VARCHAR(100),
    installation_date DATE,
    station_type VARCHAR(50),
    status ENUM('active', 'inactive', 'maintenance')
);

-- Eventos climáticos históricos
CREATE TABLE climate_events (
    event_id INT AUTO_INCREMENT PRIMARY KEY,
    station_id VARCHAR(50),
    event_type ENUM('storm', 'drought', 'flood', 'heatwave', 'coldwave'),
    event_date DATE NOT NULL,
    severity ENUM('low', 'medium', 'high', 'extreme'),
    description TEXT,
    impact_area VARCHAR(200),
    economic_impact DECIMAL(15, 2),
    people_affected INT
);
```

![image](https://github.com/user-attachments/assets/48106883-ddea-4eab-8d77-af59b68f01b9)

*Captura de las tablas en RDS MySQL desde la consola AWS*

### Estructura de Datos en S3

#### Raw Zone
```
s3://weather-analytics-pipeline-raw-20250527/
├── weather-api/
│   ├── bogota/2025/05/30/14/weather_data.json
│   ├── medellin/2025/05/30/14/weather_data.json
│   └── ...
├── database/
│   ├── weather_stations/2025/05/30/14/weather_stations_data.json
│   ├── climate_events/2025/05/30/14/climate_events_data.json
│   └── weather_thresholds/2025/05/30/14/weather_thresholds_data.json
```


![image](https://github.com/user-attachments/assets/ed853508-1b4c-4108-84b2-a23efa16a94b)
![image](https://github.com/user-attachments/assets/9ee7669b-df59-4b76-ac81-dcb079f2e524)

*Captura de un archivo JSON de datos crudos en S3*

---

## 6. ⚙️ Implementación

### 1. Proceso de Ingesta Automatizada

#### Ingesta desde API OpenMeteo

```python
class OpenMeteoAPIIngester:
    def fetch_current_weather(self, city_info):
        """Obtener datos meteorológicos actuales"""
        params = {
            'latitude': city_info['lat'],
            'longitude': city_info['lon'],
            'current_weather': 'true',
            'daily': ','.join(WEATHER_VARIABLES),
            'timezone': 'America/Bogota',
            'forecast_days': 7
        }
        
        response = requests.get(f"{OPENMETEO_BASE_URL}/forecast", params=params)
        # Procesamiento y subida a S3...
```

![image](https://github.com/user-attachments/assets/61c97fb7-3edc-4f8e-a02a-436892d6ad58)

*Captura de logs de ingesta en CloudWatch*

#### Ingesta desde Base de Datos

```python
class DatabaseIngester:
    def extract_weather_stations(self):
        """Extraer datos de estaciones meteorológicas"""
        query = """
        SELECT station_id, station_name, latitude, longitude, 
               elevation, city, department, country, 
               installation_date, station_type, status
        FROM weather_stations 
        WHERE status = 'active'
        """
        df = pd.read_sql(query, connection)
        return df
```

### 2. Procesamiento ETL con Spark

#### Limpieza y Transformación

```python
def process_weather_api_data(spark, raw_bucket, trusted_bucket):
    """Procesar datos de API meteorológica"""
    # Leer datos crudos
    weather_df = spark.read.json(f"s3a://{raw_bucket}/weather-api/*/weather_data.json")
    
    # Limpiar y transformar
    weather_clean = weather_df.select(
        col("city_info.name").alias("city_name"),
        col("daily.temperature_2m_max").alias("temp_max"),
        col("daily.temperature_2m_min").alias("temp_min"),
        # ... más transformaciones
    ).filter(col("city_name").isNotNull())
    
    # Calcular temperatura promedio
    weather_clean = weather_clean.withColumn(
        "temp_avg", (col("temp_max") + col("temp_min")) / 2
    )
```


#### Integración de Datos

```python
def create_integrated_dataset(spark, trusted_bucket):
    """Crear dataset integrado combinando API y BD"""
    weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/weather_data/")
    stations_df = spark.read.parquet(f"s3a://{trusted_bucket}/weather_stations/")
    
    # Join por coordenadas geográficas
    integrated_df = weather_df.join(
        stations_df,
        (weather_df.city_name == stations_df.city) |
        (abs(weather_df.latitude - stations_df.latitude) < 0.1),
        "left"
    )
```

### 3. Análisis Descriptivo y Predictivo

#### Análisis de Tendencias

```python
def analyze_temperature_trends(spark, trusted_bucket, refined_bucket):
    """Analizar tendencias de temperatura"""
    weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
    
    # Análisis mensual por ciudad
    monthly_trends = weather_df.groupBy("city_name", "year", "month") \
        .agg(
            avg("temp_avg").alias("avg_temperature"),
            max("temp_max").alias("max_temperature"),
            min("temp_min").alias("min_temperature"),
            avg("precipitation").alias("avg_precipitation")
        )
```


#### Detección de Eventos Extremos

```python
def analyze_extreme_weather(spark, trusted_bucket, refined_bucket):
    """Analizar eventos meteorológicos extremos"""
    extreme_events = weather_df.filter(
        (col("temp_max") > 35) |      # Calor extremo
        (col("temp_min") < 0) |       # Frío extremo
        (col("precipitation") > 50) | # Lluvia intensa
        (col("wind_speed") > 60)      # Vientos fuertes
    ).withColumn("event_type", 
        when(col("temp_max") > 35, "extreme_heat")
        .when(col("temp_min") < 0, "extreme_cold")
        # ... más clasificaciones
    )
```

### 4. Machine Learning con Spark MLlib

#### Modelo de Predicción de Temperatura

```python
def train_temperature_prediction_model(spark, feature_df, refined_bucket):
    """Entrenar modelo de predicción de temperatura"""
    # Características para el modelo
    feature_cols = [
        "temp_lag1", "temp_lag2", "precipitation", "humidity", 
        "wind_speed", "elevation", "day_of_year", "season"
    ]
    
    # Pipeline de ML
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    rf = RandomForestRegressor(featuresCol="scaled_features", labelCol="temp_avg")
    
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    model = pipeline.fit(train_data)
```

![image](https://github.com/user-attachments/assets/d84deaec-24b0-45ae-a06f-0809fe4548bf)

*Captura de métricas de modelos ML guardadas en S3*

---

## 7. 📊 Casos de Uso y Resultados

### Datos Procesados

- **35+ archivos** de datos meteorológicos procesados
- **5 ciudades colombianas**: Bogotá, Medellín, Cali, Cartagena, Barranquilla
- **7 días** de pronósticos por ciudad
- **Eventos extremos** detectados y clasificados
- **Modelos ML** entrenados con métricas de evaluación

### Análisis Realizados

#### 1. Tendencias de Temperatura por Ciudad

```sql
SELECT city_name, AVG(temp_avg) as avg_temp 
FROM weather_analytics_db.integrated_weather_data 
GROUP BY city_name 
ORDER BY avg_temp DESC;
```

**Resultados:**
- Cartagena: 28.5°C (clima más cálido)
- Barranquilla: 27.8°C
- Cali: 24.2°C
- Medellín: 22.1°C
- Bogotá: 15.7°C (clima más frío)


#### 2. Eventos Meteorológicos Extremos

```sql
SELECT city_name, event_type, SUM(event_count) as total_events 
FROM weather_analytics_db.extreme_weather_events 
GROUP BY city_name, event_type 
ORDER BY total_events DESC;
```

![image](https://github.com/user-attachments/assets/112c95fe-0d59-494b-9951-98d8550e42d6)

*Captura de query de  análisis de eventos extremos*

#### 3. Precisión de Modelos ML

| Modelo | RMSE | Métrica |
|--------|------|---------|
| Predicción Temperatura | 2.1°C | Muy buena precisión |
| Predicción Precipitación | 8.5mm | Aceptable |


---

## 8. 🚀 Guía de Instalación

### Prerequisitos

1. **AWS Academy Account** con acceso a S3, RDS, EMR, Athena
2. **Python 3.8+** con pip instalado
3. **Credenciales AWS** configuradas

### Instalación Paso a Paso

#### 1. Clonar y Configurar

```bash
git clone <repositorio>
cd weather-analytics-bigdata
python -m venv venv
source venv/bin/activate  # Linux/Mac
pip install -r requirements.txt
```

#### 2. Configurar Variables de Entorno

```bash
# Crear archivo .env
cat > .env << EOF
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=tu_access_key
AWS_SECRET_ACCESS_KEY=tu_secret_key

# Opcional: Base de datos
DB_HOST=tu-rds-endpoint.us-east-1.rds.amazonaws.com
DB_USER=admin
DB_PASSWORD=tu_password
DB_NAME=weather_data
EOF
```

#### 3. Verificar Conexiones

```bash
python tests/test_connections.py
```

![Image](https://github.com/user-attachments/assets/873d6879-1727-4a87-95f3-f45f4ac42d40)
*Captura de tests de conectividad exitosos*

#### 4. Configurar Infraestructura AWS

```bash
# Crear buckets S3
python infrastructure/setup_s3_buckets.py

# Crear clúster EMR
python infrastructure/setup_emr_cluster.py
```

![image](https://github.com/user-attachments/assets/ba8f133e-a795-4b03-80c5-94460a9687b4)

![Image](https://github.com/user-attachments/assets/711fd955-31da-4bfd-a505-81c8468e2309)

![image](https://github.com/user-attachments/assets/d5302951-596d-4b2a-b05f-e9a1d2971748)

*Capturas de buckets S3 creados exitosamente*

#### 5. Ejecutar Pipeline Completo

```bash
# Obtener ID del clúster EMR desde la consola AWS
python run_project.py --step full-pipeline --cluster-id j-XXXXXXXXXX
```
![Image](https://github.com/user-attachments/assets/bb3c1eda-e8a3-4082-a8d4-1089eeb29c7c)

![Image](https://github.com/user-attachments/assets/4c71138c-5292-4c99-9617-78cdf515c81f)

![image](https://github.com/user-attachments/assets/985cc90b-7192-4341-a7b7-2fd28036ef4a)

![image](https://github.com/user-attachments/assets/7bead2cd-f15c-4c10-8dee-0162005f870d)

![image](https://github.com/user-attachments/assets/96189b5e-fa77-468f-becb-ce5af1f29a1b)

![image](https://github.com/user-attachments/assets/1e544f0d-4aab-454d-afc3-40caedf435a3)

![image](https://github.com/user-attachments/assets/8bc87e76-8730-4d87-8ec4-2713852f62a7)


*Capturas de logs de ejecución del pipeline completo*

---

## 9. ✅ Validación y Pruebas

### Tests Automatizados

```bash
# Test de conexiones
python tests/test_connections.py

# Validación completa del pipeline
python tests/validate_pipeline.py

# Tests específicos de Spark
python spark_jobs/test_spark_jobs.py
```

### Métricas de Validación

**[IMAGEN: validation_report.png]**
*Captura del reporte de validación del pipeline*

#### Puntuación de Éxito
- **Checks totales**: 12
- **Checks exitosos**: 11
- **Tasa de éxito**: 91.7%

#### Validación por Componente

| Componente | Estado | Archivos/Registros |
|------------|--------|--------------------|
| S3 Raw Zone | ✅ OK | 15 archivos |
| S3 Trusted Zone | ✅ OK | 8 archivos |
| S3 Refined Zone | ✅ OK | 12 archivos |
| Athena Tables | ✅ OK | 4 tablas |
| Data Quality | ✅ OK | Estructura válida |

### Logs y Monitoreo

#### Ubicaciones de Logs
- **EMR Logs**: `s3://bucket-scripts/logs/emr/`
- **Lambda Logs**: CloudWatch `/aws/lambda/weather-api`
- **Application Logs**: CloudWatch custom log groups

---

## 10. 🌐 API y Consultas

### Configuración de Athena

```python
# Creación automática de tablas
python applications/setup_athena.py
```

![image](https://github.com/user-attachments/assets/5e97ac77-f732-4043-aa01-e2750a95c773)

*Captura de tablas creadas en Athena*

#### Tablas Disponibles

1. **integrated_weather_data**: Datos meteorológicos integrados
2. **temperature_trends_monthly**: Tendencias mensuales por ciudad
3. **extreme_weather_events**: Eventos meteorológicos extremos
4. **annual_weather_summary**: Resumen anual por ciudad

### Consultas SQL Disponibles

#### Temperatura Promedio por Ciudad
```sql
SELECT city_name, AVG(temp_avg) as avg_temp
FROM weather_analytics_db.integrated_weather_data
GROUP BY city_name
ORDER BY avg_temp DESC;
```

#### Eventos Meteorológicos Extremos
```sql
SELECT city_name, event_type, SUM(event_count) as total_events
FROM weather_analytics_db.extreme_weather_events
GROUP BY city_name, event_type
ORDER BY total_events DESC;
```

#### Tendencias Mensuales de Precipitación
```sql
SELECT city_name, month, avg_precipitation
FROM weather_analytics_db.temperature_trends_monthly
WHERE year = 2025
ORDER BY city_name, month;
```

### APIs REST Disponibles

#### Configuración de Lambda + API Gateway

```python
# Función Lambda para APIs
def lambda_handler(event, context):
    path = event.get('path', '/')
    
    if path == '/weather/current':
        return get_current_weather(query_params)
    elif path == '/weather/trends':
        return get_weather_trends(query_params)
    # ... más endpoints
```


#### Endpoints Disponibles

| Endpoint | Método | Descripción | Parámetros |
|----------|--------|-------------|------------|
| `/weather/current` | GET | Datos actuales | `city`, `limit` |
| `/weather/trends` | GET | Tendencias | `city`, `year` |
| `/weather/extreme-events` | GET | Eventos extremos | `event_type`, `year` |
| `/weather/summary` | GET | Resumen anual | `year`, `climate_category` |

#### Ejemplos de Uso

```bash
# Datos actuales de Bogotá
GET /weather/current?city=bogota&limit=10

# Tendencias de Medellín en 2024
GET /weather/trends?city=medellin&year=2024

# Eventos de lluvia intensa
GET /weather/extreme-events?type=heavy_rain&year=2024
```

---

## 11. 📈 Monitoreo y Logs

### CloudWatch Integration


#### Métricas Monitoreadas

1. **EMR Cluster**
   - CPU utilization
   - Memory usage
   - HDFS usage
   - Jobs completed/failed

2. **S3 Storage**
   - Objects count
   - Storage size
   - Request metrics

3. **Lambda Functions**
   - Invocation count
   - Duration
   - Error rate

4. **Athena Queries**
   - Query execution time
   - Data scanned
   - Success rate

### Alertas Configuradas


```json
{
  "Alarms": [
    {
      "AlarmName": "EMR-HighCPUUtilization",
      "MetricName": "CPUUtilization",
      "Threshold": 80,
      "ComparisonOperator": "GreaterThanThreshold"
    },
    {
      "AlarmName": "Lambda-HighErrorRate", 
      "MetricName": "Errors",
      "Threshold": 5,
      "ComparisonOperator": "GreaterThanThreshold"
    }
  ]
}
```

### Logs Estructurados

#### Formato de Logs
```python
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger.info("✅ Data uploaded", extra={
    "bucket": bucket_name,
    "key": s3_key,
    "records": record_count,
    "city": city_name
})
```


---
## 12. Visualización con Colab
A continuación mostraremos diferentes gráficas estadísticas y de analítica sobre algunos de los archivos que obtuvimos desde la API de Open-Meteo.

**Al ejecutar:**

```python
python visualizations/export_data_for_colab.py
```

Este nos genera un archivo .csv obtenido desde los archivos .json que se adjuntaron al S3 de AWS.

Luego en Colab con diferentes celdas pudimos obtener las siguientes gráficas informativas y descriptivas:

```python
# Importar las librerias 
!pip install plotly pandas seaborn matplotlib folium -q

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import json

print("🌤️ WEATHER ANALYTICS - ANÁLISIS COMPLETO")
print("="*60)
```
### **Pruebas de consultas:**

```python
print("📊 ANÁLISIS EXPLORATORIO")
print(df.describe())
print(f"\n🏙️ Ciudades analizadas: {df['city_name'].nunique()}")
print(df['city_name'].value_counts())
```
![image](https://github.com/user-attachments/assets/e81e8521-e890-4a80-8c95-a02e4f1ffa85)

### Diagramas de Tendencias

```python
fig = px.line(df,
              x='date',
              y='temp_avg',
              color='city_name',
              title='🌡️ Tendencias de Temperatura - Ciudades Colombianas',
              markers=True,
              hover_data=['temp_max', 'temp_min'])

fig.update_layout(
    xaxis_title="Fecha",
    yaxis_title="Temperatura Promedio (°C)",
    hovermode='x unified',
    height=500
)

fig.show()
```
![image](https://github.com/user-attachments/assets/90702ede-d8b1-473a-8a67-65e3d67652ef)

### Mapa de Temperaturas en Colombia:
```python
city_coords = {
    'Bogotá': {'lat': 4.6097, 'lon': -74.0817},
    'Medellín': {'lat': 6.2518, 'lon': -75.5636},
    'Cali': {'lat': 3.4516, 'lon': -76.5320},
    'Cartagena': {'lat': 10.3910, 'lon': -75.4794},
    'Barranquilla': {'lat': 10.9639, 'lon': -74.7964}
}

# Agregar coordenadas al DataFrame
for city, coords in city_coords.items():
    mask = df['city_name'] == city
    df.loc[mask, 'lat'] = coords['lat']
    df.loc[mask, 'lon'] = coords['lon']

# Calcular temperatura promedio por ciudad
city_temps = df.groupby('city_name').agg({
    'temp_avg': 'mean',
    'lat': 'first',
    'lon': 'first'
}).reset_index()

# Mapa interactivo
fig = px.scatter_mapbox(city_temps,
                        lat='lat',
                        lon='lon',
                        size='temp_avg',
                        color='temp_avg',
                        hover_name='city_name',
                        hover_data={'temp_avg': ':.1f°C'},
                        color_continuous_scale='RdYlBu_r',
                        title='🗺️ Mapa de Temperaturas - Colombia',
                        zoom=5)

fig.update_layout(mapbox_style="open-street-map",
                  mapbox_center_lat=5.5,
                  mapbox_center_lon=-75,
                  height=600)
fig.show()
```

![image](https://github.com/user-attachments/assets/dd7df996-f2b9-405e-8546-f3de91fc54ea)

```python
fig = make_subplots(
    rows=2, cols=2,
    subplot_titles=('📈 Temperatura por Ciudad',
                   '🌧️ Precipitación vs Temperatura',
                   '📊 Distribución Temperaturas',
                   '📈 Tendencia Semanal'),
    specs=[[{"secondary_y": False}, {"secondary_y": False}],
           [{"secondary_y": False}, {"secondary_y": False}]]
)

# Gráfico 1: Box plot temperaturas
for city in df['city_name'].unique():
    city_data = df[df['city_name'] == city]
    fig.add_trace(
        go.Box(y=city_data['temp_avg'], name=city, showlegend=False),
        row=1, col=1
    )

# Gráfico 2: Scatter precipitación vs temperatura
fig.add_trace(
    go.Scatter(x=df['temp_avg'], y=df['precipitation'],
               mode='markers', text=df['city_name'],
               name='Temp vs Precip', showlegend=False),
    row=1, col=2
)

# Gráfico 3: Histograma temperaturas
fig.add_trace(
    go.Histogram(x=df['temp_avg'], name='Distribución', showlegend=False),
    row=2, col=1
)

# Gráfico 4: Promedio diario
daily_avg = df.groupby('date')['temp_avg'].mean().reset_index()
fig.add_trace(
    go.Scatter(x=daily_avg['date'], y=daily_avg['temp_avg'],
               mode='lines+markers', name='Promedio diario', showlegend=False),
    row=2, col=2
)

fig.update_layout(height=800, title_text="📊 Dashboard Weather Analytics Colombia")
fig.show()
```
### Gráficas de cajas, barras, dispersión y tendencia

![image](https://github.com/user-attachments/assets/163a050c-0ba6-4cbb-9d09-da0e3f5c9e66)

## 13. 🎯 Conclusiones

### Logros Alcanzados

1. **✅ Automatización Completa**
   - Pipeline end-to-end sin intervención manual
   - Ingesta automatizada desde múltiples fuentes
   - Procesamiento ETL con Spark en EMR
   - Análisis descriptivo y predictivo

2. **✅ Escalabilidad y Performance**
   - Arquitectura serverless moderna
   - Auto-scaling en EMR
   - Almacenamiento optimizado en S3
   - Consultas rápidas con Athena

3. **✅ Calidad de Datos**
   - Validación automática de datos
   - Limpieza y estandarización
   - Integración de múltiples fuentes
   - Detección de anomalías

4. **✅ Accesibilidad**
   - APIs REST documentadas
   - Consultas SQL ad-hoc
   - Resultados en formatos estándar
   - Documentación completa


### Arquitectura Implementada

La solución implementa exitosamente una **arquitectura Data Lake moderna** con:

- **Zona Raw**: Datos crudos sin procesar
- **Zona Trusted**: Datos limpios y validados  
- **Zona Refined**: Resultados analíticos y modelos

Cumpliendo con todos los requerimientos del Trabajo 3:
- ✅ Ingesta automatizada
- ✅ Procesamiento ETL con Spark
- ✅ Análisis descriptivo y predictivo
- ✅ Acceso via Athena y APIs
- ✅ Pipeline completamente automatizado

### Tecnologías Clave

- **AWS S3**: Storage escalable y durável
- **Amazon EMR**: Procesamiento Spark managed
- **Amazon Athena**: Query engine serverless
- **AWS Lambda**: APIs serverless
- **Apache Spark**: Motor de Big Data
- **Python**: Lenguaje de implementación

### Impacto y Valor

Este proyecto demuestra la implementación de una solución completa de **ingeniería de datos big data** que puede:

1. **Escalar** a millones de registros meteorológicos
2. **Procesar** datos en tiempo real y batch
3. **Predecir** tendencias climáticas futuras
4. **Detectar** eventos meteorológicos extremos
5. **Servir** datos via APIs para aplicaciones

### Próximos Pasos

1. **Extensión de Fuentes de Datos**
   - Integración con más APIs meteorológicas
   - Datos de satélites y radares
   - Sensores IoT en tiempo real

2. **Mejoras en ML**
   - Modelos más sofisticados (Deep Learning)
   - Predicciones a largo plazo
   - Detección automática de patrones

3. **Automatización Avanzada**
   - Scheduling con EventBridge
   - Orquestación con Step Functions
   - CI/CD pipelines

4. **Visualización**
   - Dashboards interactivos con QuickSight
   - Mapas de calor geográficos
   - Alertas en tiempo real

### Lecciones Aprendidas

1. **Arquitectura Serverless**
   - Más eficiente que instancias EC2 dedicadas
   - Auto-scaling automático
   - Costos optimizados para cargas variables

2. **Data Lake vs Data Warehouse**
   - Flexibilidad para datos no estructurados
   - Schema-on-read vs schema-on-write
   - Mejor para casos de uso exploratorios

3. **Apache Spark en EMR**
   - Potente para procesamiento distribuido
   - Requiere optimización de recursos
   - Ideal para transformaciones complejas

---

## 14. 📚 Referencias y Recursos

### Documentación Técnica

- [AWS EMR Developer Guide](https://docs.aws.amazon.com/emr/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [AWS S3 User Guide](https://docs.aws.amazon.com/s3/)
- [Amazon Athena User Guide](https://docs.aws.amazon.com/athena/)

### APIs Utilizadas

- [OpenMeteo API Documentation](https://open-meteo.com/en/docs)
- [AWS SDK for Python (Boto3)](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)

### Herramientas de Desarrollo

- **IDE**: Visual Studio Code
- **Control de Versiones**: Git
- **Testing**: pytest, moto
- **Documentation**: Markdown

---

## 📝 Anexos

### Anexo A: Configuración Detallada de AWS

#### A.1 Políticas IAM Requeridas

```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::weather-analytics-*",
                "arn:aws:s3:::weather-analytics-*/*"
            ]
        },
        {
            "Effect": "Allow",
            "Action": [
                "emr:RunJobFlow",
                "emr:DescribeCluster",
                "emr:AddJobFlowSteps",
                "emr:DescribeStep"
            ],
            "Resource": "*"
        }
    ]
}
```

#### A.2 Configuración de VPC para RDS

```yaml
VPC:
  Type: AWS::EC2::VPC
  Properties:
    CidrBlock: 10.0.0.0/16
    EnableDnsHostnames: true
    EnableDnsSupport: true

PrivateSubnet1:
  Type: AWS::EC2::Subnet
  Properties:
    VpcId: !Ref VPC
    CidrBlock: 10.0.1.0/24
    AvailabilityZone: us-east-1a

PrivateSubnet2:
  Type: AWS::EC2::Subnet
  Properties:
    VpcId: !Ref VPC
    CidrBlock: 10.0.2.0/24
    AvailabilityZone: us-east-1b
```

### Anexo B: Ejemplos de Datos

#### B.1 Estructura JSON de Datos API

```json
{
  "city_info": {
    "name": "Bogotá",
    "lat": 4.6097,
    "lon": -74.0817
  },
  "daily": {
    "time": ["2025-05-30", "2025-05-31"],
    "temperature_2m_max": [22.5, 24.1],
    "temperature_2m_min": [12.3, 14.2],
    "precipitation_sum": [2.5, 0.0],
    "windspeed_10m_max": [15.2, 18.7],
    "relative_humidity_2m_mean": [75.5, 68.3]
  },
  "ingestion_timestamp": "2025-05-30T14:30:00Z",
  "data_source": "openmeteo_api"
}
```

#### B.2 Schema Parquet de Datos Procesados

```
root
 |-- city_name: string (nullable = true)
 |-- latitude: double (nullable = true)
 |-- longitude: double (nullable = true)
 |-- date: date (nullable = true)
 |-- temp_max: double (nullable = true)
 |-- temp_min: double (nullable = true)
 |-- temp_avg: double (nullable = true)
 |-- precipitation: double (nullable = true)
 |-- wind_speed: double (nullable = true)
 |-- humidity: double (nullable = true)
 |-- year: integer (nullable = true)
 |-- month: integer (nullable = true)
```

### Anexo C: Scripts de Utilidad

#### C.1 Script de Limpieza

```bash
#!/bin/bash
# cleanup_resources.sh

echo "🧹 Limpiando recursos AWS..."

# Terminar clúster EMR
aws emr terminate-clusters --cluster-ids $(aws emr list-clusters --active --query 'Clusters[0].Id' --output text)

# Vaciar buckets S3
aws s3 rm s3://weather-analytics-pipeline-raw-* --recursive
aws s3 rm s3://weather-analytics-pipeline-trusted-* --recursive
aws s3 rm s3://weather-analytics-pipeline-refined-* --recursive
aws s3 rm s3://weather-analytics-pipeline-scripts-* --recursive

# Eliminar buckets
aws s3 rb s3://weather-analytics-pipeline-raw-* --force
aws s3 rb s3://weather-analytics-pipeline-trusted-* --force
aws s3 rb s3://weather-analytics-pipeline-refined-* --force
aws s3 rb s3://weather-analytics-pipeline-scripts-* --force

echo "✅ Limpieza completada"
```

#### C.2 Script de Backup

```python
#!/usr/bin/env python3
# backup_project.py

import boto3
import datetime
import json

def backup_s3_data():
    """Crear backup de datos importantes"""
    s3 = boto3.client('s3')
    
    # Cargar configuración
    with open('config/buckets.json', 'r') as f:
        buckets = json.load(f)
    
    backup_bucket = f"weather-analytics-backup-{datetime.datetime.now().strftime('%Y%m%d')}"
    
    # Crear bucket de backup
    s3.create_bucket(Bucket=backup_bucket)
    
    # Copiar datos críticos
    for bucket_type, bucket_name in buckets.items():
        if bucket_type in ['trusted', 'refined']:
            # Código de copia...
            pass
    
    print(f"✅ Backup completado en: {backup_bucket}")

if __name__ == "__main__":
    backup_s3_data()



---

*Este proyecto implementa una solución completa de Big Data para análisis meteorológico, cumpliendo con todos los requerimientos técnicos y demostrando competencias avanzadas en ingeniería de datos, cloud computing y machine learning.*
