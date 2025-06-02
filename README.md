# Tópicos Especiales en Telemática
## Proyecto 03, Proyecto de Weather Analytics con Big Data 

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

## 1. Descripción de la actividad
Este proyecto implementa una arquitectura batch completa de Big Data para análisis meteorológico usando tecnologías AWS y Apache Spark. El sistema automatiza el proceso completo desde la captura de datos hasta los modelos predictivos y consultas SQL.
### Objetivos:

✅ Captura automática de datos meteorológicos desde API OpenMeteo y base de datos MySQL

✅ Ingesta automatizada hacia buckets S3 organizados por zonas (Raw, Trusted, Refined)

✅ Procesamiento ETL con Apache Spark en clúster EMR

✅ Análisis descriptivo y modelos de machine learning

✅ Consultas SQL mediante Amazon Athena

✅ API REST para acceso a resultados

### Problemática analizada:
Análisis y predicción de patrones meteorológicos en ciudades colombianas para:

* Identificar tendencias climáticas por ciudad y temporada
* Detectar eventos meteorológicos extremos
* Generar pronósticos basados en datos históricos
* Proporcionar datos accionables para la toma de decisiones

## 1.2 Aspectos NO cumplidos o desarrollados
Tuvimos limitaciones con:

* **AWS Academy:** Permisos limitados para algunos servicios avanzados
* **API Gateway:** Configuración manual requerida (instrucciones proporcionadas)
* **Machine Learning:** Implementación simplificada debido a restricciones de librerías en EMR
  
## 2. Información general del proyecto


### 📊 Fuente de Datos: Open-Meteo

* **API Principal:** https://api.open-meteo.com/v1/forecast
* **Datos Históricos:** https://archive-api.open-meteo.com/v1/archive
* **Ciudades analizadas:** Bogotá, Medellín, Cali, Cartagena, Barranquilla

**Variables meteorológicas:**

* Temperatura (máxima, mínima, promedio)
* Precipitación
* Humedad relativa
* Velocidad del viento
* Presión atmosférica


### Base de Datos MySQL (RDS)

Estaciones meteorológicas: Metadatos de ubicaciones
Eventos climáticos históricos: Registro de eventos extremos
Umbrales de alerta: Configuraciones para detección de anomalías

### Arquitectura del Sistema

- **Raw Zone**: Datos crudos de APIs y base de datos
- **Trusted Zone**: Datos procesados y limpios
- **Refined Zone**: Datos analíticos y modelos ML

### Flujo del Proyecto 

**1. Captura:** APIs y BD → JSON crudo

**2. Ingesta:** Almacenamiento automático en S3 Raw
   
**3.  ETL:** Spark procesa y limpia → Parquet en S3 Trusted
   
**4. Analytics:** Análisis estadístico → Parquet en S3 Refined
   
**5. ML:** Modelos predictivos simples → Métricas y pronósticos

**6. Consultas:** Athena + API para acceso final

### Diagrama de Arquitectura del Sistema

![Image](https://github.com/user-attachments/assets/84b1592f-a493-4401-84bf-7565e5576d01)

## ¿Qué implementamos?

### Infraestructura base:

* 4 buckets S3 organizados por zonas de datos
* Clúster EMR con Apache Spark (Master + Worker)
* Base de datos RDS MySQL con datos simulados
* Amazon Athena configurado con 7 tablas externas
* Roles y permisos IAM para integración de servicios

### Componentes de software:

* Scripts de ingesta automática desde múltiples fuentes
* Jobs de Spark para ETL, analytics y ML
* Orquestador para coordinación del pipeline
* API Lambda para consultas REST
* Scripts de configuración de infraestructura AWS
* Suite de validación y testing automatizado

### Funcionalidades de la aplicación:

* Pipeline completamente automatizado de datos meteorológicos
* Análisis de tendencias mensuales por ciudad
* Detección de eventos extremos (olas de calor, lluvias intensas)
* Pronósticos básicos basados en promedios históricos
* Consultas SQL interactivas via Athena
* API REST con endpoints para diferentes tipos de análisis
* Visualización con gráficas en Google Colab con los metadatos extraídos desde OpenMeteo

### Características técnicas:

* Procesamiento distribuido con Apache Spark
* Almacenamiento columnar optimizado (Parquet)
* Particionado inteligente por año/mes
* Manejo robusto de errores y recuperación
* Escalabilidad horizontal en EMR
* Compresión automática de datos

### Estructura del proyecto

```
Bookstore-P02/
├── applications/ 
│   ├── setup_athena.py                  
│   └── weaher_api.py
├── automation/ 
│   └── orchestrator.py
├── config/
│   ├── bucket.json  
│   └── config.py
├── infrastructure/
│   ├── setup_database.py
│   ├── setup_emr_cluster.py
│   ├── setup_emr_cluster_improved.py
│   └──setup_e3_buckets.py  
├── ingesta/
│   ├── fetch_database.py
│   └── fetch_openmeteo_api.py  
├── spark_jobs/
│   ├── analythics_weather_trends.py
│   ├── etl_weather_data.py
│   ├── ml_weather_prediction.py  
│   └── test_spark_jobs.py
├── scripts/
│   ├── raw/
│   ├── trusted/
│   ├── refined/ 
│   └── scripts/
├── tests/
│   ├── test_aws_connection.py
│   ├── tests_connections.py
│   ├── validate_pipeline.py  
│   └── validate_pipeline_improved.py
├── visualizations/
│   ├── notebooks/
│         └── Colab1/  
├── requirements.txt
├── run_project.py
├── verify_project_setup.py
├── .env
└── README.md
```

## 3. Descripción del ambiente de desarrollo y técnico

### Lenguajes, librerías y tecnologías usadas

**Lenguajes de programación:**
|Lenguaje|Justificación|
|--|--|
| Python 3.8+: | Lenguaje principal del proyecto |
|SQL:| Para consultas en Athena y configuración de BD|
|JSON:| Formato de intercambio de datos|

**Librerías principales:**
|python| Datos y análisis|
|--|--|
|pandas| Manipulación de datos|
|pyspark |Procesamiento distribuido con Spark|

| AWS SDK | Expliación  |
|--|--|
|boto3 |  SDK de AWS para Python|
|requests|HTTP requests para APIs|

**Base de datos:**
* mysql-connector-python: Para conexión a MySQL/RDS

**Utilidades**:
| Librería | Explicación |
|--|--|
|python-dotenv |Manejo de variables de entorno|
|pytest    |Testing framework|
|jupyter  | Notebooks (desarrollo)|

**Herramientas de AWS:**
| Herramienta | Explicación  
|--|--|
|Amazon S3:| Data Lake con 4 buckets organizados por zonas|
|Amazon EMR:| Clúster Apache Spark para procesamiento distribuido|
|Amazon RDS MySQL:| Base de datos relacional|
|Amazon Athena:| Motor SQL para consultas sobre datos procesados|
|AWS Lambda:| Functions para API REST|
|AWS IAM:| Gestión de permisos y roles|


### Compilación y ejecución

**Clonar repositorio:**

```bash
git clone https://github.com/Dalolito/weather-analytics-pipeline-P03.git
cd weather-analytics-pipeline-P03
```

**Crear entorno virtual:**

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate   # Windows
```

**Instalar dependencias:**

```bash
pip install -r requirements.txt
```

**Configurar credenciales AWS:**

```bash
# Crear archivo .env
AWS_DEFAULT_REGION=us-east-1
AWS_ACCESS_KEY_ID=tu_access_key
AWS_SECRET_ACCESS_KEY=tu_secret_key
AWS_SESSION_TOKEN=tu_session_token

# Database 
DB_HOST=tu-rds-endpoint.us-east-1.rds.amazonaws.com
DB_USER=admin
DB_PASSWORD=tu_password
DB_NAME=weather_data
```

**Ejecución del proyecto:**

```bash
# 1. Verificar configuración completa
python verify_project_setup.py
```

```bash
# 2. Configurar infraestructura AWS
python infrastructure/setup_s3_buckets.py
python infrastructure/setup_emr_cluster_improved.py
```

```bash
# 3. Ejecutar pipeline completo
python run_project.py --step full-pipeline --cluster-id CLUSTER_ID
```

```bash
# 4. Configurar Athena para consultas
python run_project.py --step setup-athena
```

```bash
# 5. Validar resultados
python tests/validate_pipeline_improved.py
```

## 4. Descripción del ambiente de EJECUCIÓN (en producción)

## Quick Start

1. Configurar AWS Academy credentials
2. Ejecutar `python infrastructure/setup_s3_buckets.py`
3. Configurar RDS MySQL
4. Ejecutar `python infrastructure/setup_database.py`
5. Ejecutar `python tests/test_connections.py`
6. Ejecutar `python ingesta/fetch_openmeteo_api.py`

## Referencias
