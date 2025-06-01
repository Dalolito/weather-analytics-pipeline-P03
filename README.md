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
Proyecto de automatización del proceso de captura, ingesta, procesamiento y salida de datos meteorológicos para análisis avanzado usando arquitectura batch para big data.

## 1.2 Aspectos NO cumplidos o desarrollados

## 2. Información general del proyecto

### Fuente de Datos: Open-Meteo

- **API**: https://api.open-meteo.com/v1/forecast
- **Datos Históricos**: https://archive-api.open-meteo.com/v1/archive
- **Variables**: Temperatura, precipitación, humedad, presión, viento

### Arquitectura del Sistema

- **Raw Zone**: Datos crudos de APIs y base de datos
- **Trusted Zone**: Datos procesados y limpios
- **Refined Zone**: Datos analíticos y modelos ML

### Flujo del Proyecto 

### Diagrama de Arquitectura del Sistema

## ¿Qué implementamos?

### Infraestructura base:

### Componentes de software:

### Funcionalidades de la aplicación:

### Características técnicas:

### Estructura del proyecto

```
Bookstore-P02/
├── applications/ 
│   ├── setup_athena.py                  
│   └── weaher_api.py
├── automation/ 
│   └── orchestrator.py
├── config/ 
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
├── tests/
│   ├── test_aws_connection.py
│   ├── tests_connections.py
│   ├── validate_pipeline.py  
│   └── validate_pipeline_improved.py
├── requirements.txt
├── run_project.py
├── verify_project_setup.py
└── README.md
```

## 3. Descripción del ambiente de desarrollo y técnico

### Lenguajes, librerías y tecnologías usadas

### Compilación y ejecución

## 4. Descripción del ambiente de EJECUCIÓN (en producción)

## Quick Start

1. Configurar AWS Academy credentials
2. Ejecutar `python infrastructure/setup_s3_buckets.py`
3. Configurar RDS MySQL
4. Ejecutar `python infrastructure/setup_database.py`
5. Ejecutar `python tests/test_connections.py`
6. Ejecutar `python ingesta/fetch_openmeteo_api.py`

## Estructura del Proyecto
