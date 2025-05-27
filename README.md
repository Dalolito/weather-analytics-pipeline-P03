# Weather Analytics Big Data Project

Proyecto de automatización del proceso de captura, ingesta, procesamiento y salida de datos meteorológicos para análisis avanzado usando arquitectura batch para big data.

## Fuente de Datos: Open-Meteo

- **API**: https://api.open-meteo.com/v1/forecast
- **Datos Históricos**: https://archive-api.open-meteo.com/v1/archive
- **Variables**: Temperatura, precipitación, humedad, presión, viento

## Arquitectura

- **Raw Zone**: Datos crudos de APIs y base de datos
- **Trusted Zone**: Datos procesados y limpios
- **Refined Zone**: Datos analíticos y modelos ML

## Quick Start

1. Configurar AWS Academy credentials
2. Ejecutar `python infrastructure/setup_s3_buckets.py`
3. Configurar RDS MySQL
4. Ejecutar `python infrastructure/setup_database.py`
5. Ejecutar `python tests/test_connections.py`
6. Ejecutar `python ingesta/fetch_openmeteo_api.py`

## Estructura del Proyecto