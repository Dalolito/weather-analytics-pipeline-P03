# Weather Analytics Pipeline - 20250601

## Estructura de Datos

Esta carpeta contiene la estructura local de datos para el proyecto Weather Analytics Pipeline.

### Fecha de creación: 2025-06-01 15:10:11

### Estructura de carpetas:

```
weather-analytics-pipeline-20250601/
├── raw/                          # Datos crudos
│   ├── weather-api/             # Datos de API OpenMeteo
│   ├── weather-historical/      # Datos históricos
│   ├── database/               # Datos de base de datos
│   └── logs/                   # Logs de ingesta
├── trusted/                     # Datos procesados y limpios
│   ├── weather_data/           # Datos meteorológicos procesados
│   ├── weather_stations/       # Estaciones meteorológicas
│   ├── climate_events/         # Eventos climáticos
│   └── integrated_weather_data/ # Datos integrados
├── refined/                     # Datos analíticos y resultados
│   ├── temperature_trends_monthly/
│   ├── extreme_weather_events/
│   ├── annual_weather_summary/
│   ├── ml_model_metrics/
│   ├── temperature_predictions/
│   ├── weather_forecasts/
│   ├── athena-results/
│   └── validation_reports/
├── scripts/                     # Scripts y configuraciones
│   ├── spark-jobs/             # Jobs de Spark
│   ├── bootstrap/              # Scripts de bootstrap
│   └── logs/                   # Logs de procesamiento
└── metadata.json               # Metadatos del proyecto
```

### Uso:

1. **Ingesta de datos**: Los scripts de ingesta guardarán automáticamente los datos tanto en S3 como en las carpetas locales correspondientes.

2. **Procesamiento**: Los datos procesados se almacenarán en las carpetas `trusted/` y `refined/`.

3. **Logs**: Todos los logs se guardan en las carpetas `logs/` respectivas.

### Sincronización con S3:

- Los datos se almacenan simultáneamente en S3 y localmente
- Los buckets S3 están configurados en `config/buckets.json`
- La sincronización es automática durante los procesos de ingesta

### Archivos importantes:

- `metadata.json`: Contiene información del proyecto y configuración
- `config/buckets.json`: Configuración de buckets S3 (en directorio raíz)

Para más información, consulta la documentación del proyecto.
