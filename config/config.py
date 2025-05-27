import os
from dotenv import load_dotenv

load_dotenv()

# Open-Meteo Configuration
OPENMETEO_BASE_URL = "https://api.open-meteo.com/v1"
OPENMETEO_ARCHIVE_URL = "https://archive-api.open-meteo.com/v1"

# Coordenadas de ciudades colombianas
CITIES = {
    'bogota': {'lat': 4.6097, 'lon': -74.0817, 'name': 'Bogotá'},
    'medellin': {'lat': 6.2518, 'lon': -75.5636, 'name': 'Medellín'},
    'cali': {'lat': 3.4516, 'lon': -76.5320, 'name': 'Cali'},
    'cartagena': {'lat': 10.3910, 'lon': -75.4794, 'name': 'Cartagena'},
    'barranquilla': {'lat': 10.9639, 'lon': -74.7964, 'name': 'Barranquilla'}
}

# Variables meteorológicas a extraer
WEATHER_VARIABLES = [
    'temperature_2m_max',
    'temperature_2m_min', 
    'temperature_2m_mean',
    'precipitation_sum',
    'windspeed_10m_max',
    'relative_humidity_2m_mean',
    'surface_pressure_mean'
]

# AWS Configuration (se carga desde variables de entorno)
AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
S3_BUCKET_RAW = os.getenv('S3_BUCKET_RAW')  # Cargar desde buckets.json

# Database Configuration
DB_CONFIG = {
    'host': os.getenv('DB_HOST'),
    'user': os.getenv('DB_USER', 'admin'),
    'password': os.getenv('DB_PASSWORD'),
    'database': os.getenv('DB_NAME', 'weather_data')
}