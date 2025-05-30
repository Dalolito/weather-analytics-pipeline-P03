import requests
import json
import boto3
import pandas as pd
from datetime import datetime, timedelta
import logging
import sys
import os

# Agregar el directorio raíz al path de Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Ahora importar la configuración
try:
    from config.config import OPENMETEO_BASE_URL, CITIES, WEATHER_VARIABLES, AWS_REGION
except ImportError:
    # Si falla el import, usar configuración directa
    from dotenv import load_dotenv
    load_dotenv()
    
    OPENMETEO_BASE_URL = "https://api.open-meteo.com/v1"
    AWS_REGION = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    CITIES = {
        'bogota': {'lat': 4.6097, 'lon': -74.0817, 'name': 'Bogotá'},
        'medellin': {'lat': 6.2518, 'lon': -75.5636, 'name': 'Medellín'},
        'cali': {'lat': 3.4516, 'lon': -76.5320, 'name': 'Cali'},
        'cartagena': {'lat': 10.3910, 'lon': -75.4794, 'name': 'Cartagena'},
        'barranquilla': {'lat': 10.9639, 'lon': -74.7964, 'name': 'Barranquilla'}
    }
    
    WEATHER_VARIABLES = [
        'temperature_2m_max',
        'temperature_2m_min', 
        'temperature_2m_mean',
        'precipitation_sum',
        'windspeed_10m_max',
        'relative_humidity_2m_mean',
        'surface_pressure_mean'
    ]

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class OpenMeteoAPIIngester:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=AWS_REGION)
        
    def fetch_current_weather(self, city_info):
        """Obtener datos meteorológicos actuales"""
        try:
            params = {
                'latitude': city_info['lat'],
                'longitude': city_info['lon'],
                'current_weather': 'true',
                'daily': ','.join(WEATHER_VARIABLES),
                'timezone': 'America/Bogota',
                'forecast_days': 7
            }
            
            response = requests.get(f"{OPENMETEO_BASE_URL}/forecast", params=params)
            response.raise_for_status()
            
            data = response.json()
            
            # Enriquecer con metadatos
            data['city_info'] = city_info
            data['ingestion_timestamp'] = datetime.now().isoformat()
            data['data_source'] = 'openmeteo_api'
            
            return data
            
        except requests.RequestException as e:
            logger.error(f"Error fetching data for {city_info['name']}: {e}")
            return None
    
    def upload_to_s3(self, data, city_name):
        """Subir datos a S3"""
        try:
            timestamp = datetime.now().strftime('%Y/%m/%d/%H')
            s3_key = f"weather-api/{city_name}/{timestamp}/weather_data.json"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(data, indent=2),
                ContentType='application/json',
                Metadata={
                    'city': city_name,
                    'ingestion_time': datetime.now().isoformat(),
                    'data_type': 'current_weather'
                }
            )
            
            logger.info(f"✅ Data uploaded to s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            return None
    
    def run_ingestion(self):
        """Ejecutar proceso completo de ingesta"""
        results = []
        
        for city_code, city_info in CITIES.items():
            logger.info(f"Fetching data for {city_info['name']}")
            
            weather_data = self.fetch_current_weather(city_info)
            if weather_data:
                s3_key = self.upload_to_s3(weather_data, city_code)
                if s3_key:
                    results.append({
                        'city': city_code,
                        's3_key': s3_key,
                        'status': 'success'
                    })
                else:
                    results.append({
                        'city': city_code,
                        'status': 'upload_failed'
                    })
            else:
                results.append({
                    'city': city_code,
                    'status': 'fetch_failed'
                })
        
        return results

if __name__ == "__main__":
    # Cargar configuración de buckets - ruta relativa desde raíz
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'buckets.json')
    
    try:
        with open(config_path, 'r') as f:
            buckets = json.load(f)
        
        ingester = OpenMeteoAPIIngester(buckets['raw'])
        results = ingester.run_ingestion()
        
        logger.info("Ingestion completed:")
        for result in results:
            logger.info(f"  {result}")
            
    except FileNotFoundError:
        logger.error("❌ Error: config/buckets.json not found")
        logger.error("💡 Ejecuta primero: python infrastructure/setup_s3_buckets.py")
    except Exception as e:
        logger.error(f"❌ Error: {e}")