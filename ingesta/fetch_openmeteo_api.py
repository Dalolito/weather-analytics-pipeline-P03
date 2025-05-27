import requests
import json
import boto3
import pandas as pd
from datetime import datetime, timedelta
from config.config import OPENMETEO_BASE_URL, CITIES, WEATHER_VARIABLES, AWS_REGION
import logging

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
    # Cargar configuración de buckets
    with open('config/buckets.json', 'r') as f:
        buckets = json.load(f)
    
    ingester = OpenMeteoAPIIngester(buckets['raw'])
    results = ingester.run_ingestion()
    
    logger.info("Ingestion completed:")
    for result in results:
        logger.info(f"  {result}")  