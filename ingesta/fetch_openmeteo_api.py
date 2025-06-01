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

class OpenMeteoAPIIngesterImproved:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=AWS_REGION)
        
        # Configurar rutas locales
        self.setup_local_paths()
        
    def setup_local_paths(self):
        """Configurar rutas de carpetas locales"""
        # Buscar la carpeta de fecha más reciente
        scripts_folder = "scripts"
        if os.path.exists(scripts_folder):
            # Buscar carpetas que sigan el patrón weather-analytics-pipeline-YYYYMMDD
            folders = [f for f in os.listdir(scripts_folder) 
                      if f.startswith('weather-analytics-pipeline-') and os.path.isdir(os.path.join(scripts_folder, f))]
            
            if folders:
                # Usar la carpeta más reciente
                latest_folder = sorted(folders)[-1]
                self.local_base_path = os.path.join(scripts_folder, latest_folder)
                self.local_raw_path = os.path.join(self.local_base_path, 'raw', 'weather-api')
                logger.info(f"📁 Usando carpeta local: {self.local_base_path}")
            else:
                # Crear nueva carpeta con fecha actual
                timestamp = datetime.now().strftime('%Y%m%d')
                date_folder = f"weather-analytics-pipeline-{timestamp}"
                self.local_base_path = os.path.join(scripts_folder, date_folder)
                self.local_raw_path = os.path.join(self.local_base_path, 'raw', 'weather-api')
                logger.warning(f"⚠️ No se encontraron carpetas existentes, creando: {self.local_base_path}")
        else:
            # Crear carpeta scripts si no existe
            timestamp = datetime.now().strftime('%Y%m%d')
            date_folder = f"weather-analytics-pipeline-{timestamp}"
            self.local_base_path = os.path.join(scripts_folder, date_folder)
            self.local_raw_path = os.path.join(self.local_base_path, 'raw', 'weather-api')
            logger.warning(f"⚠️ Carpeta scripts no existe, creando: {self.local_base_path}")
        
        # Crear carpetas locales si no existen
        os.makedirs(self.local_raw_path, exist_ok=True)
        
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
    
    def save_to_local(self, data, city_name):
        """Guardar datos en carpeta local"""
        try:
            # Crear subcarpeta por ciudad y fecha
            city_date_folder = os.path.join(self.local_raw_path, city_name, datetime.now().strftime('%Y/%m/%d'))
            os.makedirs(city_date_folder, exist_ok=True)
            
            # Nombre de archivo con timestamp
            timestamp_str = datetime.now().strftime('%H%M%S')
            filename = f"weather_data_{timestamp_str}.json"
            local_file_path = os.path.join(city_date_folder, filename)
            
            # Guardar archivo
            with open(local_file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"💾 Datos guardados localmente: {local_file_path}")
            return local_file_path
            
        except Exception as e:
            logger.error(f"Error saving locally: {e}")
            return None
    
    def upload_to_s3(self, data, city_name):
        """Subir datos a S3"""
        try:
            timestamp = datetime.now().strftime('%Y/%m/%d/%H')
            s3_key = f"weather-api/{city_name}/{timestamp}/weather_data.json"
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json.dumps(data, indent=2, default=str),
                ContentType='application/json',
                Metadata={
                    'city': city_name,
                    'ingestion_time': datetime.now().isoformat(),
                    'data_type': 'current_weather',
                    'source': 'openmeteo_api'
                }
            )
            
            logger.info(f"☁️ Datos subidos a S3: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading to S3: {e}")
            return None
    
    def update_ingestion_log(self, results):
        """Actualizar log de ingesta"""
        try:
            log_data = {
                'ingestion_timestamp': datetime.now().isoformat(),
                'source': 'openmeteo_api',
                'total_cities': len(CITIES),
                'successful_ingestions': len([r for r in results if r['status'] == 'success']),
                'failed_ingestions': len([r for r in results if r['status'] != 'success']),
                'results': results
            }
            
            # Guardar log local
            log_folder = os.path.join(self.local_base_path, 'raw', 'logs')
            os.makedirs(log_folder, exist_ok=True)
            
            log_filename = f"ingestion_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            log_path = os.path.join(log_folder, log_filename)
            
            with open(log_path, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📋 Log de ingesta guardado: {log_path}")
            
            # También subir log a S3 si es posible
            try:
                s3_log_key = f"logs/ingestion/{datetime.now().strftime('%Y/%m/%d')}/{log_filename}"
                self.s3_client.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_log_key,
                    Body=json.dumps(log_data, indent=2, default=str),
                    ContentType='application/json'
                )
                logger.info(f"📋 Log también subido a S3: {s3_log_key}")
            except:
                logger.warning("⚠️ No se pudo subir log a S3")
                
        except Exception as e:
            logger.error(f"Error updating ingestion log: {e}")
    
    def run_ingestion(self):
        """Ejecutar proceso completo de ingesta"""
        logger.info(f"🚀 Iniciando ingesta de API OpenMeteo...")
        logger.info(f"📁 Guardando en local: {self.local_raw_path}")
        logger.info(f"☁️ Guardando en S3: {self.bucket_name}")
        
        results = []
        
        for city_code, city_info in CITIES.items():
            logger.info(f"🌤️ Procesando {city_info['name']}...")
            
            # Obtener datos
            weather_data = self.fetch_current_weather(city_info)
            if not weather_data:
                results.append({
                    'city': city_code,
                    'city_name': city_info['name'],
                    'status': 'fetch_failed',
                    'local_path': None,
                    's3_key': None
                })
                continue
            
            # Guardar localmente
            local_path = self.save_to_local(weather_data, city_code)
            
            # Subir a S3
            s3_key = self.upload_to_s3(weather_data, city_code)
            
            # Registrar resultado
            if local_path or s3_key:
                status = 'success'
                if local_path and s3_key:
                    logger.info(f"✅ {city_info['name']}: Guardado local y S3")
                elif local_path:
                    logger.info(f"⚠️ {city_info['name']}: Solo guardado local")
                    status = 'partial_success_local'
                elif s3_key:
                    logger.info(f"⚠️ {city_info['name']}: Solo guardado S3")
                    status = 'partial_success_s3'
            else:
                status = 'save_failed'
                logger.error(f"❌ {city_info['name']}: Error guardando datos")
            
            results.append({
                'city': city_code,
                'city_name': city_info['name'],
                'status': status,
                'local_path': local_path,
                's3_key': s3_key,
                'timestamp': datetime.now().isoformat()
            })
        
        # Actualizar log de ingesta
        self.update_ingestion_log(results)
        
        # Mostrar resumen
        successful = len([r for r in results if r['status'] == 'success'])
        total = len(results)
        
        logger.info(f"\n📊 RESUMEN DE INGESTA:")
        logger.info(f"   ✅ Exitosos: {successful}/{total}")
        logger.info(f"   📁 Carpeta local: {self.local_raw_path}")
        logger.info(f"   ☁️ Bucket S3: {self.bucket_name}")
        
        if successful == total:
            logger.info("🎉 ¡Ingesta completada exitosamente!")
        elif successful > 0:
            logger.warning("⚠️ Ingesta completada con algunos problemas")
        else:
            logger.error("❌ Ingesta falló completamente")
        
        return results

if __name__ == "__main__":
    # Cargar configuración de buckets - ruta relativa desde raíz
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'buckets.json')
    
    try:
        with open(config_path, 'r') as f:
            buckets = json.load(f)
        
        ingester = OpenMeteoAPIIngesterImproved(buckets['raw'])
        results = ingester.run_ingestion()
        
        logger.info("\n📋 Resultados detallados:")
        for result in results:
            status_icon = "✅" if result['status'] == 'success' else "❌"
            logger.info(f"  {status_icon} {result['city_name']}: {result['status']}")
            if result['local_path']:
                logger.info(f"     📁 Local: {result['local_path']}")
            if result['s3_key']:
                logger.info(f"     ☁️ S3: {result['s3_key']}")
                
    except FileNotFoundError:
        logger.error("❌ Error: config/buckets.json not found")
        logger.error("💡 Ejecuta primero: python infrastructure/setup_s3_buckets.py")
    except Exception as e:
        logger.error(f"❌ Error: {e}")