import mysql.connector
import pandas as pd
import boto3
import json
import os
from datetime import datetime, timedelta
import logging
import sys

# Agregar el directorio raíz al path de Python
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Cargar variables de entorno
from dotenv import load_dotenv
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseIngesterImproved:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER', 'admin'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME', 'weather_data'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
        
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
                self.local_raw_path = os.path.join(self.local_base_path, 'raw', 'database')
                logger.info(f"📁 Usando carpeta local: {self.local_base_path}")
            else:
                # Crear nueva carpeta con fecha actual
                timestamp = datetime.now().strftime('%Y%m%d')
                date_folder = f"weather-analytics-pipeline-{timestamp}"
                self.local_base_path = os.path.join(scripts_folder, date_folder)
                self.local_raw_path = os.path.join(self.local_base_path, 'raw', 'database')
                logger.warning(f"⚠️ No se encontraron carpetas existentes, creando: {self.local_base_path}")
        else:
            # Crear carpeta scripts si no existe
            timestamp = datetime.now().strftime('%Y%m%d')
            date_folder = f"weather-analytics-pipeline-{timestamp}"
            self.local_base_path = os.path.join(scripts_folder, date_folder)
            self.local_raw_path = os.path.join(self.local_base_path, 'raw', 'database')
            logger.warning(f"⚠️ Carpeta scripts no existe, creando: {self.local_base_path}")
        
        # Crear carpetas locales si no existen
        os.makedirs(self.local_raw_path, exist_ok=True)

    def extract_weather_stations(self):
        """Extraer datos de estaciones meteorológicas"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            query = """
            SELECT station_id, station_name, latitude, longitude, 
                   elevation, city, department, country, 
                   installation_date, station_type, status
            FROM weather_stations 
            WHERE status = 'active'
            """
            df = pd.read_sql(query, connection)
            connection.close()
            
            logger.info(f"📊 Extraídas {len(df)} estaciones meteorológicas")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo estaciones: {e}")
            return None

    def extract_climate_events(self, days_back=30):
        """Extraer eventos climáticos recientes"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days_back)
            
            query = """
            SELECT ce.event_id, ce.station_id, ce.event_type, ce.event_date,
                   ce.severity, ce.description, ce.impact_area, ce.economic_impact,
                   ce.people_affected, ws.station_name, ws.city, ws.department
            FROM climate_events ce
            JOIN weather_stations ws ON ce.station_id = ws.station_id
            WHERE ce.event_date BETWEEN %s AND %s
            ORDER BY ce.event_date DESC
            """
            
            df = pd.read_sql(query, connection, params=[start_date, end_date])
            connection.close()
            
            logger.info(f"📊 Extraídos {len(df)} eventos climáticos")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo eventos climáticos: {e}")
            return None

    def extract_weather_thresholds(self):
        """Extraer umbrales de alerta meteorológica"""
        try:
            connection = mysql.connector.connect(**self.db_config)
            query = """
            SELECT wt.threshold_id, wt.station_id, wt.parameter_name,
                   wt.min_value, wt.max_value, wt.alert_level,
                   wt.notification_enabled, ws.station_name, ws.city
            FROM weather_thresholds wt
            JOIN weather_stations ws ON wt.station_id = ws.station_id
            WHERE wt.notification_enabled = TRUE
            """
            
            df = pd.read_sql(query, connection)
            connection.close()
            
            logger.info(f"📊 Extraídos {len(df)} umbrales de alerta")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo umbrales: {e}")
            return None

    def save_to_local(self, data, table_name):
        """Guardar datos en carpeta local"""
        try:
            # Crear subcarpeta por tabla y fecha
            table_date_folder = os.path.join(self.local_raw_path, table_name, datetime.now().strftime('%Y/%m/%d'))
            os.makedirs(table_date_folder, exist_ok=True)
            
            # Nombre de archivo con timestamp
            timestamp_str = datetime.now().strftime('%H%M%S')
            filename = f"{table_name}_data_{timestamp_str}.json"
            local_file_path = os.path.join(table_date_folder, filename)
            
            # Convertir DataFrame a JSON
            if isinstance(data, pd.DataFrame):
                json_data = data.to_dict(orient='records')
            else:
                json_data = data
            
            # Guardar archivo
            with open(local_file_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"💾 {table_name} guardado localmente: {local_file_path}")
            return local_file_path
            
        except Exception as e:
            logger.error(f"Error saving {table_name} locally: {e}")
            return None

    def upload_to_s3(self, data, table_name):
        """Subir datos a S3"""
        try:
            timestamp = datetime.now().strftime('%Y/%m/%d/%H')
            s3_key = f"database/{table_name}/{timestamp}/{table_name}_data.json"
            
            # Convertir DataFrame a JSON
            if isinstance(data, pd.DataFrame):
                json_data = data.to_json(orient='records', date_format='iso', indent=2)
            else:
                json_data = json.dumps(data, indent=2, default=str)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json',
                Metadata={
                    'source': 'mysql_database',
                    'table': table_name,
                    'ingestion_time': datetime.now().isoformat(),
                    'record_count': str(len(data)) if hasattr(data, '__len__') else '1'
                }
            )
            
            logger.info(f"☁️ {table_name} subido a S3: s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error uploading {table_name} to S3: {e}")
            return None

    def update_ingestion_log(self, results):
        """Actualizar log de ingesta"""
        try:
            log_data = {
                'ingestion_timestamp': datetime.now().isoformat(),
                'source': 'mysql_database',
                'database_config': {
                    'host': self.db_config['host'],
                    'database': self.db_config['database'],
                    'user': self.db_config['user']
                },
                'total_tables': len(results),
                'successful_extractions': len([r for r in results if r['status'] == 'success']),
                'failed_extractions': len([r for r in results if r['status'] != 'success']),
                'results': results
            }
            
            # Guardar log local
            log_folder = os.path.join(self.local_base_path, 'raw', 'logs')
            os.makedirs(log_folder, exist_ok=True)
            
            log_filename = f"database_ingestion_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            log_path = os.path.join(log_folder, log_filename)
            
            with open(log_path, 'w', encoding='utf-8') as f:
                json.dump(log_data, f, indent=2, ensure_ascii=False, default=str)
            
            logger.info(f"📋 Log de ingesta guardado: {log_path}")
            
            # También subir log a S3 si es posible
            try:
                s3_log_key = f"logs/database_ingestion/{datetime.now().strftime('%Y/%m/%d')}/{log_filename}"
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
        """Ejecutar proceso completo de ingesta desde BD"""
        logger.info(f"🚀 Iniciando ingesta desde base de datos...")
        logger.info(f"📁 Guardando en local: {self.local_raw_path}")
        logger.info(f"☁️ Guardando en S3: {self.bucket_name}")
        
        results = []

        # Verificar configuración de BD
        if not all([self.db_config['host'], self.db_config['password']]):
            logger.error("❌ Configuración de BD incompleta en variables de entorno")
            return results

        # Lista de extracciones a realizar
        extractions = [
            ('weather_stations', self.extract_weather_stations),
            ('climate_events', self.extract_climate_events),
            ('weather_thresholds', self.extract_weather_thresholds)
        ]

        for table_name, extract_func in extractions:
            logger.info(f"🗄️ Procesando tabla: {table_name}")
            
            # Extraer datos
            data = extract_func()
            
            if data is not None and not data.empty:
                # Guardar localmente
                local_path = self.save_to_local(data, table_name)
                
                # Subir a S3
                s3_key = self.upload_to_s3(data, table_name)
                
                # Determinar estado
                if local_path and s3_key:
                    status = 'success'
                    logger.info(f"✅ {table_name}: Guardado local y S3")
                elif local_path:
                    status = 'partial_success_local'
                    logger.info(f"⚠️ {table_name}: Solo guardado local")
                elif s3_key:
                    status = 'partial_success_s3'
                    logger.info(f"⚠️ {table_name}: Solo guardado S3")
                else:
                    status = 'save_failed'
                    logger.error(f"❌ {table_name}: Error guardando datos")
                
                results.append({
                    'table': table_name,
                    'records': len(data),
                    'status': status,
                    'local_path': local_path,
                    's3_key': s3_key,
                    'timestamp': datetime.now().isoformat()
                })
            else:
                logger.warning(f"⚠️ {table_name}: Sin datos o error en extracción")
                results.append({
                    'table': table_name,
                    'records': 0,
                    'status': 'no_data_or_error',
                    'local_path': None,
                    's3_key': None,
                    'timestamp': datetime.now().isoformat()
                })

        # Actualizar log de ingesta
        self.update_ingestion_log(results)
        
        # Mostrar resumen
        successful = len([r for r in results if r['status'] == 'success'])
        total = len(results)
        total_records = sum([r['records'] for r in results])
        
        logger.info(f"\n📊 RESUMEN DE INGESTA DE BD:")
        logger.info(f"   ✅ Tablas exitosas: {successful}/{total}")
        logger.info(f"   📊 Total registros: {total_records}")
        logger.info(f"   📁 Carpeta local: {self.local_raw_path}")
        logger.info(f"   ☁️ Bucket S3: {self.bucket_name}")
        
        if successful == total:
            logger.info("🎉 ¡Ingesta de BD completada exitosamente!")
        elif successful > 0:
            logger.warning("⚠️ Ingesta de BD completada con algunos problemas")
        else:
            logger.error("❌ Ingesta de BD falló completamente")

        return results

if __name__ == "__main__":
    # Cargar configuración de buckets - ruta relativa desde raíz
    config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'config', 'buckets.json')
    
    try:
        with open(config_path, 'r') as f:
            buckets = json.load(f)
        
        ingester = DatabaseIngesterImproved(buckets['raw'])
        results = ingester.run_ingestion()
        
        logger.info("\n📋 Resultados detallados:")
        for result in results:
            status_icon = "✅" if result['status'] == 'success' else "❌" if result['status'] in ['save_failed', 'no_data_or_error'] else "⚠️"
            logger.info(f"  {status_icon} {result['table']}: {result['records']} registros - {result['status']}")
            if result['local_path']:
                logger.info(f"     📁 Local: {result['local_path']}")
            if result['s3_key']:
                logger.info(f"     ☁️ S3: {result['s3_key']}")
                
    except FileNotFoundError:
        logger.error("❌ Error: config/buckets.json not found")
        logger.error("💡 Ejecuta primero: python infrastructure/setup_s3_buckets.py")
    except Exception as e:
        logger.error(f"❌ Error: {e}")