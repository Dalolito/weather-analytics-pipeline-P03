import mysql.connector
import pandas as pd
import boto3
import json
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv
import logging

# Cargar variables de entorno
load_dotenv()

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseIngester:
    def __init__(self, bucket_name):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        self.db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER', 'admin'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME', 'weather_data')
        }

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
            
            logger.info(f"Extraídas {len(df)} estaciones meteorológicas")
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
            
            logger.info(f"Extraídos {len(df)} eventos climáticos")
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
            
            logger.info(f"Extraídos {len(df)} umbrales de alerta")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo umbrales: {e}")
            return None

    def upload_to_s3(self, data, table_name):
        """Subir datos a S3"""
        try:
            timestamp = datetime.now().strftime('%Y/%m/%d/%H')
            s3_key = f"database/{table_name}/{timestamp}/{table_name}_data.json"
            
            # Convertir DataFrame a JSON
            json_data = data.to_json(orient='records', date_format='iso', indent=2)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=s3_key,
                Body=json_data,
                ContentType='application/json',
                Metadata={
                    'source': 'mysql_database',
                    'table': table_name,
                    'ingestion_time': datetime.now().isoformat(),
                    'record_count': str(len(data))
                }
            )
            
            logger.info(f"Datos subidos a s3://{self.bucket_name}/{s3_key}")
            return s3_key
            
        except Exception as e:
            logger.error(f"Error subiendo {table_name} a S3: {e}")
            return None

    def run_ingestion(self):
        """Ejecutar proceso completo de ingesta desde BD"""
        logger.info("🚀 Iniciando ingesta desde base de datos...")
        results = []

        # Extraer y subir estaciones meteorológicas
        stations_df = self.extract_weather_stations()
        if stations_df is not None and not stations_df.empty:
            s3_key = self.upload_to_s3(stations_df, 'weather_stations')
            if s3_key:
                results.append({
                    'table': 'weather_stations',
                    'records': len(stations_df),
                    's3_key': s3_key,
                    'status': 'success'
                })

        # Extraer y subir eventos climáticos
        events_df = self.extract_climate_events()
        if events_df is not None and not events_df.empty:
            s3_key = self.upload_to_s3(events_df, 'climate_events')
            if s3_key:
                results.append({
                    'table': 'climate_events',
                    'records': len(events_df),
                    's3_key': s3_key,
                    'status': 'success'
                })

        # Extraer y subir umbrales de alerta
        thresholds_df = self.extract_weather_thresholds()
        if thresholds_df is not None and not thresholds_df.empty:
            s3_key = self.upload_to_s3(thresholds_df, 'weather_thresholds')
            if s3_key:
                results.append({
                    'table': 'weather_thresholds',
                    'records': len(thresholds_df),
                    's3_key': s3_key,
                    'status': 'success'
                })

        return results

if __name__ == "__main__":
    # Cargar configuración de buckets
    with open('config/buckets.json', 'r') as f:
        buckets = json.load(f)
    
    ingester = DatabaseIngester(buckets['raw'])
    results = ingester.run_ingestion()
    
    logger.info("Ingesta completada:")
    for result in results:
        logger.info(f"  {result['table']}: {result['records']} registros -> {result['s3_key']}")