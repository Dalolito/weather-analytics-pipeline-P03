import boto3
import json
import time
import logging
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AthenaManager:
    def __init__(self):
        # Usar región desde variables de entorno
        aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        
        self.athena_client = boto3.client('athena', region_name=aws_region)
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
        # Cargar configuración
        with open('config/buckets.json', 'r') as f:
            self.buckets = json.load(f)
        
        self.database_name = 'weather_analytics_db'
        self.query_results_bucket = self.buckets['refined']

    def create_database(self):
        """Crear base de datos en Athena"""
        query = f"CREATE DATABASE IF NOT EXISTS {self.database_name}"
        
        response = self.athena_client.start_query_execution(
            QueryString=query,
            ResultConfiguration={
                'OutputLocation': f"s3://{self.query_results_bucket}/athena-results/"
            }
        )
        
        query_id = response['QueryExecutionId']
        self.wait_for_query_completion(query_id)
        logger.info(f"✅ Base de datos {self.database_name} creada")

    def create_tables(self):
        """Crear tablas externas en Athena"""
        
        # Tabla de datos meteorológicos integrados
        weather_table_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.integrated_weather_data (
            city_name string,
            latitude double,
            longitude double,
            date date,
            temp_max double,
            temp_min double,
            temp_avg double,
            precipitation double,
            wind_speed double,
            humidity double,
            ingestion_timestamp timestamp,
            data_source string,
            station_id string,
            station_name string,
            elevation double,
            station_type string
        )
        PARTITIONED BY (
            year int,
            month int
        )
        STORED AS PARQUET
        LOCATION 's3://{self.buckets['trusted']}/integrated_weather_data/'
        TBLPROPERTIES ('has_encrypted_data'='false')
        """
        
        # Tabla de tendencias mensuales
        trends_table_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.temperature_trends_monthly (
            city_name string,
            month int,
            avg_temperature double,
            max_temperature double,
            min_temperature double,
            avg_precipitation double,
            avg_humidity double,
            data_points bigint
        )
        PARTITIONED BY (
            year int
        )
        STORED AS PARQUET
        LOCATION 's3://{self.buckets['refined']}/temperature_trends_monthly/'
        TBLPROPERTIES ('has_encrypted_data'='false')
        """
        
        # Tabla de eventos extremos
        extreme_events_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.extreme_weather_events (
            city_name string,
            event_type string,
            event_count bigint,
            avg_temp_during_event double,
            max_precipitation double,
            max_wind_speed double
        )
        PARTITIONED BY (
            year int
        )
        STORED AS PARQUET
        LOCATION 's3://{self.buckets['refined']}/extreme_weather_events/'
        TBLPROPERTIES ('has_encrypted_data'='false')
        """
        
        # Tabla de resumen anual
        annual_summary_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.annual_weather_summary (
            city_name string,
            annual_avg_temp double,
            annual_max_temp double,
            annual_min_temp double,
            annual_precipitation double,
            annual_avg_humidity double,
            annual_avg_wind double,
            total_measurements bigint,
            climate_category string
        )
        PARTITIONED BY (
            year int
        )
        STORED AS PARQUET
        LOCATION 's3://{self.buckets['refined']}/annual_weather_summary/'
        TBLPROPERTIES ('has_encrypted_data'='false')
        """
        
        # Tabla de métricas ML
        ml_metrics_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.ml_model_metrics (
            model_name string,
            metric_name string,
            metric_value double,
            evaluation_date string,
            city_name string,
            month int
        )
        STORED AS PARQUET
        LOCATION 's3://{self.buckets['refined']}/ml_model_metrics/'
        TBLPROPERTIES ('has_encrypted_data'='false')
        """
        
        # Tabla de predicciones de temperatura
        temp_predictions_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.temperature_predictions (
            city_name string,
            date date,
            temp_avg double,
            prediction double,
            error double,
            month int
        )
        STORED AS PARQUET
        LOCATION 's3://{self.buckets['refined']}/temperature_predictions/'
        TBLPROPERTIES ('has_encrypted_data'='false')
        """
        
        # Tabla de pronósticos
        forecasts_query = f"""
        CREATE EXTERNAL TABLE IF NOT EXISTS {self.database_name}.weather_forecasts (
            city_name string,
            forecast_date string,
            historical_avg_temp double,
            historical_avg_precip double,
            historical_avg_humidity double,
            years_of_data bigint,
            forecast_type string,
            day_of_year int
        )
        STORED AS PARQUET
        LOCATION 's3://{self.buckets['refined']}/weather_forecasts/'
        TBLPROPERTIES ('has_encrypted_data'='false')
        """
        
        queries = [
            ("integrated_weather_data", weather_table_query),
            ("temperature_trends_monthly", trends_table_query),
            ("extreme_weather_events", extreme_events_query),
            ("annual_weather_summary", annual_summary_query),
            ("ml_model_metrics", ml_metrics_query),
            ("temperature_predictions", temp_predictions_query),
            ("weather_forecasts", forecasts_query)
        ]
        
        for table_name, query in queries:
            try:
                response = self.athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={'Database': self.database_name},
                    ResultConfiguration={
                        'OutputLocation': f"s3://{self.query_results_bucket}/athena-results/"
                    }
                )
                
                query_id = response['QueryExecutionId']
                self.wait_for_query_completion(query_id)
                logger.info(f"✅ Tabla {table_name} creada")
                
            except Exception as e:
                logger.warning(f"⚠️ Error creando tabla {table_name}: {e}")

    def repair_partitions(self):
        """Reparar particiones para tablas particionadas"""
        partitioned_tables = [
            'integrated_weather_data',
            'temperature_trends_monthly',
            'extreme_weather_events',
            'annual_weather_summary'
        ]
        
        for table in partitioned_tables:
            try:
                query = f"MSCK REPAIR TABLE {self.database_name}.{table}"
                
                response = self.athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={'Database': self.database_name},
                    ResultConfiguration={
                        'OutputLocation': f"s3://{self.query_results_bucket}/athena-results/"
                    }
                )
                
                query_id = response['QueryExecutionId']
                self.wait_for_query_completion(query_id)
                logger.info(f"✅ Particiones reparadas para {table}")
                
            except Exception as e:
                logger.warning(f"⚠️ Error reparando particiones de {table}: {e}")

    def wait_for_query_completion(self, query_id):
        """Esperar a que complete una consulta"""
        max_wait_time = 60  # 60 segundos máximo
        wait_time = 0
        
        while wait_time < max_wait_time:
            try:
                response = self.athena_client.get_query_execution(
                    QueryExecutionId=query_id
                )
                
                status = response['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    if status == 'FAILED':
                        error = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                        logger.warning(f"Query falló: {error}")
                        return status
                    return status
                
                time.sleep(2)
                wait_time += 2
                
            except Exception as e:
                logger.warning(f"Error verificando query {query_id}: {e}")
                return 'ERROR'
        
        logger.warning(f"Query {query_id} timeout after {max_wait_time} seconds")
        return 'TIMEOUT'

    def test_queries(self):
        """Ejecutar consultas de prueba básicas"""
        test_queries = [
            ("Contar registros integrados", 
             f"SELECT COUNT(*) as total FROM {self.database_name}.integrated_weather_data LIMIT 1"),
            
            ("Verificar tendencias mensuales", 
             f"SELECT COUNT(*) as total FROM {self.database_name}.temperature_trends_monthly LIMIT 1"),
            
            ("Verificar métricas ML", 
             f"SELECT COUNT(*) as total FROM {self.database_name}.ml_model_metrics LIMIT 1")
        ]
        
        for query_name, query in test_queries:
            try:
                logger.info(f"🔍 Ejecutando: {query_name}")
                
                response = self.athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={'Database': self.database_name},
                    ResultConfiguration={
                        'OutputLocation': f"s3://{self.query_results_bucket}/athena-results/"
                    }
                )
                
                query_id = response['QueryExecutionId']
                status = self.wait_for_query_completion(query_id)
                
                if status == 'SUCCEEDED':
                    logger.info(f"✅ {query_name} completada")
                else:
                    logger.warning(f"⚠️ {query_name} falló con status: {status}")
                    
            except Exception as e:
                logger.warning(f"⚠️ Error en {query_name}: {e}")

    def setup_complete_athena(self):
        """Configuración completa de Athena"""
        logger.info("🏗️ Configurando Athena...")
        
        try:
            # Paso 1: Crear base de datos
            logger.info("📊 Paso 1: Creando base de datos...")
            self.create_database()
            
            # Paso 2: Crear tablas
            logger.info("📋 Paso 2: Creando tablas...")
            self.create_tables()
            
            # Paso 3: Reparar particiones
            logger.info("🔧 Paso 3: Reparando particiones...")
            self.repair_partitions()
            
            # Paso 4: Probar consultas
            logger.info("🔍 Paso 4: Probando consultas...")
            self.test_queries()
            
            logger.info("🎉 Configuración de Athena completada!")
            
            # Mostrar información útil
            print("\n" + "="*60)
            print("📊 ATHENA CONFIGURADO EXITOSAMENTE")
            print("="*60)
            print(f"🗄️ Base de datos: {self.database_name}")
            print(f"📁 Ubicación resultados: s3://{self.query_results_bucket}/athena-results/")
            
            print(f"\n📋 Tablas disponibles:")
            tables = [
                "integrated_weather_data",
                "temperature_trends_monthly", 
                "extreme_weather_events",
                "annual_weather_summary",
                "ml_model_metrics",
                "temperature_predictions",
                "weather_forecasts"
            ]
            
            for table in tables:
                print(f"   • {table}")
            
            print(f"\n💡 Ejemplo de consulta:")
            print(f"   SELECT city_name, AVG(temp_avg) as avg_temp")
            print(f"   FROM {self.database_name}.integrated_weather_data")
            print(f"   GROUP BY city_name;")
            
        except Exception as e:
            logger.error(f"❌ Error en configuración de Athena: {e}")
            raise e

if __name__ == "__main__":
    try:
        athena_manager = AthenaManager()
        athena_manager.setup_complete_athena()
    except Exception as e:
        print(f"❌ Error: {e}")
        print("💡 Asegúrate de que:")
        print("   1. AWS_DEFAULT_REGION esté configurado en .env")
        print("   2. Tengas permisos para Athena")
        print("   3. Los buckets S3 existan")