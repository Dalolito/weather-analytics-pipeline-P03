import boto3
import json
import time
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AthenaManager:
    def __init__(self):
        self.athena_client = boto3.client('athena')
        self.s3_client = boto3.client('s3')
        
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
        """
        
        queries = [
            ("integrated_weather_data", weather_table_query),
            ("temperature_trends_monthly", trends_table_query),
            ("extreme_weather_events", extreme_events_query),
            ("annual_weather_summary", annual_summary_query)
        ]
        
        for table_name, query in queries:
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

    def repair_partitions(self):
        """Reparar particiones para todas las tablas"""
        tables = [
            'integrated_weather_data',
            'temperature_trends_monthly',
            'extreme_weather_events',
            'annual_weather_summary'
        ]
        
        for table in tables:
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

    def wait_for_query_completion(self, query_id):
        """Esperar a que complete una consulta"""
        while True:
            response = self.athena_client.get_query_execution(
                QueryExecutionId=query_id
            )
            
            status = response['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                if status == 'FAILED':
                    error = response['QueryExecution']['Status']['StateChangeReason']
                    raise Exception(f"Query falló: {error}")
                return status
            
            time.sleep(2)

    def test_queries(self):
        """Ejecutar consultas de prueba"""
        test_queries = [
            ("Conteo de registros por ciudad", 
             f"SELECT city_name, COUNT(*) as total FROM {self.database_name}.integrated_weather_data GROUP BY city_name LIMIT 10"),
            
            ("Temperatura promedio por ciudad", 
             f"SELECT city_name, AVG(temp_avg) as avg_temp FROM {self.database_name}.integrated_weather_data GROUP BY city_name ORDER BY avg_temp DESC LIMIT 10"),
            
            ("Eventos extremos por año", 
             f"SELECT year, event_type, SUM(event_count) as total_events FROM {self.database_name}.extreme_weather_events GROUP BY year, event_type ORDER BY year DESC")
        ]
        
        for query_name, query in test_queries:
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
                logger.error(f"❌ {query_name} falló")

    def setup_complete_athena(self):
        """Configuración completa de Athena"""
        logger.info("🏗️ Configurando Athena...")
        
        self.create_database()
        self.create_tables()
        self.repair_partitions()
        self.test_queries()
        
        logger.info("🎉 Configuración de Athena completada!")

if __name__ == "__main__":
    athena_manager = AthenaManager()
    athena_manager.setup_complete_athena()