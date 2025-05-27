from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Crear sesión de Spark"""
    return SparkSession.builder \
        .appName("WeatherAnalytics-ETL") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()

def process_weather_api_data(spark, raw_bucket, trusted_bucket):
    """Procesar datos de API meteorológica"""
    print("🌤️ Procesando datos de API meteorológica...")
    
    # Leer datos crudos de la API
    weather_df = spark.read.json(f"s3a://{raw_bucket}/weather-api/*/weather_data.json")
    
    # Limpiar y transformar datos de clima
    weather_clean = weather_df.select(
        col("city_info.name").alias("city_name"),
        col("city_info.lat").alias("latitude"),
        col("city_info.lon").alias("longitude"),
        col("daily.time").alias("date"),
        col("daily.temperature_2m_max").alias("temp_max"),
        col("daily.temperature_2m_min").alias("temp_min"),
        col("daily.precipitation_sum").alias("precipitation"),
        col("daily.windspeed_10m_max").alias("wind_speed"),
        col("daily.relative_humidity_2m_mean").alias("humidity"),
        col("ingestion_timestamp"),
        col("data_source")
    ).filter(col("city_name").isNotNull())
    
    # Calcular temperatura promedio
    weather_clean = weather_clean.withColumn(
        "temp_avg", (col("temp_max") + col("temp_min")) / 2
    )
    
    # Agregar particiones por año y mes
    weather_clean = weather_clean.withColumn("year", year(col("date"))) \
                                .withColumn("month", month(col("date")))
    
    # Escribir a zona Trusted
    weather_clean.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"s3a://{trusted_bucket}/weather_data/")
    
    print(f"✅ Procesados {weather_clean.count()} registros de datos meteorológicos")
    return weather_clean

def process_database_data(spark, raw_bucket, trusted_bucket):
    """Procesar datos de base de datos"""
    print("🏛️ Procesando datos de base de datos...")
    
    # Procesar estaciones meteorológicas
    stations_df = spark.read.json(f"s3a://{raw_bucket}/database/weather_stations/*/weather_stations_data.json")
    
    stations_clean = stations_df.select(
        col("station_id"),
        col("station_name"),
        col("latitude"),
        col("longitude"),
        col("elevation"),
        col("city"),
        col("department"),
        col("country"),
        col("installation_date"),
        col("station_type"),
        col("status")
    ).filter(col("station_id").isNotNull())
    
    # Escribir estaciones limpias
    stations_clean.write.mode("overwrite") \
        .parquet(f"s3a://{trusted_bucket}/weather_stations/")
    
    # Procesar eventos climáticos
    try:
        events_df = spark.read.json(f"s3a://{raw_bucket}/database/climate_events/*/climate_events_data.json")
        
        events_clean = events_df.select(
            col("event_id"),
            col("station_id"),
            col("event_type"),
            col("event_date"),
            col("severity"),
            col("description"),
            col("impact_area"),
            col("economic_impact"),
            col("people_affected"),
            col("station_name"),
            col("city"),
            col("department")
        ).filter(col("event_id").isNotNull())
        
        # Agregar particiones por año
        events_clean = events_clean.withColumn("year", year(col("event_date")))
        
        events_clean.write.mode("overwrite") \
            .partitionBy("year") \
            .parquet(f"s3a://{trusted_bucket}/climate_events/")
        
        print(f"✅ Procesados {events_clean.count()} registros de eventos climáticos")
    except Exception as e:
        print(f"⚠️ No se encontraron eventos climáticos: {e}")
    
    print(f"✅ Procesadas {stations_clean.count()} estaciones meteorológicas")

def create_integrated_dataset(spark, trusted_bucket):
    """Crear dataset integrado combinando API y BD"""
    print("🔗 Creando dataset integrado...")
    
    # Leer datos procesados
    weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/weather_data/")
    stations_df = spark.read.parquet(f"s3a://{trusted_bucket}/weather_stations/")
    
    # Hacer join por ciudad (aproximado)
    integrated_df = weather_df.join(
        stations_df,
        (weather_df.city_name == stations_df.city) |
        (abs(weather_df.latitude - stations_df.latitude) < 0.1) &
        (abs(weather_df.longitude - stations_df.longitude) < 0.1),
        "left"
    ).select(
        weather_df["*"],
        stations_df.station_id,
        stations_df.station_name,
        stations_df.elevation,
        stations_df.station_type
    )
    
    # Escribir dataset integrado
    integrated_df.write.mode("overwrite") \
        .partitionBy("year", "month") \
        .parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
    
    print(f"✅ Dataset integrado creado con {integrated_df.count()} registros")

def main():
    if len(sys.argv) != 3:
        print("Uso: spark-submit etl_weather_data.py <raw_bucket> <trusted_bucket>")
        sys.exit(1)
    
    raw_bucket = sys.argv[1]
    trusted_bucket = sys.argv[2]
    
    # Crear sesión Spark
    spark = create_spark_session()
    
    try:
        # Procesar datos de API
        process_weather_api_data(spark, raw_bucket, trusted_bucket)
        
        # Procesar datos de BD
        process_database_data(spark, raw_bucket, trusted_bucket)
        
        # Crear dataset integrado
        create_integrated_dataset(spark, trusted_bucket)
        
        print("🎉 ETL completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error en ETL: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()