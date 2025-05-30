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
    
    try:
        # Leer datos crudos de la API con el path correcto
        weather_df = spark.read.option("multiline", "true").json(f"s3a://{raw_bucket}/weather-api/*/*/*/*/weather_data.json")
        
        print(f"📊 Archivos JSON leídos: {weather_df.count()} registros")
        
        # Mostrar esquema para debug
        print("📋 Esquema de datos:")
        weather_df.printSchema()
        
        # Explotar arrays de datos diarios usando arrays_zip
        weather_exploded = weather_df.select(
            col("city_info.name").alias("city_name"),
            col("city_info.lat").alias("latitude"),
            col("city_info.lon").alias("longitude"),
            col("ingestion_timestamp"),
            col("data_source"),
            explode(arrays_zip(
                col("daily.time"),
                col("daily.temperature_2m_max"),
                col("daily.temperature_2m_min"),
                col("daily.precipitation_sum"),
                col("daily.windspeed_10m_max"),
                col("daily.relative_humidity_2m_mean")
            )).alias("daily_data")
        ).select(
            col("city_name"),
            col("latitude"),
            col("longitude"),
            col("daily_data.time").alias("date"),
            col("daily_data.temperature_2m_max").alias("temp_max"),
            col("daily_data.temperature_2m_min").alias("temp_min"),
            col("daily_data.precipitation_sum").alias("precipitation"),
            col("daily_data.windspeed_10m_max").alias("wind_speed"),
            col("daily_data.relative_humidity_2m_mean").alias("humidity"),
            col("ingestion_timestamp"),
            col("data_source")
        ).filter(col("city_name").isNotNull() & col("date").isNotNull())
        
        # Calcular temperatura promedio
        weather_clean = weather_exploded.withColumn(
            "temp_avg", 
            when(col("temp_max").isNotNull() & col("temp_min").isNotNull(),
                (col("temp_max") + col("temp_min")) / 2)
            .otherwise(col("temp_max"))
        )
        
        # Convertir date string a date type y agregar particiones
        weather_clean = weather_clean.withColumn("date", to_date(col("date"))) \
                                    .withColumn("year", year(col("date"))) \
                                    .withColumn("month", month(col("date")))
        
        # Filtrar registros válidos
        weather_clean = weather_clean.filter(
            col("date").isNotNull() & 
            col("temp_max").isNotNull()
        )
        
        print(f"📊 Registros procesados: {weather_clean.count()}")
        
        # Escribir a zona Trusted
        weather_clean.write.mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(f"s3a://{trusted_bucket}/weather_data/")
        
        print(f"✅ Datos meteorológicos procesados y guardados")
        return weather_clean
        
    except Exception as e:
        print(f"❌ Error procesando datos de API: {e}")
        import traceback
        traceback.print_exc()
        
        # Crear DataFrame vacío para continuar
        empty_schema = StructType([
            StructField("city_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("date", DateType(), True),
            StructField("temp_max", DoubleType(), True),
            StructField("temp_min", DoubleType(), True),
            StructField("temp_avg", DoubleType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("wind_speed", DoubleType(), True),
            StructField("humidity", DoubleType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True)
        ])
        empty_df = spark.createDataFrame([], empty_schema)
        empty_df.write.mode("overwrite").parquet(f"s3a://{trusted_bucket}/weather_data/")
        return empty_df

def process_database_data(spark, raw_bucket, trusted_bucket):
    """Procesar datos de base de datos"""
    print("🏛️ Procesando datos de base de datos...")
    
    try:
        # Procesar estaciones meteorológicas con path correcto
        stations_df = spark.read.option("multiline", "true").json(f"s3a://{raw_bucket}/database/weather_stations/*/*/*/*/weather_stations_data.json")
        
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
        
        print(f"✅ Procesadas {stations_clean.count()} estaciones meteorológicas")
        
    except Exception as e:
        print(f"⚠️ Error procesando estaciones: {e}")
        # Crear DataFrame vacío de estaciones
        empty_stations_schema = StructType([
            StructField("station_id", StringType(), True),
            StructField("station_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("elevation", IntegerType(), True),
            StructField("city", StringType(), True)
        ])
        empty_stations = spark.createDataFrame([], empty_stations_schema)
        empty_stations.write.mode("overwrite").parquet(f"s3a://{trusted_bucket}/weather_stations/")
    
    # Procesar eventos climáticos
    try:
        events_df = spark.read.option("multiline", "true").json(f"s3a://{raw_bucket}/database/climate_events/*/*/*/*/climate_events_data.json")
        
        if events_df.count() > 0:
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
            
            # Convertir event_date y agregar particiones
            events_clean = events_clean.withColumn("event_date", to_date(col("event_date"))) \
                                     .withColumn("year", year(col("event_date")))
            
            events_clean.write.mode("overwrite") \
                .partitionBy("year") \
                .parquet(f"s3a://{trusted_bucket}/climate_events/")
            
            print(f"✅ Procesados {events_clean.count()} registros de eventos climáticos")
        else:
            print("⚠️ No se encontraron eventos climáticos")
            
    except Exception as e:
        print(f"⚠️ No se pudieron procesar eventos climáticos: {e}")
    
    # Procesar umbrales meteorológicos
    try:
        thresholds_df = spark.read.option("multiline", "true").json(f"s3a://{raw_bucket}/database/weather_thresholds/*/*/*/*/weather_thresholds_data.json")
        
        if thresholds_df.count() > 0:
            thresholds_clean = thresholds_df.select(
                col("threshold_id"),
                col("station_id"),
                col("parameter_name"),
                col("min_value"),
                col("max_value"),
                col("alert_level"),
                col("notification_enabled"),
                col("station_name"),
                col("city")
            ).filter(col("threshold_id").isNotNull())
            
            thresholds_clean.write.mode("overwrite") \
                .parquet(f"s3a://{trusted_bucket}/weather_thresholds/")
            
            print(f"✅ Procesados {thresholds_clean.count()} umbrales de alerta")
        else:
            print("⚠️ No se encontraron umbrales de alerta")
            
    except Exception as e:
        print(f"⚠️ No se pudieron procesar umbrales: {e}")

def create_integrated_dataset(spark, trusted_bucket):
    """Crear dataset integrado combinando API y BD"""
    print("🔗 Creando dataset integrado...")
    
    try:
        # Leer datos procesados
        weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/weather_data/")
        
        print(f"📊 Datos meteorológicos cargados: {weather_df.count()} registros")
        
        try:
            stations_df = spark.read.parquet(f"s3a://{trusted_bucket}/weather_stations/")
            print(f"📊 Estaciones cargadas: {stations_df.count()} registros")
            
            # Hacer join por ciudad (aproximado)
            integrated_df = weather_df.join(
                stations_df,
                (weather_df.city_name == stations_df.city) |
                ((abs(weather_df.latitude - stations_df.latitude) < 0.1) &
                 (abs(weather_df.longitude - stations_df.longitude) < 0.1)),
                "left"
            ).select(
                weather_df["*"],
                stations_df.station_id,
                stations_df.station_name,
                stations_df.elevation,
                stations_df.station_type
            )
            
        except Exception as e:
            print(f"⚠️ No se pudieron cargar estaciones, usando solo datos meteorológicos: {e}")
            # Si no hay estaciones, usar solo datos meteorológicos
            integrated_df = weather_df.withColumn("station_id", lit(None).cast(StringType())) \
                                     .withColumn("station_name", lit(None).cast(StringType())) \
                                     .withColumn("elevation", lit(None).cast(IntegerType())) \
                                     .withColumn("station_type", lit(None).cast(StringType()))
        
        # Escribir dataset integrado
        integrated_df.write.mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
        
        print(f"✅ Dataset integrado creado con {integrated_df.count()} registros")
        
    except Exception as e:
        print(f"❌ Error creando dataset integrado: {e}")
        import traceback
        traceback.print_exc()
        raise e

def main():
    if len(sys.argv) != 3:
        print("Uso: spark-submit etl_weather_data.py <raw_bucket> <trusted_bucket>")
        sys.exit(1)
    
    raw_bucket = sys.argv[1]
    trusted_bucket = sys.argv[2]
    
    print(f"🚀 Iniciando ETL Weather Data")
    print(f"📥 Raw bucket: {raw_bucket}")
    print(f"📤 Trusted bucket: {trusted_bucket}")
    
    # Crear sesión Spark
    spark = create_spark_session()
    
    try:
        # Procesar datos de API
        weather_clean = process_weather_api_data(spark, raw_bucket, trusted_bucket)
        
        # Procesar datos de BD
        process_database_data(spark, raw_bucket, trusted_bucket)
        
        # Crear dataset integrado
        create_integrated_dataset(spark, trusted_bucket)
        
        print("🎉 ETL completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error en ETL: {e}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()