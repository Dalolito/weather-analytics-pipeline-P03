from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def create_spark_session():
    """Crear sesión de Spark"""
    return SparkSession.builder \
        .appName("WeatherAnalytics-Trends") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def analyze_temperature_trends(spark, trusted_bucket, refined_bucket):
    """Analizar tendencias de temperatura"""
    print("🌡️ Analizando tendencias de temperatura...")
    
    # Leer datos integrados
    weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
    
    # Análisis mensual por ciudad
    monthly_trends = weather_df.groupBy("city_name", "year", "month") \
        .agg(
            avg("temp_avg").alias("avg_temperature"),
            max("temp_max").alias("max_temperature"),
            min("temp_min").alias("min_temperature"),
            avg("precipitation").alias("avg_precipitation"),
            avg("humidity").alias("avg_humidity"),
            count("*").alias("data_points")
        ).orderBy("city_name", "year", "month")
    
    # Escribir resultados
    monthly_trends.write.mode("overwrite") \
        .partitionBy("year") \
        .parquet(f"s3a://{refined_bucket}/temperature_trends_monthly/")

def analyze_extreme_weather(spark, trusted_bucket, refined_bucket):
    """Analizar eventos meteorológicos extremos"""
    print("⛈️ Analizando eventos meteorológicos extremos...")
    
    weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
    
    # Definir umbrales para eventos extremos
    extreme_events = weather_df.filter(
        (col("temp_max") > 35) |  # Calor extremo
        (col("temp_min") < 0) |   # Frío extremo
        (col("precipitation") > 50) |  # Lluvia intensa
        (col("wind_speed") > 60)  # Vientos fuertes
    ).withColumn("event_type", 
        when(col("temp_max") > 35, "extreme_heat")
        .when(col("temp_min") < 0, "extreme_cold")
        .when(col("precipitation") > 50, "heavy_rain")
        .when(col("wind_speed") > 60, "strong_winds")
        .otherwise("other")
    )
    
    # Contar eventos por ciudad y tipo
    extreme_summary = extreme_events.groupBy("city_name", "event_type", "year") \
        .agg(
            count("*").alias("event_count"),
            avg("temp_avg").alias("avg_temp_during_event"),
            max("precipitation").alias("max_precipitation"),
            max("wind_speed").alias("max_wind_speed")
        )
    
    extreme_summary.write.mode("overwrite") \
        .partitionBy("year") \
        .parquet(f"s3a://{refined_bucket}/extreme_weather_events/")

def create_weather_summary(spark, trusted_bucket, refined_bucket):
    """Crear resumen general del clima"""
    print("📊 Creando resumen general del clima...")
    
    weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
    
    # Resumen anual por ciudad
    annual_summary = weather_df.groupBy("city_name", "year") \
        .agg(
            avg("temp_avg").alias("annual_avg_temp"),
            max("temp_max").alias("annual_max_temp"),
            min("temp_min").alias("annual_min_temp"),
            sum("precipitation").alias("annual_precipitation"),
            avg("humidity").alias("annual_avg_humidity"),
            avg("wind_speed").alias("annual_avg_wind"),
            count("*").alias("total_measurements")
        )
    
    # Clasificar clima de la ciudad
    climate_classified = annual_summary.withColumn("climate_category",
        when((col("annual_avg_temp") > 25) & (col("annual_precipitation") > 1000), "tropical")
        .when((col("annual_avg_temp") > 20) & (col("annual_precipitation") < 500), "arid")
        .when(col("annual_avg_temp") < 10, "cold")
        .otherwise("temperate")
    )
    
    climate_classified.write.mode("overwrite") \
        .partitionBy("year") \
        .parquet(f"s3a://{refined_bucket}/annual_weather_summary/")

def main():
    if len(sys.argv) != 3:
        print("Uso: spark-submit analytics_weather_trends.py <trusted_bucket> <refined_bucket>")
        sys.exit(1)
    
    trusted_bucket = sys.argv[1]
    refined_bucket = sys.argv[2]
    
    spark = create_spark_session()
    
    try:
        # Ejecutar análisis
        analyze_temperature_trends(spark, trusted_bucket, refined_bucket)
        analyze_extreme_weather(spark, trusted_bucket, refined_bucket)
        create_weather_summary(spark, trusted_bucket, refined_bucket)
        
        print("🎉 Análisis completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error en análisis: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()  