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
    
    try:
        # Leer datos integrados
        weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
        
        print(f"📊 Registros de datos integrados: {weather_df.count()}")
        
        # Verificar si tenemos datos
        if weather_df.count() == 0:
            print("⚠️ No hay datos integrados, intentando con datos básicos...")
            weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/weather_data/")
        
        # Verificar columnas necesarias
        columns = weather_df.columns
        print(f"📋 Columnas disponibles: {columns}")
        
        # Análisis mensual por ciudad con manejo robusto de columnas
        if "temp_avg" in columns and "city_name" in columns:
            # Filtrar datos válidos
            valid_weather = weather_df.filter(
                col("temp_avg").isNotNull() & 
                col("city_name").isNotNull() &
                col("year").isNotNull() &
                col("month").isNotNull()
            )
            
            print(f"📊 Registros válidos para análisis: {valid_weather.count()}")
            
            # Crear agregaciones con manejo de columnas opcionales
            agg_expressions = [
                avg("temp_avg").alias("avg_temperature"),
                count("*").alias("data_points")
            ]
            
            # Agregar métricas opcionales si existen
            if "temp_max" in columns:
                agg_expressions.append(max("temp_max").alias("max_temperature"))
            else:
                agg_expressions.append(lit(None).cast(DoubleType()).alias("max_temperature"))
            
            if "temp_min" in columns:
                agg_expressions.append(min("temp_min").alias("min_temperature"))
            else:
                agg_expressions.append(lit(None).cast(DoubleType()).alias("min_temperature"))
            
            if "precipitation" in columns:
                agg_expressions.append(avg("precipitation").alias("avg_precipitation"))
            else:
                agg_expressions.append(lit(None).cast(DoubleType()).alias("avg_precipitation"))
            
            if "humidity" in columns:
                agg_expressions.append(avg("humidity").alias("avg_humidity"))
            else:
                agg_expressions.append(lit(None).cast(DoubleType()).alias("avg_humidity"))
            
            monthly_trends = valid_weather.groupBy("city_name", "year", "month") \
                .agg(*agg_expressions) \
                .orderBy("city_name", "year", "month")
            
            print(f"📊 Tendencias mensuales calculadas: {monthly_trends.count()} registros")
            
            # Escribir resultados
            monthly_trends.write.mode("overwrite") \
                .partitionBy("year") \
                .parquet(f"s3a://{refined_bucket}/temperature_trends_monthly/")
            
            print("✅ Tendencias de temperatura guardadas")
            
        else:
            print("❌ Columnas necesarias no encontradas para análisis de temperatura")
            
    except Exception as e:
        print(f"❌ Error en análisis de temperatura: {e}")
        import traceback
        traceback.print_exc()

def analyze_extreme_weather(spark, trusted_bucket, refined_bucket):
    """Analizar eventos meteorológicos extremos"""
    print("⛈️ Analizando eventos meteorológicos extremos...")
    
    try:
        # Leer datos integrados
        weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
        
        if weather_df.count() == 0:
            weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/weather_data/")
        
        columns = weather_df.columns
        
        # Definir umbrales para eventos extremos (ajustados para Colombia)
        conditions = []
        
        if "temp_max" in columns:
            conditions.append(col("temp_max") > 35)  # Calor extremo
        if "temp_min" in columns:
            conditions.append(col("temp_min") < 5)   # Frío extremo (ajustado para Colombia)
        if "precipitation" in columns:
            conditions.append(col("precipitation") > 50)  # Lluvia intensa
        if "wind_speed" in columns:
            conditions.append(col("wind_speed") > 60)  # Vientos fuertes
        
        if conditions:
            # Combinar condiciones con OR
            extreme_condition = conditions[0]
            for condition in conditions[1:]:
                extreme_condition = extreme_condition | condition
            
            extreme_events = weather_df.filter(extreme_condition)
            
            # Clasificar tipo de evento
            event_type_expr = lit("other")
            if "temp_max" in columns:
                event_type_expr = when(col("temp_max") > 35, "extreme_heat").otherwise(event_type_expr)
            if "temp_min" in columns:
                event_type_expr = when(col("temp_min") < 5, "extreme_cold").otherwise(event_type_expr)
            if "precipitation" in columns:
                event_type_expr = when(col("precipitation") > 50, "heavy_rain").otherwise(event_type_expr)
            if "wind_speed" in columns:
                event_type_expr = when(col("wind_speed") > 60, "strong_winds").otherwise(event_type_expr)
            
            extreme_events = extreme_events.withColumn("event_type", event_type_expr)
            
            # Crear agregaciones para eventos extremos
            agg_extreme = [
                count("*").alias("event_count")
            ]
            
            if "temp_avg" in columns:
                agg_extreme.append(avg("temp_avg").alias("avg_temp_during_event"))
            else:
                agg_extreme.append(lit(None).cast(DoubleType()).alias("avg_temp_during_event"))
            
            if "precipitation" in columns:
                agg_extreme.append(max("precipitation").alias("max_precipitation"))
            else:
                agg_extreme.append(lit(None).cast(DoubleType()).alias("max_precipitation"))
            
            if "wind_speed" in columns:
                agg_extreme.append(max("wind_speed").alias("max_wind_speed"))
            else:
                agg_extreme.append(lit(None).cast(DoubleType()).alias("max_wind_speed"))
            
            # Contar eventos por ciudad y tipo
            extreme_summary = extreme_events.groupBy("city_name", "event_type", "year") \
                .agg(*agg_extreme)
            
            print(f"📊 Eventos extremos identificados: {extreme_summary.count()} registros")
            
            extreme_summary.write.mode("overwrite") \
                .partitionBy("year") \
                .parquet(f"s3a://{refined_bucket}/extreme_weather_events/")
            
            print("✅ Eventos extremos guardados")
        else:
            print("⚠️ No hay columnas suficientes para análisis de eventos extremos")
            
    except Exception as e:
        print(f"❌ Error en análisis de eventos extremos: {e}")
        import traceback
        traceback.print_exc()

def create_weather_summary(spark, trusted_bucket, refined_bucket):
    """Crear resumen general del clima"""
    print("📊 Creando resumen general del clima...")
    
    try:
        # Leer datos integrados
        weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
        
        if weather_df.count() == 0:
            weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/weather_data/")
        
        columns = weather_df.columns
        
        # Filtrar datos válidos
        valid_data = weather_df.filter(
            col("city_name").isNotNull() & 
            col("year").isNotNull()
        )
        
        # Crear agregaciones anuales
        agg_annual = [
            count("*").alias("total_measurements")
        ]
        
        if "temp_avg" in columns:
            agg_annual.extend([
                avg("temp_avg").alias("annual_avg_temp"),
                max("temp_avg").alias("annual_max_temp"),
                min("temp_avg").alias("annual_min_temp")
            ])
        else:
            agg_annual.extend([
                lit(None).cast(DoubleType()).alias("annual_avg_temp"),
                lit(None).cast(DoubleType()).alias("annual_max_temp"),
                lit(None).cast(DoubleType()).alias("annual_min_temp")
            ])
        
        if "precipitation" in columns:
            agg_annual.append(sum("precipitation").alias("annual_precipitation"))
        else:
            agg_annual.append(lit(None).cast(DoubleType()).alias("annual_precipitation"))
        
        if "humidity" in columns:
            agg_annual.append(avg("humidity").alias("annual_avg_humidity"))
        else:
            agg_annual.append(lit(None).cast(DoubleType()).alias("annual_avg_humidity"))
        
        if "wind_speed" in columns:
            agg_annual.append(avg("wind_speed").alias("annual_avg_wind"))
        else:
            agg_annual.append(lit(None).cast(DoubleType()).alias("annual_avg_wind"))
        
        # Resumen anual por ciudad
        annual_summary = valid_data.groupBy("city_name", "year") \
            .agg(*agg_annual)
        
        # Clasificar clima de la ciudad (solo si tenemos datos de temperatura y precipitación)
        if "temp_avg" in columns and "precipitation" in columns:
            climate_classified = annual_summary.withColumn("climate_category",
                when((col("annual_avg_temp") > 25) & (col("annual_precipitation") > 1000), "tropical")
                .when((col("annual_avg_temp") > 20) & (col("annual_precipitation") < 500), "arid")
                .when(col("annual_avg_temp") < 15, "cold")
                .otherwise("temperate")
            )
        else:
            climate_classified = annual_summary.withColumn("climate_category", lit("unknown"))
        
        print(f"📊 Resumen anual calculado: {climate_classified.count()} registros")
        
        climate_classified.write.mode("overwrite") \
            .partitionBy("year") \
            .parquet(f"s3a://{refined_bucket}/annual_weather_summary/")
        
        print("✅ Resumen del clima guardado")
        
    except Exception as e:
        print(f"❌ Error en resumen del clima: {e}")
        import traceback
        traceback.print_exc()

def main():
    if len(sys.argv) != 3:
        print("Uso: spark-submit analytics_weather_trends.py <trusted_bucket> <refined_bucket>")
        sys.exit(1)
    
    trusted_bucket = sys.argv[1]
    refined_bucket = sys.argv[2]
    
    print(f"🚀 Iniciando Analytics Weather Trends")
    print(f"📥 Trusted bucket: {trusted_bucket}")
    print(f"📤 Refined bucket: {refined_bucket}")
    
    spark = create_spark_session()
    
    try:
        # Ejecutar análisis
        analyze_temperature_trends(spark, trusted_bucket, refined_bucket)
        analyze_extreme_weather(spark, trusted_bucket, refined_bucket)
        create_weather_summary(spark, trusted_bucket, refined_bucket)
        
        print("🎉 Análisis completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error en análisis: {e}")
        import traceback
        traceback.print_exc()
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()