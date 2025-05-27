from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression, RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml import Pipeline
import sys

def create_spark_session():
    """Crear sesión de Spark"""
    return SparkSession.builder \
        .appName("WeatherAnalytics-ML") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()

def prepare_features(spark, trusted_bucket):
    """Preparar características para ML"""
    print("🧮 Preparando características para ML...")
    
    # Leer datos integrados
    weather_df = spark.read.parquet(f"s3a://{trusted_bucket}/integrated_weather_data/")
    
    # Seleccionar características relevantes
    feature_df = weather_df.select(
        "city_name",
        "date",
        "temp_avg",
        "temp_max",
        "temp_min",
        "precipitation",
        "humidity",
        "wind_speed",
        "elevation",
        "year",
        "month"
    ).filter(
        col("temp_avg").isNotNull() &
        col("precipitation").isNotNull() &
        col("humidity").isNotNull() &
        col("wind_speed").isNotNull()
    )
    
    # Agregar características temporales
    feature_df = feature_df.withColumn("day_of_year", dayofyear(col("date"))) \
                          .withColumn("season", 
                              when(col("month").isin([12, 1, 2]), 1)  # Invierno
                              .when(col("month").isin([3, 4, 5]), 2)  # Primavera
                              .when(col("month").isin([6, 7, 8]), 3)  # Verano
                              .otherwise(4)  # Otoño
                          )
    
    # Crear características de lag (valores anteriores)
    from pyspark.sql.window import Window
    
    window_spec = Window.partitionBy("city_name").orderBy("date")
    
    feature_df = feature_df.withColumn("temp_lag1", lag("temp_avg", 1).over(window_spec)) \
                          .withColumn("temp_lag2", lag("temp_avg", 2).over(window_spec)) \
                          .withColumn("precip_lag1", lag("precipitation", 1).over(window_spec))
    
    # Filtrar filas con valores null después del lag
    feature_df = feature_df.filter(
        col("temp_lag1").isNotNull() &
        col("temp_lag2").isNotNull() &
        col("precip_lag1").isNotNull()
    )
    
    return feature_df

def train_temperature_prediction_model(spark, feature_df, refined_bucket):
    """Entrenar modelo de predicción de temperatura"""
    print("🤖 Entrenando modelo de predicción de temperatura...")
    
    # Definir características
    feature_cols = [
        "temp_lag1", "temp_lag2", "precipitation", "humidity", 
        "wind_speed", "elevation", "day_of_year", "season", "precip_lag1"
    ]
    
    # Ensamblar características
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Escalar características
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    
    # Modelo de regresión
    rf = RandomForestRegressor(
        featuresCol="scaled_features",
        labelCol="temp_avg",
        numTrees=100,
        maxDepth=10
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[assembler, scaler, rf])
    
    # Dividir datos en entrenamiento y prueba
    train_data, test_data = feature_df.randomSplit([0.8, 0.2], seed=42)
    
    # Entrenar modelo
    model = pipeline.fit(train_data)
    
    # Hacer predicciones
    predictions = model.transform(test_data)
    
    # Evaluar modelo
    evaluator = RegressionEvaluator(
        labelCol="temp_avg",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"📊 RMSE del modelo de temperatura: {rmse:.2f}")
    
    # Guardar métricas
    metrics_df = spark.createDataFrame([
        ("temperature_prediction", "rmse", rmse, datetime.now().isoformat())
    ], ["model_name", "metric_name", "metric_value", "evaluation_date"])
    
    metrics_df.write.mode("append") \
        .parquet(f"s3a://{refined_bucket}/ml_model_metrics/")
    
    # Guardar predicciones de muestra
    sample_predictions = predictions.select(
        "city_name", "date", "temp_avg", "prediction",
        (col("prediction") - col("temp_avg")).alias("error")
    ).limit(1000)
    
    sample_predictions.write.mode("overwrite") \
        .parquet(f"s3a://{refined_bucket}/temperature_predictions/")
    
    print("✅ Modelo de temperatura entrenado y guardado")

def train_precipitation_prediction_model(spark, feature_df, refined_bucket):
    """Entrenar modelo de predicción de precipitación"""
    print("🌧️ Entrenando modelo de predicción de precipitación...")
    
    # Características para precipitación
    feature_cols = [
        "temp_avg", "humidity", "wind_speed", "elevation", 
        "day_of_year", "season", "precip_lag1"
    ]
    
    # Ensamblar características
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    
    # Escalar características
    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    
    # Modelo de regresión lineal para precipitación
    lr = LinearRegression(
        featuresCol="scaled_features",
        labelCol="precipitation",
        maxIter=100
    )
    
    # Pipeline
    pipeline = Pipeline(stages=[assembler, scaler, lr])
    
    # Dividir datos
    train_data, test_data = feature_df.randomSplit([0.8, 0.2], seed=42)
    
    # Entrenar modelo
    model = pipeline.fit(train_data)
    
    # Hacer predicciones
    predictions = model.transform(test_data)
    
    # Evaluar modelo
    evaluator = RegressionEvaluator(
        labelCol="precipitation",
        predictionCol="prediction",
        metricName="rmse"
    )
    
    rmse = evaluator.evaluate(predictions)
    print(f"📊 RMSE del modelo de precipitación: {rmse:.2f}")
    
    # Guardar métricas
    metrics_df = spark.createDataFrame([
        ("precipitation_prediction", "rmse", rmse, datetime.now().isoformat())
    ], ["model_name", "metric_name", "metric_value", "evaluation_date"])
    
    metrics_df.write.mode("append") \
        .parquet(f"s3a://{refined_bucket}/ml_model_metrics/")
    
    # Guardar predicciones de muestra
    sample_predictions = predictions.select(
        "city_name", "date", "precipitation", "prediction",
        (col("prediction") - col("precipitation")).alias("error")
    ).limit(1000)
    
    sample_predictions.write.mode("overwrite") \
        .parquet(f"s3a://{refined_bucket}/precipitation_predictions/")
    
    print("✅ Modelo de precipitación entrenado y guardado")

def create_weather_forecast(spark, feature_df, refined_bucket):
    """Crear pronósticos meteorológicos simples"""
    print("🔮 Creando pronósticos meteorológicos...")
    
    # Calcular promedios históricos por ciudad y día del año
    historical_averages = feature_df.groupBy("city_name", "day_of_year") \
        .agg(
            avg("temp_avg").alias("historical_avg_temp"),
            avg("precipitation").alias("historical_avg_precip"),
            avg("humidity").alias("historical_avg_humidity"),
            count("*").alias("years_of_data")
        ).filter(col("years_of_data") >= 2)  # Al menos 2 años de datos
    
    # Crear pronósticos para los próximos 30 días
    from datetime import datetime, timedelta
    import calendar
    
    # Obtener día actual del año
    current_day = datetime.now().timetuple().tm_yday
    
    # Crear fechas futuras
    future_days = []
    for i in range(1, 31):  # Próximos 30 días
        future_day = (current_day + i) % 366
        if future_day == 0:
            future_day = 366
        future_date = datetime.now() + timedelta(days=i)
        future_days.append((future_day, future_date.strftime('%Y-%m-%d')))
    
    future_df = spark.createDataFrame(future_days, ["day_of_year", "forecast_date"])
    
    # Hacer join con promedios históricos
    forecasts = historical_averages.join(future_df, "day_of_year") \
        .select(
            "city_name",
            "forecast_date",
            "historical_avg_temp",
            "historical_avg_precip",
            "historical_avg_humidity",
            "years_of_data"
        ).withColumn("forecast_type", lit("historical_average"))
    
    # Guardar pronósticos
    forecasts.write.mode("overwrite") \
        .parquet(f"s3a://{refined_bucket}/weather_forecasts/")
    
    print(f"✅ Pronósticos creados para {forecasts.count()} combinaciones ciudad-fecha")

def main():
    if len(sys.argv) != 3:
        print("Uso: spark-submit ml_weather_prediction.py <trusted_bucket> <refined_bucket>")
        sys.exit(1)
    
    trusted_bucket = sys.argv[1]
    refined_bucket = sys.argv[2]
    
    spark = create_spark_session()
    
    try:
        # Preparar características
        feature_df = prepare_features(spark, trusted_bucket)
        
        # Entrenar modelos
        train_temperature_prediction_model(spark, feature_df, refined_bucket)
        train_precipitation_prediction_model(spark, feature_df, refined_bucket)
        
        # Crear pronósticos
        create_weather_forecast(spark, feature_df, refined_bucket)
        
        print("🎉 Machine Learning completado exitosamente!")
        
    except Exception as e:
        print(f"❌ Error en ML: {e}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    main()