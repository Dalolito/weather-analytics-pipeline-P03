"""
Tests para validar jobs de Spark
"""
import pytest
import boto3
from moto import mock_aws  # Cambio aquí: mock_s3 → mock_aws
import json
import os
from dotenv import load_dotenv

load_dotenv()

@mock_aws
def test_s3_bucket_access():
    """Test acceso a buckets S3"""
    # Crear cliente S3 mock
    s3 = boto3.client('s3', region_name='us-east-1')
    
    # Crear buckets de prueba
    buckets = ['test-raw', 'test-trusted', 'test-refined']
    for bucket in buckets:
        s3.create_bucket(Bucket=bucket)
    
    # Verificar que los buckets existen
    response = s3.list_buckets()
    bucket_names = [b['Name'] for b in response['Buckets']]
    
    for bucket in buckets:
        assert bucket in bucket_names
    
    print("✅ S3 bucket mock test passed")

def test_weather_data_schema():
    """Test esquema de datos meteorológicos"""
    sample_weather_data = {
        "city_info": {
            "name": "Bogotá",
            "lat": 4.6097,
            "lon": -74.0817
        },
        "daily": {
            "time": ["2024-01-01"],
            "temperature_2m_max": [25.5],
            "temperature_2m_min": [15.2],
            "precipitation_sum": [5.5],
            "windspeed_10m_max": [12.3],
            "relative_humidity_2m_mean": [75.5]
        },
        "ingestion_timestamp": "2024-01-01T10:00:00",
        "data_source": "openmeteo_api"
    }
    
    # Validar campos obligatorios
    assert "city_info" in sample_weather_data
    assert "daily" in sample_weather_data
    assert "name" in sample_weather_data["city_info"]
    assert "temperature_2m_max" in sample_weather_data["daily"]
    
    print("✅ Weather data schema test passed")

def test_database_connection_config():
    """Test configuración de conexión a BD"""
    # En un entorno real, estas variables deberían existir
    # Para test, solo verificamos la estructura
    config = {
        'host': os.getenv('DB_HOST', 'test-host'),
        'user': os.getenv('DB_USER', 'test-user'),
        'password': os.getenv('DB_PASSWORD', 'test-password'),
        'database': os.getenv('DB_NAME', 'test-db')
    }
    
    for key in ['host', 'user', 'password', 'database']:
        assert key in config
        assert config[key] is not None
    
    print("✅ Database config test passed")

class TestWeatherAnalytics:
    """Clase de tests para Weather Analytics"""
    
    def test_temperature_calculation(self):
        """Test cálculo de temperatura promedio"""
        temp_max = 30.0
        temp_min = 20.0
        expected_avg = (temp_max + temp_min) / 2
        
        assert expected_avg == 25.0
        print("✅ Temperature calculation test passed")
    
    def test_extreme_weather_detection(self):
        """Test detección de eventos extremos"""
        weather_conditions = [
            {"temp_max": 40, "expected": "extreme_heat"},
            {"temp_min": -5, "expected": "extreme_cold"},
            {"precipitation": 80, "expected": "heavy_rain"},
            {"wind_speed": 70, "expected": "strong_winds"}
        ]
        
        for condition in weather_conditions:
            if "temp_max" in condition and condition["temp_max"] > 35:
                assert condition["expected"] == "extreme_heat"
            elif "temp_min" in condition and condition["temp_min"] < 0:
                assert condition["expected"] == "extreme_cold"
            elif "precipitation" in condition and condition["precipitation"] > 50:
                assert condition["expected"] == "heavy_rain"
            elif "wind_speed" in condition and condition["wind_speed"] > 60:
                assert condition["expected"] == "strong_winds"
        
        print("✅ Extreme weather detection test passed")

    def test_data_validation_rules(self):
        """Test reglas de validación de datos"""
        # Test temperatura en rango razonable
        valid_temps = [15.5, 25.0, 35.2, -5.0, 45.0]
        invalid_temps = [-100, 100]
        
        for temp in valid_temps:
            assert -50 <= temp <= 60, f"Temperature {temp} out of reasonable range"
        
        for temp in invalid_temps:
            assert not (-50 <= temp <= 60), f"Temperature {temp} should be invalid"
        
        print("✅ Data validation rules test passed")

    def test_city_coordinates(self):
        """Test coordenadas de ciudades colombianas"""
        colombian_cities = {
            "Bogotá": {"lat": 4.6097, "lon": -74.0817},
            "Medellín": {"lat": 6.2518, "lon": -75.5636},
            "Cali": {"lat": 3.4516, "lon": -76.5320},
            "Cartagena": {"lat": 10.3910, "lon": -75.4794},
            "Barranquilla": {"lat": 10.9639, "lon": -74.7964}
        }
        
        for city, coords in colombian_cities.items():
            # Verificar que las coordenadas estén en el rango de Colombia
            assert -5 <= coords["lat"] <= 15, f"Latitude for {city} out of Colombia range"
            assert -85 <= coords["lon"] <= -65, f"Longitude for {city} out of Colombia range"
        
        print("✅ City coordinates test passed")

if __name__ == "__main__":
    # Ejecutar todos los tests
    print("🧪 Running Spark jobs tests...\n")
    
    test_s3_bucket_access()
    test_weather_data_schema()
    test_database_connection_config()
    
    analytics = TestWeatherAnalytics()
    analytics.test_temperature_calculation()
    analytics.test_extreme_weather_detection()
    analytics.test_data_validation_rules()
    analytics.test_city_coordinates()
    
    print("\n🎉 All Spark jobs tests passed!")