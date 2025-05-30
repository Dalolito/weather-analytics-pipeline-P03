"""
Tests para validar conexiones del proyecto
"""
import pytest
import boto3
import requests
import mysql.connector
import json
import os
from dotenv import load_dotenv
from botocore.exceptions import NoCredentialsError, ClientError

# Cargar variables de entorno
load_dotenv()

def test_s3_connection():
    """Test conexión a S3"""
    try:
        # Crear cliente S3 con región explícita
        s3_client = boto3.client(
            's3', 
            region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        )
        
        # Intentar listar buckets
        response = s3_client.list_buckets()
        
        # Verificar que la respuesta sea exitosa
        assert 'Buckets' in response
        print(f"✅ S3 connection successful. Found {len(response['Buckets'])} buckets")
        
    except NoCredentialsError:
        pytest.skip("AWS credentials not configured")
    except Exception as e:
        pytest.fail(f"S3 connection failed: {e}")

def test_openmeteo_api():
    """Test API de Open-Meteo"""
    try:
        # Test de la API con parámetros básicos
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            'latitude': 6.25,
            'longitude': -75.56,
            'current_weather': 'true'
        }
        
        response = requests.get(url, params=params, timeout=10)
        
        # Verificar que la respuesta sea exitosa
        assert response.status_code == 200
        
        data = response.json()
        assert 'current_weather' in data
        print("✅ OpenMeteo API connection successful")
        
    except requests.exceptions.RequestException as e:
        pytest.fail(f"OpenMeteo API connection failed: {e}")
    except Exception as e:
        pytest.fail(f"OpenMeteo API test failed: {e}")

def test_database_connection():
    """Test conexión a base de datos MySQL"""
    # Obtener configuración de BD
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER', 'admin'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME', 'weather_data'),
        'port': int(os.getenv('DB_PORT', 3306))
    }
    
    # Verificar que las variables estén configuradas
    if not all([db_config['host'], db_config['password']]):
        pytest.skip("Database credentials not configured in .env file")
    
    try:
        # Intentar conexión
        connection = mysql.connector.connect(**db_config)
        
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            
            assert result[0] == 1
            print("✅ Database connection successful")
            
            cursor.close()
            connection.close()
        else:
            pytest.fail("Database connection failed - not connected")
            
    except mysql.connector.Error as e:
        if "Can't connect to MySQL server" in str(e):
            pytest.skip(f"Database server not available: {e}")
        else:
            pytest.fail(f"Database connection error: {e}")
    except Exception as e:
        pytest.fail(f"Database test failed: {e}")

def test_bucket_configuration():
    """Test configuración de buckets"""
    try:
        # Verificar que existe el archivo de configuración
        config_path = 'config/buckets.json'
        assert os.path.exists(config_path), "buckets.json configuration file not found"
        
        # Cargar configuración
        with open(config_path, 'r') as f:
            buckets = json.load(f)
        
        # Verificar buckets requeridos
        required_buckets = ['raw', 'trusted', 'refined', 'scripts']
        for bucket_type in required_buckets:
            assert bucket_type in buckets, f"Missing {bucket_type} bucket configuration"
            assert buckets[bucket_type], f"Empty {bucket_type} bucket name"
        
        print("✅ Bucket configuration valid")
        print(f"   Raw: {buckets['raw']}")
        print(f"   Trusted: {buckets['trusted']}")
        print(f"   Refined: {buckets['refined']}")
        print(f"   Scripts: {buckets['scripts']}")
        
    except FileNotFoundError:
        pytest.fail("config/buckets.json file not found")
    except json.JSONDecodeError as e:
        pytest.fail(f"Invalid JSON in buckets.json: {e}")
    except Exception as e:
        pytest.fail(f"Bucket configuration test failed: {e}")

def test_environment_variables():
    """Test variables de entorno críticas"""
    required_vars = [
        'AWS_DEFAULT_REGION'
    ]
    
    optional_vars = [
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'DB_HOST',
        'DB_USER',
        'DB_PASSWORD',
        'DB_NAME'
    ]
    
    # Verificar variables requeridas
    for var in required_vars:
        value = os.getenv(var)
        assert value is not None, f"Required environment variable {var} not set"
        print(f"✅ {var}: {value}")
    
    # Verificar variables opcionales (solo mostrar si están configuradas)
    for var in optional_vars:
        value = os.getenv(var)
        if value:
            # Mostrar solo los primeros caracteres para seguridad
            display_value = value[:10] + "..." if len(value) > 10 else value
            print(f"ℹ️  {var}: {display_value}")
        else:
            print(f"⚠️  {var}: Not configured")

if __name__ == "__main__":
    # Ejecutar tests individuales para debugging
    print("🧪 Running connection tests...\n")
    
    try:
        test_environment_variables()
        print()
        test_s3_connection()
        print()
        test_openmeteo_api()
        print()
        test_database_connection()
        print()
        test_bucket_configuration()
        print("\n🎉 All tests completed!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")