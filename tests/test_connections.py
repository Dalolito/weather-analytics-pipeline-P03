import boto3
import requests
import mysql.connector
import json
from config.config import DB_CONFIG, OPENMETEO_BASE_URL

def test_s3_connection():
    """Test conexión a S3"""
    try:
        s3 = boto3.client('s3')
        s3.list_buckets()
        print("✅ S3 connection successful")
        return True
    except Exception as e:
        print(f"❌ S3 connection failed: {e}")
        return False

def test_openmeteo_api():
    """Test API de Open-Meteo"""
    try:
        response = requests.get(f"{OPENMETEO_BASE_URL}/forecast?latitude=6.25&longitude=-75.56&current_weather=true")
        response.raise_for_status()
        print("✅ Open-Meteo API connection successful")
        return True
    except Exception as e:
        print(f"❌ Open-Meteo API connection failed: {e}")
        return False

def test_database_connection():
    """Test conexión a base de datos"""
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        connection.close()
        print("✅ Database connection successful")
        return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

if __name__ == "__main__":
    print("Testing connections...")
    test_s3_connection()
    test_openmeteo_api()
    test_database_connection()