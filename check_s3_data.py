#!/usr/bin/env python3
"""
Script para verificar datos en S3
"""
import boto3
import json
import os
from dotenv import load_dotenv

load_dotenv()

def check_s3_data():
    """Verificar qué datos tenemos en S3"""
    print("🔍 VERIFICANDO DATOS EN S3")
    print("="*40)
    
    # Cargar configuración de buckets
    try:
        with open('config/buckets.json', 'r') as f:
            buckets = json.load(f)
    except FileNotFoundError:
        print("❌ config/buckets.json no encontrado")
        return False
    
    s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION'))
    
    # Verificar cada bucket
    for bucket_type, bucket_name in buckets.items():
        print(f"\n📂 Bucket {bucket_type}: {bucket_name}")
        try:
            response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=50)
            
            if 'Contents' in response:
                print(f"   📁 {len(response['Contents'])} objetos encontrados:")
                
                for obj in response['Contents'][:10]:  # Mostrar primeros 10
                    size = obj['Size']
                    modified = obj['LastModified'].strftime('%Y-%m-%d %H:%M')
                    
                    if size > 0:
                        print(f"   ✅ {obj['Key']} ({size} bytes) - {modified}")
                    else:
                        print(f"   ⚠️ {obj['Key']} (VACÍO) - {modified}")
                
                if len(response['Contents']) > 10:
                    print(f"   ... y {len(response['Contents']) - 10} objetos más")
            else:
                print("   📭 Bucket vacío")
                
        except Exception as e:
            print(f"   ❌ Error accediendo bucket: {e}")
    
    # Verificar específicamente datos de ingesta
    print(f"\n🔍 VERIFICANDO DATOS ESPECÍFICOS")
    print("="*40)
    
    # Verificar datos de API
    try:
        raw_bucket = buckets['raw']
        response = s3_client.list_objects_v2(
            Bucket=raw_bucket, 
            Prefix='weather-api/',
            MaxKeys=20
        )
        
        api_files = response.get('Contents', [])
        non_empty_api = [f for f in api_files if f['Size'] > 0]
        
        print(f"📡 Datos de API:")
        print(f"   • Total archivos: {len(api_files)}")
        print(f"   • Archivos con datos: {len(non_empty_api)}")
        
        if non_empty_api:
            print(f"   📋 Últimos archivos:")
            for f in non_empty_api[-3:]:
                print(f"      ✅ {f['Key']} ({f['Size']} bytes)")
        
    except Exception as e:
        print(f"❌ Error verificando datos API: {e}")
    
    # Verificar datos de BD
    try:
        response = s3_client.list_objects_v2(
            Bucket=raw_bucket, 
            Prefix='database/',
            MaxKeys=20
        )
        
        db_files = response.get('Contents', [])
        non_empty_db = [f for f in db_files if f['Size'] > 0]
        
        print(f"\n🗄️ Datos de Base de Datos:")
        print(f"   • Total archivos: {len(db_files)}")
        print(f"   • Archivos con datos: {len(non_empty_db)}")
        
        if non_empty_db:
            print(f"   📋 Archivos encontrados:")
            for f in non_empty_db:
                print(f"      ✅ {f['Key']} ({f['Size']} bytes)")
        
    except Exception as e:
        print(f"❌ Error verificando datos BD: {e}")

def sample_data_content():
    """Mostrar contenido de muestra de un archivo"""
    print(f"\n🔍 MUESTRA DE CONTENIDO")
    print("="*40)
    
    try:
        with open('config/buckets.json', 'r') as f:
            buckets = json.load(f)
        
        s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION'))
        raw_bucket = buckets['raw']
        
        # Buscar un archivo de API
        response = s3_client.list_objects_v2(
            Bucket=raw_bucket, 
            Prefix='weather-api/',
            MaxKeys=5
        )
        
        api_files = [f for f in response.get('Contents', []) if f['Size'] > 0]
        
        if api_files:
            # Leer el primer archivo
            first_file = api_files[0]
            print(f"📄 Leyendo: {first_file['Key']}")
            
            obj = s3_client.get_object(Bucket=raw_bucket, Key=first_file['Key'])
            content = obj['Body'].read().decode('utf-8')
            
            try:
                data = json.loads(content)
                print(f"✅ JSON válido:")
                print(f"   • Claves principales: {list(data.keys())}")
                
                if 'city_info' in data:
                    print(f"   • Ciudad: {data['city_info'].get('name', 'N/A')}")
                
                if 'daily' in data and 'time' in data['daily']:
                    dates = data['daily']['time']
                    print(f"   • Fechas: {len(dates)} días (desde {dates[0]} hasta {dates[-1]})")
                
                if 'ingestion_timestamp' in data:
                    print(f"   • Timestamp: {data['ingestion_timestamp']}")
                    
            except json.JSONDecodeError as e:
                print(f"❌ JSON inválido: {e}")
                print(f"Contenido (primeros 200 chars): {content[:200]}")
        else:
            print("❌ No se encontraron archivos de API con datos")
            
    except Exception as e:
        print(f"❌ Error leyendo contenido: {e}")

if __name__ == "__main__":
    check_s3_data()
    sample_data_content()