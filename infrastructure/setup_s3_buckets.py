"""
Script para crear y configurar buckets S3
"""
import boto3
import json
import os
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

def create_s3_buckets():
    """Crear buckets S3 necesarios para el proyecto"""
    print("🪣 Configurando buckets S3...")
    
    # Cliente S3
    s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
    
    # Nombres de buckets únicos
    timestamp = datetime.now().strftime('%Y%m%d')
    bucket_config = {
        'raw': f'weather-analytics-pipeline-raw-{timestamp}',
        'trusted': f'weather-analytics-pipeline-trusted-{timestamp}',
        'refined': f'weather-analytics-pipeline-refined-{timestamp}',
        'scripts': f'weather-analytics-pipeline-scripts-{timestamp}'
    }
    
    created_buckets = {}
    
    for bucket_type, bucket_name in bucket_config.items():
        try:
            print(f"📁 Creando bucket {bucket_type}: {bucket_name}")
            
            # Crear bucket
            if region == 'us-east-1':
                # us-east-1 no necesita LocationConstraint
                s3_client.create_bucket(Bucket=bucket_name)
            else:
                s3_client.create_bucket(
                    Bucket=bucket_name,
                    CreateBucketConfiguration={'LocationConstraint': region}
                )
            
            # Configurar versionado
            s3_client.put_bucket_versioning(
                Bucket=bucket_name,
                VersioningConfiguration={'Status': 'Enabled'}
            )
            
            # Configurar encriptación
            s3_client.put_bucket_encryption(
                Bucket=bucket_name,
                ServerSideEncryptionConfiguration={
                    'Rules': [
                        {
                            'ApplyServerSideEncryptionByDefault': {
                                'SSEAlgorithm': 'AES256'
                            }
                        }
                    ]
                }
            )
            
            # Crear estructura de folders para bucket raw
            if bucket_type == 'raw':
                folders = [
                    'weather-api/',
                    'weather-historical/',
                    'database/'
                ]
                for folder in folders:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=folder,
                        Body=''
                    )
                    
            # Crear estructura para bucket trusted
            elif bucket_type == 'trusted':
                folders = [
                    'weather_data/',
                    'weather_stations/',
                    'climate_events/',
                    'integrated_weather_data/'
                ]
                for folder in folders:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=folder,
                        Body=''
                    )
                    
            # Crear estructura para bucket refined
            elif bucket_type == 'refined':
                folders = [
                    'temperature_trends_monthly/',
                    'extreme_weather_events/',
                    'annual_weather_summary/',
                    'ml_model_metrics/',
                    'temperature_predictions/',
                    'weather_forecasts/',
                    'athena-results/',
                    'validation_reports/'
                ]
                for folder in folders:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=folder,
                        Body=''
                    )
                    
            # Crear estructura para bucket scripts
            elif bucket_type == 'scripts':
                folders = [
                    'spark-jobs/',
                    'bootstrap/',
                    'logs/'
                ]
                for folder in folders:
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=folder,
                        Body=''
                    )
            
            created_buckets[bucket_type] = bucket_name
            print(f"✅ Bucket {bucket_type} creado exitosamente")
            
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'BucketAlreadyOwnedByYou':
                print(f"ℹ️ Bucket {bucket_name} ya existe y es tuyo")
                created_buckets[bucket_type] = bucket_name
            elif error_code == 'BucketAlreadyExists':
                print(f"❌ Bucket {bucket_name} ya existe (propiedad de otra cuenta)")
                # Intentar con un nombre más único
                unique_bucket = f"{bucket_name}-{os.getenv('USER', 'user')}"
                try:
                    if region == 'us-east-1':
                        s3_client.create_bucket(Bucket=unique_bucket)
                    else:
                        s3_client.create_bucket(
                            Bucket=unique_bucket,
                            CreateBucketConfiguration={'LocationConstraint': region}
                        )
                    created_buckets[bucket_type] = unique_bucket
                    print(f"✅ Bucket alternativo creado: {unique_bucket}")
                except Exception as e2:
                    print(f"❌ Error creando bucket alternativo: {e2}")
            else:
                print(f"❌ Error creando bucket {bucket_name}: {e}")
    
    # Guardar configuración
    os.makedirs('config', exist_ok=True)
    with open('config/buckets.json', 'w') as f:
        json.dump(created_buckets, f, indent=2)
    
    print(f"\n✅ Configuración guardada en config/buckets.json")
    return created_buckets

def verify_buckets():
    """Verificar que los buckets estén accesibles"""
    print("\n🔍 Verificando acceso a buckets...")
    
    try:
        with open('config/buckets.json', 'r') as f:
            buckets = json.load(f)
    except FileNotFoundError:
        print("❌ config/buckets.json no encontrado")
        return False
    
    s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    
    all_accessible = True
    for bucket_type, bucket_name in buckets.items():
        try:
            # Intentar listar objetos
            response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            print(f"✅ {bucket_type}: {bucket_name} - Accesible")
        except Exception as e:
            print(f"❌ {bucket_type}: {bucket_name} - Error: {e}")
            all_accessible = False
    
    return all_accessible

def main():
    """Función principal"""
    print("🚀 CONFIGURACIÓN DE BUCKETS S3")
    print("="*50)
    
    try:
        # Verificar credenciales AWS
        sts = boto3.client('sts', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        identity = sts.get_caller_identity()
        print(f"🔑 Conectado como: {identity.get('Arn', 'Unknown')}")
        
        # Crear buckets
        created_buckets = create_s3_buckets()
        
        # Verificar acceso
        if verify_buckets():
            print("\n🎉 Todos los buckets configurados exitosamente!")
            print("\n📋 Buckets creados:")
            for bucket_type, bucket_name in created_buckets.items():
                print(f"   • {bucket_type}: {bucket_name}")
        else:
            print("\n⚠️ Algunos buckets tienen problemas de acceso")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error en configuración: {e}")
        return False

if __name__ == "__main__":
    main()