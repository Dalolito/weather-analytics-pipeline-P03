import boto3
import json
import os
from botocore.exceptions import ClientError
from dotenv import load_dotenv  # ← AGREGAR ESTA LÍNEA

# ← AGREGAR ESTA LÍNEA
load_dotenv()  # Cargar variables del archivo .env

def create_s3_buckets():
    # Verificar que las credenciales estén cargadas
    access_key = os.getenv('AWS_ACCESS_KEY_ID')
    if not access_key:
        print("❌ Error: No se encontraron credenciales AWS")
        print("💡 Asegúrate de que el archivo .env esté en la raíz del proyecto")
        return {}
    
    print(f"🔑 Usando Access Key: {access_key[:10]}...{access_key[-4:]}")
    
    s3_client = boto3.client('s3')
    
    # Definir buckets (nombres únicos globalmente)
    bucket_base = "weather-analytics-pipeline"
    timestamp = "20250527"  # Usar timestamp para unicidad
    
    buckets = {
        'raw': f"{bucket_base}-raw-{timestamp}",
        'trusted': f"{bucket_base}-trusted-{timestamp}",
        'refined': f"{bucket_base}-refined-{timestamp}",
        'scripts': f"{bucket_base}-scripts-{timestamp}"
    }
    
    created_buckets = {}
    
    for purpose, bucket_name in buckets.items():
        try:
            print(f"⏳ Creando bucket: {bucket_name}")
            s3_client.create_bucket(Bucket=bucket_name)
            print(f"✅ Bucket creado: {bucket_name}")
            created_buckets[purpose] = bucket_name
            
            # Crear estructura de folders
            if purpose == 'raw':
                folders = ['weather-api/', 'weather-historical/', 'database/']
                for folder in folders:
                    s3_client.put_object(Bucket=bucket_name, Key=folder)
                    print(f"   📁 Folder creado: {folder}")
                    
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'BucketAlreadyExists':
                print(f"⚠️  Bucket ya existe: {bucket_name}")
                created_buckets[purpose] = bucket_name
            else:
                print(f"❌ Error creando bucket {bucket_name}: {e}")
    
    # Crear directorio config si no existe
    os.makedirs('config', exist_ok=True)
    
    # Guardar configuración
    with open('config/buckets.json', 'w') as f:
        json.dump(created_buckets, f, indent=2)
    
    print(f"💾 Configuración guardada en config/buckets.json")
    return created_buckets

if __name__ == "__main__":
    print("🚀 Configurando buckets S3...")
    buckets = create_s3_buckets()
    
    if buckets:
        print("\n🎉 Buckets configurados exitosamente:")
        for purpose, bucket_name in buckets.items():
            print(f"   {purpose}: {bucket_name}")
    else:
        print("\n❌ No se pudieron crear los buckets")