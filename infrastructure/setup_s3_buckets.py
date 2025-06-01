"""
Script completo para crear buckets S3 y configurar estructura local de datos
Mantiene compatibilidad con el proyecto original pero añade organización local mejorada
"""
import boto3
import json
import os
import shutil
from datetime import datetime
from botocore.exceptions import ClientError
from dotenv import load_dotenv

load_dotenv()

def create_s3_buckets():
    """Crear buckets S3 necesarios para el proyecto (función original)"""
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
    
    return created_buckets

def setup_local_structure():
    """Crear estructura de carpetas locales con organización por fecha"""
    print("\n📁 Configurando estructura local de datos...")
    
    # Configurar rutas
    scripts_folder = "scripts"
    timestamp = datetime.now().strftime('%Y%m%d')
    date_folder = f"weather-analytics-pipeline-{timestamp}"
    local_data_path = os.path.join(scripts_folder, date_folder)
    
    # Crear carpeta principal scripts si no existe
    if not os.path.exists(scripts_folder):
        os.makedirs(scripts_folder)
        print(f"✅ Carpeta '{scripts_folder}' creada")
    
    # Crear subcarpeta con fecha
    if not os.path.exists(local_data_path):
        os.makedirs(local_data_path)
        print(f"✅ Carpeta de fecha '{local_data_path}' creada")
    
    # Crear subcarpetas para cada tipo de bucket S3
    bucket_folders = {
        'raw': [
            'weather-api',
            'weather-historical', 
            'database',
            'logs'
        ],
        'trusted': [
            'weather_data',
            'weather_stations',
            'climate_events',
            'integrated_weather_data'
        ],
        'refined': [
            'temperature_trends_monthly',
            'extreme_weather_events',
            'annual_weather_summary',
            'ml_model_metrics',
            'temperature_predictions',
            'weather_forecasts',
            'athena-results',
            'validation_reports'
        ],
        'scripts': [
            'spark-jobs',
            'bootstrap',
            'logs'
        ]
    }
    
    for bucket_type, subfolders in bucket_folders.items():
        bucket_path = os.path.join(local_data_path, bucket_type)
        if not os.path.exists(bucket_path):
            os.makedirs(bucket_path)
            print(f"   📂 Carpeta '{bucket_type}' creada")
            
            # Crear subcarpetas específicas
            for subfolder in subfolders:
                subfolder_path = os.path.join(bucket_path, subfolder)
                if not os.path.exists(subfolder_path):
                    os.makedirs(subfolder_path)
                    print(f"      📄 Subcarpeta '{subfolder}' creada")
    
    # Crear archivo de metadatos
    metadata = {
        "creation_date": datetime.now().strftime('%Y-%m-%d'),
        "timestamp": timestamp,
        "project": "weather-analytics-pipeline",
        "description": "Datos de ingesta y procesamiento para análisis meteorológico",
        "bucket_types": list(bucket_folders.keys()),
        "s3_buckets": {},
        "local_path": local_data_path,
        "structure": bucket_folders
    }
    
    metadata_path = os.path.join(local_data_path, 'metadata.json')
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"✅ Metadatos guardados en {metadata_path}")
    
    # Crear archivo README con instrucciones
    readme_content = f"""# Weather Analytics Pipeline - {timestamp}

## Estructura de Datos

Esta carpeta contiene la estructura local de datos para el proyecto Weather Analytics Pipeline.

### Fecha de creación: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

### Estructura de carpetas:

```
{date_folder}/
├── raw/                          # Datos crudos
│   ├── weather-api/             # Datos de API OpenMeteo
│   ├── weather-historical/      # Datos históricos
│   ├── database/               # Datos de base de datos
│   └── logs/                   # Logs de ingesta
├── trusted/                     # Datos procesados y limpios
│   ├── weather_data/           # Datos meteorológicos procesados
│   ├── weather_stations/       # Estaciones meteorológicas
│   ├── climate_events/         # Eventos climáticos
│   └── integrated_weather_data/ # Datos integrados
├── refined/                     # Datos analíticos y resultados
│   ├── temperature_trends_monthly/
│   ├── extreme_weather_events/
│   ├── annual_weather_summary/
│   ├── ml_model_metrics/
│   ├── temperature_predictions/
│   ├── weather_forecasts/
│   ├── athena-results/
│   └── validation_reports/
├── scripts/                     # Scripts y configuraciones
│   ├── spark-jobs/             # Jobs de Spark
│   ├── bootstrap/              # Scripts de bootstrap
│   └── logs/                   # Logs de procesamiento
└── metadata.json               # Metadatos del proyecto
```

### Uso:

1. **Ingesta de datos**: Los scripts de ingesta guardarán automáticamente los datos tanto en S3 como en las carpetas locales correspondientes.

2. **Procesamiento**: Los datos procesados se almacenarán en las carpetas `trusted/` y `refined/`.

3. **Logs**: Todos los logs se guardan en las carpetas `logs/` respectivas.

### Sincronización con S3:

- Los datos se almacenan simultáneamente en S3 y localmente
- Los buckets S3 están configurados en `config/buckets.json`
- La sincronización es automática durante los procesos de ingesta

### Archivos importantes:

- `metadata.json`: Contiene información del proyecto y configuración
- `config/buckets.json`: Configuración de buckets S3 (en directorio raíz)

Para más información, consulta la documentación del proyecto.
"""
    
    readme_path = os.path.join(local_data_path, 'README.md')
    with open(readme_path, 'w', encoding='utf-8') as f:
        f.write(readme_content)
    
    print(f"✅ README creado en {readme_path}")
    
    return local_data_path, metadata

def update_metadata_with_buckets(local_data_path, bucket_config):
    """Actualizar archivo de metadatos con información de buckets S3"""
    metadata_path = os.path.join(local_data_path, 'metadata.json')
    
    try:
        with open(metadata_path, 'r') as f:
            metadata = json.load(f)
        
        metadata['s3_buckets'] = bucket_config
        metadata['setup_completed'] = datetime.now().isoformat()
        
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=2)
            
        print(f"✅ Metadatos actualizados con información de S3")
        
    except Exception as e:
        print(f"⚠️ Error actualizando metadatos: {e}")

def verify_buckets(bucket_config):
    """Verificar que los buckets estén accesibles (función original)"""
    print("\n🔍 Verificando acceso a buckets...")
    
    s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    
    all_accessible = True
    for bucket_type, bucket_name in bucket_config.items():
        try:
            # Intentar listar objetos
            response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            print(f"✅ {bucket_type}: {bucket_name} - Accesible")
        except Exception as e:
            print(f"❌ {bucket_type}: {bucket_name} - Error: {e}")
            all_accessible = False
    
    return all_accessible

def main():
    """Función principal mejorada que mantiene compatibilidad"""
    print("🚀 CONFIGURACIÓN COMPLETA DE BUCKETS S3 Y ESTRUCTURA LOCAL")
    print("="*70)
    
    try:
        # Verificar credenciales AWS
        sts = boto3.client('sts', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        identity = sts.get_caller_identity()
        print(f"🔑 Conectado como: {identity.get('Arn', 'Unknown')}")
        
        # Paso 1: Configurar estructura local
        print(f"\n📂 Paso 1: Configurando estructura local...")
        local_data_path, metadata = setup_local_structure()
        
        # Paso 2: Crear buckets S3 (función original)
        print(f"\n☁️ Paso 2: Creando buckets S3...")
        created_buckets = create_s3_buckets()
        
        # Paso 3: Verificar acceso (función original)
        print(f"\n🔍 Paso 3: Verificando acceso...")
        buckets_ok = verify_buckets(created_buckets)
        
        # Paso 4: Guardar configuración
        print(f"\n💾 Paso 4: Guardando configuración...")
        
        # Guardar configuración de buckets (mantiene compatibilidad)
        os.makedirs('config', exist_ok=True)
        with open('config/buckets.json', 'w') as f:
            json.dump(created_buckets, f, indent=2)
        
        print(f"✅ Configuración guardada en config/buckets.json")
        
        # Actualizar metadatos locales
        update_metadata_with_buckets(local_data_path, created_buckets)
        
        # Mostrar resumen
        if buckets_ok:
            print("\n🎉 ¡Configuración completada exitosamente!")
            print("\n" + "="*50)
            print("📊 RESUMEN DE CONFIGURACIÓN")
            print("="*50)
            
            print(f"\n☁️ Buckets S3 creados:")
            for bucket_type, bucket_name in created_buckets.items():
                print(f"   • {bucket_type}: {bucket_name}")
            
            print(f"\n📁 Estructura local creada:")
            print(f"   📍 Ubicación: {local_data_path}")
            print(f"   📂 Carpetas: raw, trusted, refined, scripts")
            print(f"   📄 Archivos: metadata.json, README.md")
            
            print(f"\n⚙️ Archivos de configuración:")
            print(f"   • config/buckets.json (compatibilidad con proyecto original)")
            print(f"   • {local_data_path}/metadata.json (información extendida)")
            
            print(f"\n🎯 Próximos pasos:")
            print(f"   1. Ejecutar ingesta: python ingesta/fetch_openmeteo_api.py")
            print(f"   2. Los datos se guardarán automáticamente en:")
            print(f"      - S3: buckets configurados")
            print(f"      - Local: {local_data_path}")
            print(f"   3. Procesar datos: python run_project.py --step ingest-data")
            
        else:
            print("\n⚠️ Configuración completada con algunos problemas de acceso a S3")
            print("💡 Los datos se pueden seguir guardando localmente")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error en configuración: {e}")
        print("💡 Asegúrate de que:")
        print("   1. AWS_DEFAULT_REGION esté configurado en .env")
        print("   2. Tengas credenciales AWS válidas")
        print("   3. Tengas permisos para crear buckets S3")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)