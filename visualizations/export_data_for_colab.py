# export_data_for_colab.py
import boto3
import json
import pandas as pd
from datetime import datetime
import os

def get_next_colab_folder():
    """Crear la siguiente carpeta Colab con numeración automática"""
    # Asegurar que la carpeta notebook esté dentro de visualizations
    script_dir = os.path.dirname(os.path.abspath(__file__))  # Directorio donde está el script
    base_path = os.path.join(script_dir, "notebook")  # visualizations/notebook
    
    # Crear carpetas base si no existen
    os.makedirs(base_path, exist_ok=True)
    
    # Buscar carpetas existentes tipo "Colab1", "Colab2", etc.
    existing_folders = []
    if os.path.exists(base_path):
        for folder in os.listdir(base_path):
            if folder.startswith("Colab") and folder[5:].isdigit():
                existing_folders.append(int(folder[5:]))
    
    # Determinar el siguiente número
    next_number = max(existing_folders) + 1 if existing_folders else 1
    
    # Crear la nueva carpeta
    new_folder = f"Colab{next_number}"
    new_path = os.path.join(base_path, new_folder)
    os.makedirs(new_path, exist_ok=True)
    
    print(f"📁 Carpeta creada: visualizations/notebook/{new_folder}")
    return new_path

def export_weather_data_for_colab(export_folder=None):
    """Exportar datos de S3 para usar en Google Colab"""
    
    print("📊 Exportando datos para Google Colab...")
    
    # Si no se proporciona carpeta, crear una nueva (para compatibilidad)
    if export_folder is None:
        export_folder = get_next_colab_folder()
    
    # Cargar configuración (buscar config desde la raíz del proyecto)
    try:
        # Obtener directorio raíz del proyecto (un nivel arriba de visualizations)
        script_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(script_dir)
        config_path = os.path.join(project_root, 'config', 'buckets.json')
        
        with open(config_path, 'r') as f:
            buckets = json.load(f)
    except FileNotFoundError:
        print("❌ Error: config/buckets.json no encontrado")
        print("💡 Ejecuta primero: python infrastructure/setup_s3_buckets.py")
        return None
    
    s3 = boto3.client('s3')
    bucket_name = buckets['raw']
    
    print(f"🪣 Accediendo al bucket: {bucket_name}")
    
    # Obtener todos los archivos de weather-api
    try:
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix='weather-api/'
        )
    except Exception as e:
        print(f"❌ Error accediendo a S3: {e}")
        print("💡 Verifica tus credenciales AWS y permisos")
        return None
    
    all_weather_data = []
    
    if 'Contents' not in response:
        print("⚠️ No se encontraron archivos de datos meteorológicos en S3")
        print("💡 Ejecuta primero la ingesta: python ingesta/fetch_openmeteo_api.py")
        return None
    
    print(f"📁 Encontrados {len(response['Contents'])} archivos")
    
    for obj in response['Contents']:
        if obj['Key'].endswith('weather_data.json'):
            print(f"📄 Procesando: {obj['Key']}")
            
            try:
                # Descargar archivo
                file_obj = s3.get_object(Bucket=bucket_name, Key=obj['Key'])
                content = file_obj['Body'].read().decode('utf-8')
                data = json.loads(content)
                
                # Extraer datos para análisis
                city_info = data.get('city_info', {})
                current_weather = data.get('current_weather', {})
                daily_data = data.get('daily', {})
                
                city_name = city_info.get('name', 'Unknown')
                
                # Procesar datos diarios
                times = daily_data.get('time', [])
                temp_max = daily_data.get('temperature_2m_max', [])
                temp_min = daily_data.get('temperature_2m_min', [])
                precipitation = daily_data.get('precipitation_sum', [])
                humidity = daily_data.get('relative_humidity_2m_mean', [])
                wind_speed = daily_data.get('windspeed_10m_max', [])
                
                for i, date in enumerate(times):
                    if i < len(temp_max):
                        record = {
                            'city_name': city_name,
                            'latitude': city_info.get('lat'),
                            'longitude': city_info.get('lon'),
                            'date': date,
                            'temp_max': temp_max[i] if i < len(temp_max) else None,
                            'temp_min': temp_min[i] if i < len(temp_min) else None,
                            'temp_avg': (temp_max[i] + temp_min[i]) / 2 if i < len(temp_max) and i < len(temp_min) and temp_max[i] is not None and temp_min[i] is not None else None,
                            'precipitation': precipitation[i] if i < len(precipitation) else None,
                            'humidity': humidity[i] if i < len(humidity) else None,
                            'wind_speed': wind_speed[i] if i < len(wind_speed) else None,
                            'ingestion_timestamp': data.get('ingestion_timestamp'),
                            'data_source': data.get('data_source', 'openmeteo_api')
                        }
                        all_weather_data.append(record)
                        
            except Exception as e:
                print(f"⚠️ Error procesando {obj['Key']}: {e}")
                continue
    
    if not all_weather_data:
        print("❌ No se pudieron procesar datos meteorológicos")
        return None
    
    # Crear DataFrame
    df = pd.DataFrame(all_weather_data)
    
    # Limpiar datos
    df = df.dropna(subset=['city_name', 'date'])
    df['date'] = pd.to_datetime(df['date'])
    
    print(f"📊 Datos procesados: {len(df)} registros")
    print(f"🏙️ Ciudades: {df['city_name'].unique()}")
    print(f"📅 Rango fechas: {df['date'].min()} a {df['date'].max()}")
    
    # Exportar en múltiples formatos en la carpeta correcta
    csv_path = os.path.join(export_folder, 'weather_data_for_colab.csv')
    json_path = os.path.join(export_folder, 'weather_data_for_colab.json')
    metadata_path = os.path.join(export_folder, 'weather_data_metadata.json')
    
    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient='records', indent=2, date_format='iso')
    
    # Crear archivo de metadatos
    metadata = {
        'total_records': len(df),
        'cities': df['city_name'].unique().tolist(),
        'date_range': {
            'start': df['date'].min().isoformat(),
            'end': df['date'].max().isoformat()
        },
        'variables': list(df.columns),
        'export_timestamp': datetime.now().isoformat(),
        'source': 'weather_analytics_pipeline',
        'export_folder': export_folder
    }
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    print(f"\n✅ Archivos exportados en: {export_folder}")
    print(f"   📄 weather_data_for_colab.csv ({len(df)} registros)")
    print(f"   📄 weather_data_for_colab.json")
    print(f"   📄 weather_data_metadata.json")
    
    return df

def create_sample_data_if_empty(export_folder):
    """Crear datos de ejemplo si no hay datos reales en la carpeta ya creada"""
    print("🎲 Creando datos de ejemplo para demostración...")
    
    import numpy as np
    
    # Datos de ejemplo
    cities = ['Bogotá', 'Medellín', 'Cali', 'Cartagena', 'Barranquilla']
    dates = pd.date_range('2025-05-20', periods=10, freq='D')
    
    sample_data = []
    for city in cities:
        for date in dates:
            # Simular temperaturas realistas para Colombia
            base_temp = {'Bogotá': 14, 'Medellín': 22, 'Cali': 24, 'Cartagena': 28, 'Barranquilla': 29}[city]
            
            record = {
                'city_name': city,
                'latitude': {'Bogotá': 4.6097, 'Medellín': 6.2518, 'Cali': 3.4516, 'Cartagena': 10.3910, 'Barranquilla': 10.9639}[city],
                'longitude': {'Bogotá': -74.0817, 'Medellín': -75.5636, 'Cali': -76.5320, 'Cartagena': -75.4794, 'Barranquilla': -74.7964}[city],
                'date': date,
                'temp_max': base_temp + np.random.normal(8, 3),
                'temp_min': base_temp + np.random.normal(-2, 2),
                'temp_avg': base_temp + np.random.normal(3, 2),
                'precipitation': max(0, np.random.exponential(5)),
                'humidity': np.random.normal(75, 15),
                'wind_speed': max(0, np.random.normal(10, 5)),
                'ingestion_timestamp': datetime.now().isoformat(),
                'data_source': 'sample_data'
            }
            sample_data.append(record)
    
    df = pd.DataFrame(sample_data)
    
    # Exportar datos de ejemplo en la carpeta ya creada
    csv_path = os.path.join(export_folder, 'weather_data_for_colab.csv')
    json_path = os.path.join(export_folder, 'weather_data_for_colab.json')
    metadata_path = os.path.join(export_folder, 'weather_data_metadata.json')
    
    df.to_csv(csv_path, index=False)
    df.to_json(json_path, orient='records', indent=2, date_format='iso')
    
    # Crear metadatos para datos de ejemplo
    metadata = {
        'total_records': len(df),
        'cities': df['city_name'].unique().tolist(),
        'date_range': {
            'start': df['date'].min().isoformat(),
            'end': df['date'].max().isoformat()
        },
        'variables': list(df.columns),
        'export_timestamp': datetime.now().isoformat(),
        'source': 'sample_data_for_demo',
        'export_folder': export_folder,
        'note': 'Datos de ejemplo generados automáticamente para demostración'
    }
    
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2, default=str)
    
    print(f"✅ Datos de ejemplo creados en: {export_folder}")
    print(f"   📄 weather_data_for_colab.csv")
    print(f"   📄 weather_data_for_colab.json")
    print(f"   📄 weather_data_metadata.json")
    
    return df

if __name__ == "__main__":
    print("🚀 EXPORTACIÓN DE DATOS PARA GOOGLE COLAB")
    print("="*50)
    
    # Crear carpeta una sola vez al inicio
    export_folder = get_next_colab_folder()
    
    # Intentar exportar datos reales usando la carpeta creada
    df = export_weather_data_for_colab(export_folder)
    
    # Si no hay datos reales, crear datos de ejemplo en la misma carpeta
    if df is None or df.empty:
        print("\n🎲 No se encontraron datos reales, creando datos de ejemplo...")
        df = create_sample_data_if_empty(export_folder)
    
    # Estadísticas rápidas
    if df is not None and not df.empty:
        print(f"\n📈 ESTADÍSTICAS RÁPIDAS:")
        print(f"   🌡️ Temperatura promedio: {df['temp_avg'].mean():.1f}°C")
        print(f"   🔥 Temperatura máxima: {df['temp_max'].max():.1f}°C")
        print(f"   ❄️ Temperatura mínima: {df['temp_min'].min():.1f}°C")
        print(f"   🌧️ Precipitación total: {df['precipitation'].sum():.1f}mm")
        
        print(f"\n🏙️ POR CIUDAD:")
        city_stats = df.groupby('city_name').agg({
            'temp_avg': 'mean',
            'precipitation': 'sum'
        }).round(1)
        print(city_stats)
        
        folder_name = os.path.basename(export_folder)
        print(f"\n🎯 LISTO PARA COLAB:")
        print(f"   1. Ve a la carpeta: visualizations/notebook/{folder_name}")
        print(f"   2. Sube el archivo 'weather_data_for_colab.csv' a Google Colab")
        print(f"   3. Usa el código del notebook que te proporcioné")
        print(f"   4. ¡Crea visualizaciones impresionantes!")
    else:
        print("❌ No se pudieron crear los datos")