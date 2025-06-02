import mysql.connector
import pandas as pd
from mysql.connector import Error
import os
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

def create_weather_database():
    """Crear base de datos con manejo mejorado de errores"""
    # Configuración de conexión desde variables de entorno
    config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER', 'admin'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME', 'weather_data'),
        'port': int(os.getenv('DB_PORT', 3306))
    }
    
    # Verificar que las credenciales están configuradas
    print("🔍 Verificando configuración de base de datos...")
    print(f"📍 Host: {config['host']}")
    print(f"👤 Usuario: {config['user']}")
    print(f"🗄️ Base de datos: {config['database']}")
    print(f"🔌 Puerto: {config['port']}")
    print(f"🔐 Password: {'✅ Configurado' if config['password'] else '❌ NO configurado'}")
    
    # Advertencia sobre nombres con guiones
    if '-' in config['database']:
        print(f"⚠️  NOTA: El nombre de BD contiene guiones, usando escape automático")
    
    if not config['host'] or not config['password']:
        print("\n❌ ERROR: Credenciales de base de datos no configuradas")
        print("💡 Asegúrate de tener en tu archivo .env:")
        print("   DB_HOST=tu-rds-endpoint.amazonaws.com")
        print("   DB_USER=admin")
        print("   DB_PASSWORD=tu-password")
        print("   DB_NAME=weather_data")
        print("   DB_PORT=3306")
        return False
    
    connection = None
    try:
        print(f"\n🔄 Intentando conectar a {config['host']}...")
        
        # Intentar conexión
        connection = mysql.connector.connect(**config)
        
        if connection.is_connected():
            print("✅ Conexión exitosa a la base de datos")
            cursor = connection.cursor()
            
            # Crear base de datos
            print("🗄️ Creando base de datos...")
            db_name = config['database']
            escaped_db_name = f"`{db_name}`" if '-' in db_name else db_name
            cursor.execute(f"USE {escaped_db_name}")
            
            # Crear tablas
            create_tables(cursor)
            
            # Insertar datos de muestra
            insert_sample_data(cursor)
            
            # Confirmar cambios
            connection.commit()
            print("✅ Base de datos configurada exitosamente")
            return True
            
    except Error as e:
        print(f"\n❌ Error de MySQL: {e}")
        
        # Diagnóstico específico de errores
        if "Access denied" in str(e):
            print("🔧 SOLUCIONES POSIBLES:")
            print("   1. Verificar usuario y contraseña en .env")
            print("   2. Verificar que el usuario tenga permisos")
            print("   3. Verificar que la instancia RDS esté activa")
            print("   4. Verificar security groups (puerto 3306 abierto)")
            
        elif "Can't connect" in str(e):
            print("🔧 SOLUCIONES POSIBLES:")
            print("   1. Verificar que la instancia RDS esté ejecutándose")
            print("   2. Verificar el endpoint de RDS")
            print("   3. Verificar conectividad de red")
            
        elif "Unknown database" in str(e):
            print("🔧 SOLUCIÓN:")
            print("   La base de datos se creará automáticamente")
            
        return False
        
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        return False
        
    finally:
        # Cerrar conexión de forma segura
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("🔌 Conexión cerrada")

def create_tables(cursor):
    """Crear todas las tablas necesarias"""
    print("📋 Creando tablas...")
    
    # Tabla de estaciones meteorológicas
    create_stations_table = """
    CREATE TABLE IF NOT EXISTS weather_stations (
        station_id VARCHAR(50) PRIMARY KEY,
        station_name VARCHAR(200) NOT NULL,
        latitude DECIMAL(10, 6) NOT NULL,
        longitude DECIMAL(10, 6) NOT NULL,
        elevation INT,
        city VARCHAR(100),
        department VARCHAR(100),
        country VARCHAR(50) DEFAULT 'Colombia',
        installation_date DATE,
        station_type VARCHAR(50),
        status ENUM('active', 'inactive', 'maintenance') DEFAULT 'active',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    """
    
    # Tabla de eventos climáticos históricos
    create_events_table = """
    CREATE TABLE IF NOT EXISTS climate_events (
        event_id INT AUTO_INCREMENT PRIMARY KEY,
        station_id VARCHAR(50),
        event_type ENUM('storm', 'drought', 'flood', 'heatwave', 'coldwave') NOT NULL,
        event_date DATE NOT NULL,
        severity ENUM('low', 'medium', 'high', 'extreme') NOT NULL,
        description TEXT,
        impact_area VARCHAR(200),
        economic_impact DECIMAL(15, 2),
        people_affected INT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (station_id) REFERENCES weather_stations(station_id) ON DELETE SET NULL
    )
    """
    
    # Tabla de umbrales y alertas
    create_thresholds_table = """
    CREATE TABLE IF NOT EXISTS weather_thresholds (
        threshold_id INT AUTO_INCREMENT PRIMARY KEY,
        station_id VARCHAR(50),
        parameter_name VARCHAR(100) NOT NULL,
        min_value DECIMAL(10, 2),
        max_value DECIMAL(10, 2),
        alert_level ENUM('info', 'warning', 'critical') NOT NULL,
        notification_enabled BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (station_id) REFERENCES weather_stations(station_id) ON DELETE SET NULL
    )
    """
    
    try:
        cursor.execute(create_stations_table)
        print("   ✅ Tabla weather_stations creada")
        
        cursor.execute(create_events_table)
        print("   ✅ Tabla climate_events creada")
        
        cursor.execute(create_thresholds_table)
        print("   ✅ Tabla weather_thresholds creada")
        
    except Error as e:
        print(f"   ❌ Error creando tablas: {e}")
        raise e

def insert_sample_data(cursor):
    """Insertar datos de ejemplo"""
    print("📊 Insertando datos de ejemplo...")
    
    # Estaciones principales de Colombia
    stations_data = [
        ('BOG001', 'Estación Bogotá Centro', 4.6097, -74.0817, 2640, 'Bogotá', 'Cundinamarca', 'Colombia', '2010-01-01', 'urban', 'active'),
        ('MED001', 'Estación Medellín Olaya Herrera', 6.2518, -75.5636, 1495, 'Medellín', 'Antioquia', 'Colombia', '2008-03-15', 'urban', 'active'),
        ('CAL001', 'Estación Cali Alfonso Bonilla', 3.4516, -76.5320, 969, 'Cali', 'Valle del Cauca', 'Colombia', '2009-06-10', 'urban', 'active'),
        ('CTG001', 'Estación Cartagena Centro', 10.3910, -75.4794, 2, 'Cartagena', 'Bolívar', 'Colombia', '2011-02-20', 'coastal', 'active'),
        ('BAQ001', 'Estación Barranquilla Metropolitana', 10.9639, -74.7964, 98, 'Barranquilla', 'Atlántico', 'Colombia', '2012-08-05', 'coastal', 'active')
    ]
    
    insert_stations = """
    INSERT IGNORE INTO weather_stations 
    (station_id, station_name, latitude, longitude, elevation, city, department, country, installation_date, station_type, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    try:
        cursor.executemany(insert_stations, stations_data)
        print(f"   ✅ {len(stations_data)} estaciones insertadas")
        
        # Insertar algunos eventos de ejemplo
        events_data = [
            ('BOG001', 'storm', '2024-01-15', 'medium', 'Tormenta fuerte en Bogotá', 'Zona metropolitana', 1000000.00, 50000),
            ('MED001', 'flood', '2024-02-20', 'high', 'Inundaciones en Medellín', 'Valle de Aburrá', 2500000.00, 30000),
            ('CTG001', 'heatwave', '2024-03-10', 'extreme', 'Ola de calor en la costa', 'Región Caribe', 500000.00, 100000)
        ]
        
        insert_events = """
        INSERT IGNORE INTO climate_events 
        (station_id, event_type, event_date, severity, description, impact_area, economic_impact, people_affected)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_events, events_data)
        print(f"   ✅ {len(events_data)} eventos climáticos insertados")
        
        # Insertar umbrales de ejemplo
        thresholds_data = [
            ('BOG001', 'temperature_max', None, 25.0, 'warning'),
            ('BOG001', 'temperature_min', 5.0, None, 'critical'),
            ('CTG001', 'temperature_max', None, 35.0, 'warning'),
            ('MED001', 'precipitation', None, 50.0, 'warning')
        ]
        
        insert_thresholds = """
        INSERT IGNORE INTO weather_thresholds 
        (station_id, parameter_name, min_value, max_value, alert_level)
        VALUES (%s, %s, %s, %s, %s)
        """
        
        cursor.executemany(insert_thresholds, thresholds_data)
        print(f"   ✅ {len(thresholds_data)} umbrales insertados")
        
    except Error as e:
        print(f"   ❌ Error insertando datos: {e}")
        raise e

def test_database_connection():
    """Probar conexión sin crear nada"""
    config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER', 'admin'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', 3306))
    }
    
    # No incluir 'database' para probar conexión básica
    try:
        print("🔍 Probando conexión básica...")
        connection = mysql.connector.connect(
            host=config['host'],
            user=config['user'],
            password=config['password'],
            port=config['port']
        )
        
        if connection.is_connected():
            print("✅ Conexión básica exitosa")
            cursor = connection.cursor()
            cursor.execute("SHOW DATABASES")
            databases = cursor.fetchall()
            print(f"📋 Bases de datos disponibles: {[db[0] for db in databases]}")
            connection.close()
            return True
            
    except Error as e:
        print(f"❌ Error en conexión básica: {e}")
        return False

if __name__ == "__main__":
    print("🗄️ CONFIGURACIÓN DE BASE DE DATOS MYSQL")
    print("="*50)
    
    # Primero probar conexión básica
    if test_database_connection():
        print("\n🚀 Procediendo con la configuración completa...")
        success = create_weather_database()
        
        if success:
            print("\n🎉 ¡Base de datos configurada exitosamente!")
            print("✅ Tablas creadas:")
            print("   • weather_stations")
            print("   • climate_events") 
            print("   • weather_thresholds")
            print("✅ Datos de ejemplo insertados")
        else:
            print("\n❌ Error en la configuración")
    else:
        print("\n❌ No se puede proceder - problema de conexión básica")
        print("💡 Verifica tus credenciales y configuración de red")