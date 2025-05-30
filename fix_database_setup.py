#!/usr/bin/env python3
"""
Script corregido para manejar nombres de BD con guiones
"""
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def fix_database_setup():
    """Corregir configuración de base de datos con sintaxis correcta"""
    print("🔧 CORRIGIENDO CONFIGURACIÓN DE BASE DE DATOS")
    print("="*50)
    
    # Configuración usando variables de entorno
    config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER', 'admin'),
        'password': os.getenv('DB_PASSWORD'),
        'port': int(os.getenv('DB_PORT', 3306))
    }
    
    # Verificar que tenemos las credenciales
    if not all([config['host'], config['password']]):
        print("❌ Faltan credenciales en .env")
        return False
    
    print(f"🎯 Conectando a: {config['host']}")
    
    try:
        # Paso 1: Conectar SIN especificar base de datos
        print("\n📝 Paso 1: Conectando a RDS...")
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        print("✅ Conexión exitosa a RDS")
        
        # Paso 2: Ver qué bases de datos existen
        print("\n📝 Paso 2: Verificando bases de datos existentes...")
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        
        print("📋 Bases de datos encontradas:")
        for db in databases:
            print(f"   • {db[0]}")
        
        # Paso 3: Crear base de datos con sintaxis correcta para nombres con guiones
        db_name = os.getenv('DB_NAME', 'weather_data')
        print(f"\n📝 Paso 3: Creando base de datos '{db_name}'...")
        
        # CORRECCIÓN: Usar backticks para nombres con guiones
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS `{db_name}`")
        print(f"✅ Base de datos '{db_name}' creada/verificada")
        
        # Paso 4: Usar la base de datos (también con backticks)
        cursor.execute(f"USE `{db_name}`")
        print(f"✅ Usando base de datos '{db_name}'")
        
        # Paso 5: Crear tablas
        print(f"\n📝 Paso 5: Creando tablas...")
        create_tables(cursor)
        
        # Paso 6: Insertar datos de muestra
        print(f"\n📝 Paso 6: Insertando datos de muestra...")
        insert_sample_data(cursor)
        
        # Confirmar cambios
        connection.commit()
        
        # Paso 7: Verificar que todo está bien
        print(f"\n📝 Paso 7: Verificando configuración...")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        print("📋 Tablas creadas:")
        for table in tables:
            print(f"   • {table[0]}")
            
            # Contar registros en cada tabla
            cursor.execute(f"SELECT COUNT(*) FROM `{table[0]}`")
            count = cursor.fetchone()[0]
            print(f"     → {count} registros")
        
        cursor.close()
        connection.close()
        
        print(f"\n🎉 ¡Base de datos configurada exitosamente!")
        print(f"💡 Nombre de BD: {db_name}")
        print(f"💡 Ahora puedes ejecutar: python tests/test_connections.py")
        
        return True
        
    except mysql.connector.Error as e:
        print(f"❌ Error de MySQL: {e}")
        print(f"💡 Código de error: {e.errno}")
        if e.errno == 1064:
            print("🔧 Problema de sintaxis SQL - verificando nombre de BD...")
            print(f"🔧 Nombre actual: '{os.getenv('DB_NAME')}'")
            print("💡 Sugerencia: Usa nombres sin guiones o caracteres especiales")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

def create_tables(cursor):
    """Crear tablas necesarias"""
    
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
    
    # Tabla de eventos climáticos
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
        FOREIGN KEY (station_id) REFERENCES weather_stations(station_id)
    )
    """
    
    # Tabla de umbrales
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
        FOREIGN KEY (station_id) REFERENCES weather_stations(station_id)
    )
    """
    
    cursor.execute(create_stations_table)
    cursor.execute(create_events_table) 
    cursor.execute(create_thresholds_table)
    
    print("✅ Tablas creadas exitosamente")

def insert_sample_data(cursor):
    """Insertar datos de muestra"""
    
    # Datos de estaciones para las 5 ciudades colombianas
    stations_data = [
        ('BOG001', 'Estación Bogotá Centro', 4.6097, -74.0817, 2640, 'Bogotá', 'Cundinamarca', 'Colombia', '2010-01-01', 'urban', 'active'),
        ('MED001', 'Estación Medellín Olaya Herrera', 6.2518, -75.5636, 1495, 'Medellín', 'Antioquia', 'Colombia', '2008-03-15', 'urban', 'active'),
        ('CAL001', 'Estación Cali Alfonso Bonilla', 3.4516, -76.5320, 969, 'Cali', 'Valle del Cauca', 'Colombia', '2009-06-10', 'urban', 'active'),
        ('CTG001', 'Estación Cartagena Centro', 10.3910, -75.4794, 2, 'Cartagena', 'Bolívar', 'Colombia', '2011-02-20', 'coastal', 'active'),
        ('BAQ001', 'Estación Barranquilla Metropolitana', 10.9639, -74.7964, 98, 'Barranquilla', 'Atlántico', 'Colombia', '2012-08-05', 'coastal', 'active')
    ]
    
    # Insertar estaciones (usar INSERT IGNORE para evitar duplicados)
    insert_stations = """
    INSERT IGNORE INTO weather_stations 
    (station_id, station_name, latitude, longitude, elevation, city, department, country, installation_date, station_type, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(insert_stations, stations_data)
    
    # Insertar algunos eventos climáticos de ejemplo
    events_data = [
        ('BOG001', 'storm', '2024-01-15', 'medium', 'Tormenta eléctrica en Bogotá centro', 'Centro de Bogotá', 50000.00, 1000),
        ('MED001', 'flood', '2024-02-20', 'high', 'Inundación por lluvias intensas', 'Comuna 1', 200000.00, 5000),
        ('CTG001', 'heatwave', '2024-03-10', 'high', 'Ola de calor en la costa caribeña', 'Cartagena centro', 0, 15000)
    ]
    
    insert_events = """
    INSERT IGNORE INTO climate_events 
    (station_id, event_type, event_date, severity, description, impact_area, economic_impact, people_affected)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(insert_events, events_data)
    
    # Insertar umbrales de alerta
    thresholds_data = [
        ('BOG001', 'temperature', -5.0, 30.0, 'warning', 1),
        ('BOG001', 'precipitation', 0.0, 50.0, 'critical', 1),
        ('MED001', 'temperature', 10.0, 35.0, 'warning', 1),
        ('CTG001', 'temperature', 20.0, 40.0, 'critical', 1)
    ]
    
    insert_thresholds = """
    INSERT IGNORE INTO weather_thresholds 
    (station_id, parameter_name, min_value, max_value, alert_level, notification_enabled)
    VALUES (%s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(insert_thresholds, thresholds_data)
    
    print("✅ Datos de muestra insertados")

if __name__ == "__main__":
    success = fix_database_setup()
    if success:
        print(f"\n🔄 Ejecutando test de conexión...")
        import subprocess
        result = subprocess.run(["python", "tests/test_connections.py"], capture_output=True, text=True)
        print(result.stdout)
        if result.stderr:
            print("Errores:", result.stderr)
    else:
        print(f"\n❌ Falló la configuración. Revisa las credenciales en .env")