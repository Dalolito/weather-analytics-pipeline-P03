import mysql.connector
import pandas as pd
from mysql.connector import Error

def create_weather_database():
    # Configuración de conexión
    config = {
        'host': 'weather-stations-db.xxxxx.us-east-1.rds.amazonaws.com',  # Endpoint de RDS
        'user': 'admin',
        'password': 'WeatherDB2025!',
        'database': 'weather_data'
    }
    
    try:
        connection = mysql.connector.connect(**config)
        cursor = connection.cursor()
        
        # Crear base de datos
        cursor.execute("CREATE DATABASE IF NOT EXISTS weather_data")
        cursor.execute("USE weather_data")
        
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
            FOREIGN KEY (station_id) REFERENCES weather_stations(station_id)
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
            FOREIGN KEY (station_id) REFERENCES weather_stations(station_id)
        )
        """
        
        cursor.execute(create_stations_table)
        cursor.execute(create_events_table)
        cursor.execute(create_thresholds_table)
        
        print("✅ Tablas creadas exitosamente")
        
        # Insertar datos de muestra
        insert_sample_data(cursor)
        connection.commit()
        
    except Error as e:
        print(f"❌ Error: {e}")
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()

def insert_sample_data(cursor):
    # Estaciones principales de Colombia
    stations_data = [
        ('BOG001', 'Estación Bogotá Centro', 4.6097, -74.0817, 2640, 'Bogotá', 'Cundinamarca', 'Colombia', '2010-01-01', 'urban', 'active'),
        ('MED001', 'Estación Medellín Olaya Herrera', 6.2518, -75.5636, 1495, 'Medellín', 'Antioquia', 'Colombia', '2008-03-15', 'urban', 'active'),
        ('CAL001', 'Estación Cali Alfonso Bonilla', 3.4516, -76.5320, 969, 'Cali', 'Valle del Cauca', 'Colombia', '2009-06-10', 'urban', 'active'),
        ('CTG001', 'Estación Cartagena Centro', 10.3910, -75.4794, 2, 'Cartagena', 'Bolívar', 'Colombia', '2011-02-20', 'coastal', 'active'),
        ('BAQ001', 'Estación Barranquilla Metropolitana', 10.9639, -74.7964, 98, 'Barranquilla', 'Atlántico', 'Colombia', '2012-08-05', 'coastal', 'active')
    ]
    
    insert_stations = """
    INSERT INTO weather_stations 
    (station_id, station_name, latitude, longitude, elevation, city, department, country, installation_date, station_type, status)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    
    cursor.executemany(insert_stations, stations_data)
    print("✅ Datos de estaciones insertados")

if __name__ == "__main__":
    create_weather_database()