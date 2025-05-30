#!/usr/bin/env python3
"""
Script para verificar credenciales y configuración de RDS
"""
import boto3
import mysql.connector
import os
from dotenv import load_dotenv
import json

load_dotenv()

def check_environment_variables():
    """Verificar variables de entorno de la base de datos"""
    print("🔍 VERIFICANDO VARIABLES DE ENTORNO")
    print("="*50)
    
    db_vars = {
        'DB_HOST': os.getenv('DB_HOST'),
        'DB_USER': os.getenv('DB_USER'),
        'DB_PASSWORD': os.getenv('DB_PASSWORD'),
        'DB_NAME': os.getenv('DB_NAME'),
        'DB_PORT': os.getenv('DB_PORT', '3306')
    }
    
    print("📋 Variables configuradas:")
    for var, value in db_vars.items():
        if value:
            if 'PASSWORD' in var:
                # Mostrar solo primeros y últimos caracteres de la contraseña
                masked = value[:3] + '*' * (len(value) - 6) + value[-3:] if len(value) > 6 else '*' * len(value)
                print(f"✅ {var}: {masked}")
            else:
                print(f"✅ {var}: {value}")
        else:
            print(f"❌ {var}: NO CONFIGURADA")
    
    missing_vars = [var for var, value in db_vars.items() if not value]
    
    if missing_vars:
        print(f"\n❌ Variables faltantes: {missing_vars}")
        return False, db_vars
    else:
        print("\n✅ Todas las variables están configuradas")
        return True, db_vars

def get_rds_info_from_aws():
    """Obtener información real de RDS desde AWS"""
    print("\n🔍 VERIFICANDO INFORMACIÓN DE RDS EN AWS")
    print("="*50)
    
    try:
        rds_client = boto3.client('rds', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        
        # Extraer el nombre de la instancia desde el host
        db_host = os.getenv('DB_HOST')
        if not db_host:
            print("❌ DB_HOST no configurado")
            return None
        
        db_instance_id = db_host.split('.')[0]
        print(f"🎯 Buscando instancia: {db_instance_id}")
        
        response = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_id)
        instance = response['DBInstances'][0]
        
        rds_info = {
            'DBInstanceIdentifier': instance['DBInstanceIdentifier'],
            'DBInstanceStatus': instance['DBInstanceStatus'],
            'Engine': instance['Engine'],
            'EngineVersion': instance['EngineVersion'],
            'DBName': instance.get('DBName', 'N/A'),
            'MasterUsername': instance['MasterUsername'],
            'Endpoint': instance['Endpoint']['Address'],
            'Port': instance['Endpoint']['Port'],
            'PubliclyAccessible': instance['PubliclyAccessible'],
            'VpcId': instance.get('DBSubnetGroup', {}).get('VpcId', 'N/A'),
            'AvailabilityZone': instance['AvailabilityZone']
        }
        
        print("📊 Información de RDS:")
        for key, value in rds_info.items():
            icon = "✅" if key != 'DBInstanceStatus' or value == 'available' else "⚠️"
            print(f"   {icon} {key}: {value}")
        
        return rds_info
        
    except Exception as e:
        print(f"❌ Error obteniendo información de RDS: {e}")
        return None

def compare_credentials(env_vars, rds_info):
    """Comparar credenciales del .env con la información real de RDS"""
    print("\n🔍 COMPARANDO CREDENCIALES")
    print("="*50)
    
    issues = []
    
    # Verificar endpoint
    if env_vars['DB_HOST'] != rds_info['Endpoint']:
        issues.append(f"❌ Endpoint incorrecto: .env={env_vars['DB_HOST']}, RDS={rds_info['Endpoint']}")
    else:
        print(f"✅ Endpoint correcto: {env_vars['DB_HOST']}")
    
    # Verificar puerto
    env_port = str(env_vars['DB_PORT'])
    rds_port = str(rds_info['Port'])
    if env_port != rds_port:
        issues.append(f"❌ Puerto incorrecto: .env={env_port}, RDS={rds_port}")
    else:
        print(f"✅ Puerto correcto: {env_port}")
    
    # Verificar usuario maestro
    if env_vars['DB_USER'] != rds_info['MasterUsername']:
        issues.append(f"❌ Usuario incorrecto: .env={env_vars['DB_USER']}, RDS={rds_info['MasterUsername']}")
    else:
        print(f"✅ Usuario correcto: {env_vars['DB_USER']}")
    
    # Verificar nombre de base de datos
    if rds_info['DBName'] != 'N/A' and env_vars['DB_NAME'] != rds_info['DBName']:
        issues.append(f"⚠️ Nombre de DB diferente: .env={env_vars['DB_NAME']}, RDS={rds_info['DBName']}")
    else:
        print(f"✅ Nombre de DB: {env_vars['DB_NAME']}")
    
    # Verificar estado
    if rds_info['DBInstanceStatus'] != 'available':
        issues.append(f"⚠️ RDS no está disponible: Estado={rds_info['DBInstanceStatus']}")
    else:
        print(f"✅ RDS disponible: {rds_info['DBInstanceStatus']}")
    
    return issues

def test_connection_step_by_step(db_config):
    """Probar conexión paso a paso con diferentes configuraciones"""
    print("\n🔌 PROBANDO CONEXIÓN PASO A PASO")
    print("="*50)
    
    # Test 1: Conexión básica sin base de datos específica
    print("📝 Test 1: Conexión sin especificar base de datos...")
    test_config = db_config.copy()
    test_config.pop('database', None)  # Remover database específica
    
    try:
        conn = mysql.connector.connect(
            host=test_config['host'],
            user=test_config['user'],
            password=test_config['password'],
            port=int(test_config['port']),
            connect_timeout=10
        )
        print("✅ Conexión básica exitosa!")
        
        cursor = conn.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()
        print(f"   📊 Versión MySQL: {version[0]}")
        
        cursor.execute("SHOW DATABASES")
        databases = cursor.fetchall()
        print(f"   📋 Bases de datos disponibles:")
        for db in databases:
            print(f"      • {db[0]}")
        
        cursor.close()
        conn.close()
        
        return True, databases
        
    except mysql.connector.Error as e:
        print(f"❌ Error en conexión básica: {e}")
        return False, []

def test_specific_database(db_config, available_databases):
    """Probar conexión a base de datos específica"""
    print(f"\n📝 Test 2: Conexión a base de datos '{db_config['database']}'...")
    
    # Verificar si la base de datos existe
    db_exists = any(db[0] == db_config['database'] for db in available_databases)
    
    if not db_exists:
        print(f"⚠️ La base de datos '{db_config['database']}' no existe")
        print("💡 Bases de datos disponibles:")
        for db in available_databases:
            print(f"   • {db[0]}")
        
        # Intentar con una base de datos que existe
        if available_databases:
            test_db = available_databases[0][0]
            print(f"🔄 Probando con '{test_db}' en su lugar...")
            
            test_config = db_config.copy()
            test_config['database'] = test_db
            
            try:
                conn = mysql.connector.connect(**test_config, connect_timeout=10)
                print(f"✅ Conexión exitosa a '{test_db}'!")
                conn.close()
                return True
            except Exception as e:
                print(f"❌ Error conectando a '{test_db}': {e}")
                return False
    else:
        try:
            conn = mysql.connector.connect(**db_config, connect_timeout=10)
            print(f"✅ Conexión exitosa a '{db_config['database']}'!")
            
            cursor = conn.cursor()
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"   📋 Tablas en '{db_config['database']}':")
            if tables:
                for table in tables:
                    print(f"      • {table[0]}")
            else:
                print("      (No hay tablas)")
            
            cursor.close()
            conn.close()
            return True
            
        except mysql.connector.Error as e:
            print(f"❌ Error conectando a base de datos específica: {e}")
            return False

def suggest_fixes(issues, env_vars, rds_info):
    """Sugerir correcciones basadas en los problemas encontrados"""
    print("\n💡 SUGERENCIAS DE CORRECCIÓN")
    print("="*50)
    
    if not issues:
        print("🎉 ¡No se encontraron problemas de configuración!")
        return
    
    print("📋 Problemas encontrados y soluciones:")
    
    for issue in issues:
        print(f"\n{issue}")
        
        if "Endpoint incorrecto" in issue:
            print("🔧 Solución:")
            print(f"   Actualiza DB_HOST en tu .env a: {rds_info['Endpoint']}")
            
        elif "Puerto incorrecto" in issue:
            print("🔧 Solución:")
            print(f"   Actualiza DB_PORT en tu .env a: {rds_info['Port']}")
            
        elif "Usuario incorrecto" in issue:
            print("🔧 Solución:")
            print(f"   Actualiza DB_USER en tu .env a: {rds_info['MasterUsername']}")
            
        elif "no está disponible" in issue:
            print("🔧 Solución:")
            print("   Espera a que RDS esté en estado 'available'")
            print("   Puede tomar varios minutos después de crear la instancia")
    
    # Generar archivo .env corregido
    print(f"\n📝 Archivo .env sugerido:")
    print("-" * 30)
    print(f"DB_HOST={rds_info['Endpoint']}")
    print(f"DB_USER={rds_info['MasterUsername']}")
    print(f"DB_PASSWORD={env_vars['DB_PASSWORD']}")
    print(f"DB_NAME={env_vars['DB_NAME']}")
    print(f"DB_PORT={rds_info['Port']}")

def main():
    """Función principal"""
    print("🔐 VERIFICACIÓN COMPLETA DE CREDENCIALES RDS")
    print("="*60)
    
    # 1. Verificar variables de entorno
    vars_ok, env_vars = check_environment_variables()
    if not vars_ok:
        print("\n❌ Configura las variables faltantes en tu .env y vuelve a ejecutar")
        return False
    
    # 2. Obtener información real de RDS
    rds_info = get_rds_info_from_aws()
    if not rds_info:
        print("\n❌ No se pudo obtener información de RDS")
        return False
    
    # 3. Comparar credenciales
    issues = compare_credentials(env_vars, rds_info)
    
    # 4. Probar conexión paso a paso
    db_config = {
        'host': env_vars['DB_HOST'],
        'user': env_vars['DB_USER'],
        'password': env_vars['DB_PASSWORD'],
        'database': env_vars['DB_NAME'],
        'port': int(env_vars['DB_PORT'])
    }
    
    basic_conn_ok, available_dbs = test_connection_step_by_step(db_config)
    
    if basic_conn_ok:
        db_conn_ok = test_specific_database(db_config, available_dbs)
    else:
        db_conn_ok = False
    
    # 5. Sugerir correcciones
    suggest_fixes(issues, env_vars, rds_info)
    
    # Resumen final
    print("\n" + "="*60)
    print("📊 RESUMEN FINAL")
    print("="*60)
    
    print(f"🔧 Variables de entorno: {'✅ OK' if vars_ok else '❌ PROBLEMA'}")
    print(f"☁️ Información de RDS: {'✅ OK' if rds_info else '❌ PROBLEMA'}")
    print(f"🔌 Conexión básica: {'✅ OK' if basic_conn_ok else '❌ PROBLEMA'}")
    print(f"🗄️ Conexión a BD específica: {'✅ OK' if db_conn_ok else '❌ PROBLEMA'}")
    
    if basic_conn_ok and db_conn_ok:
        print("\n🎉 ¡TODAS LAS CREDENCIALES ESTÁN CORRECTAS!")
        print("💡 Tu conexión a RDS debería funcionar perfectamente")
    elif basic_conn_ok:
        print("\n⚠️ Conexión básica funciona, pero hay problema con la base de datos específica")
        print("💡 Verifica el nombre de la base de datos en DB_NAME")
    else:
        print("\n❌ Hay problemas con las credenciales o configuración")
        print("💡 Revisa las sugerencias de corrección arriba")
    
    return basic_conn_ok and db_conn_ok

if __name__ == "__main__":
    main()