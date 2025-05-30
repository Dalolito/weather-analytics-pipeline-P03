"""
Script para diagnosticar y corregir problemas de conexión a RDS
"""
import boto3
import mysql.connector
import os
from dotenv import load_dotenv

load_dotenv()

def check_database_status():
    """Verificar estado de la base de datos RDS"""
    print("🔍 Verificando estado de RDS...")
    
    rds_client = boto3.client('rds', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    
    try:
        # Extraer nombre de la instancia desde el endpoint
        db_host = os.getenv('DB_HOST')
        if not db_host:
            print("❌ DB_HOST no configurado en .env")
            return False
        
        # El nombre de la instancia está antes del primer punto
        db_instance_id = db_host.split('.')[0]
        
        response = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_id)
        
        instance = response['DBInstances'][0]
        status = instance['DBInstanceStatus']
        
        print(f"📊 Estado de RDS: {status}")
        print(f"📍 Endpoint: {instance['Endpoint']['Address']}")
        print(f"🔌 Puerto: {instance['Endpoint']['Port']}")
        print(f"🌐 VPC: {instance.get('DBSubnetGroup', {}).get('VpcId', 'N/A')}")
        
        if status == 'available':
            print("✅ RDS está disponible")
            return True
        else:
            print(f"⚠️ RDS no está disponible (Status: {status})")
            return False
            
    except Exception as e:
        print(f"❌ Error verificando RDS: {e}")
        return False

def check_security_groups():
    """Verificar configuración de Security Groups"""
    print("\n🛡️ Verificando Security Groups...")
    
    ec2_client = boto3.client('ec2', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    rds_client = boto3.client('rds', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
    
    try:
        # Obtener security groups de RDS
        db_host = os.getenv('DB_HOST')
        db_instance_id = db_host.split('.')[0]
        
        rds_response = rds_client.describe_db_instances(DBInstanceIdentifier=db_instance_id)
        instance = rds_response['DBInstances'][0]
        
        security_groups = instance.get('VpcSecurityGroups', [])
        
        for sg in security_groups:
            sg_id = sg['VpcSecurityGroupId']
            print(f"🔒 Security Group: {sg_id}")
            
            # Obtener reglas del security group
            sg_response = ec2_client.describe_security_groups(GroupIds=[sg_id])
            sg_details = sg_response['SecurityGroups'][0]
            
            print(f"   Nombre: {sg_details.get('GroupName', 'N/A')}")
            print(f"   Descripción: {sg_details.get('Description', 'N/A')}")
            
            # Verificar reglas inbound
            inbound_rules = sg_details.get('IpPermissions', [])
            mysql_access = False
            
            for rule in inbound_rules:
                if rule.get('FromPort') == 3306 and rule.get('ToPort') == 3306:
                    mysql_access = True
                    print(f"   ✅ Puerto 3306 abierto desde: {rule.get('IpRanges', [])}")
            
            if not mysql_access:
                print(f"   ❌ Puerto 3306 (MySQL) no está abierto")
                return False
        
        return True
        
    except Exception as e:
        print(f"❌ Error verificando Security Groups: {e}")
        return False

def test_database_connection():
    """Probar conexión directa a la base de datos"""
    print("\n🔌 Probando conexión a base de datos...")
    
    db_config = {
        'host': os.getenv('DB_HOST'),
        'user': os.getenv('DB_USER', 'admin'),
        'password': os.getenv('DB_PASSWORD'),
        'database': os.getenv('DB_NAME', 'weather_data'),
        'port': int(os.getenv('DB_PORT', 3306)),
        'connect_timeout': 10
    }
    
    print(f"🎯 Conectando a: {db_config['host']}:{db_config['port']}")
    print(f"👤 Usuario: {db_config['user']}")
    print(f"🗄️ Base de datos: {db_config['database']}")
    
    try:
        connection = mysql.connector.connect(**db_config)
        
        if connection.is_connected():
            print("✅ Conexión exitosa!")
            
            cursor = connection.cursor()
            cursor.execute("SELECT VERSION()")
            version = cursor.fetchone()
            print(f"📊 Versión MySQL: {version[0]}")
            
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            print(f"📋 Tablas encontradas: {len(tables)}")
            
            cursor.close()
            connection.close()
            return True
        else:
            print("❌ Conexión falló")
            return False
            
    except mysql.connector.Error as e:
        error_code = e.errno
        error_msg = str(e)
        
        print(f"❌ Error de conexión: {error_msg}")
        
        if error_code == 2003:
            print("💡 Posibles soluciones:")
            print("   1. Verificar que RDS esté en estado 'available'")
            print("   2. Revisar Security Groups (puerto 3306)")
            print("   3. Verificar que estés en la misma VPC o que el acceso público esté habilitado")
            print("   4. Verificar configuración de red/firewall local")
        elif error_code == 1045:
            print("💡 Error de autenticación - verificar usuario/contraseña")
        elif error_code == 1049:
            print("💡 Base de datos no existe - verificar DB_NAME")
        
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False

def suggest_fixes():
    """Sugerir correcciones para problemas comunes"""
    print("\n🔧 SUGERENCIAS DE CORRECCIÓN")
    print("="*50)
    
    print("\n📋 Para habilitar acceso desde tu IP:")
    print("1. Ve a AWS Console → RDS → Databases")
    print("2. Selecciona tu instancia weather-stations-db")
    print("3. Ve a 'Connectivity & security' → Security groups")
    print("4. Click en el security group")
    print("5. Edit inbound rules → Add rule:")
    print("   - Type: MySQL/Aurora")
    print("   - Port: 3306")
    print("   - Source: My IP (tu IP actual)")
    print("6. Save rules")
    
    print("\n📋 Para habilitar acceso público (si está en VPC privada):")
    print("1. Ve a AWS Console → RDS → Databases")
    print("2. Selecciona tu instancia")
    print("3. Modify → Connectivity")
    print("4. Public access: Yes")
    print("5. Apply immediately")
    
    print("\n📋 Verificar endpoint correcto:")
    print("1. Ve a AWS Console → RDS → Databases")
    print("2. Selecciona tu instancia")
    print("3. Copia el endpoint exacto")
    print("4. Actualiza DB_HOST en tu archivo .env")

def main():
    """Función principal"""
    print("🔧 DIAGNÓSTICO DE CONEXIÓN RDS")
    print("="*50)
    
    # Verificar variables de entorno
    required_vars = ['DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print(f"❌ Variables faltantes en .env: {missing_vars}")
        return False
    
    # Ejecutar diagnósticos
    rds_ok = check_database_status()
    sg_ok = check_security_groups() if rds_ok else False
    conn_ok = test_database_connection() if rds_ok else False
    
    print("\n" + "="*50)
    print("📊 RESUMEN DEL DIAGNÓSTICO")
    print("="*50)
    
    print(f"🗄️ Estado RDS: {'✅ OK' if rds_ok else '❌ PROBLEMA'}")
    print(f"🛡️ Security Groups: {'✅ OK' if sg_ok else '❌ PROBLEMA'}")
    print(f"🔌 Conexión: {'✅ OK' if conn_ok else '❌ PROBLEMA'}")
    
    if not conn_ok:
        suggest_fixes()
    else:
        print("\n🎉 ¡Conexión a RDS funcionando correctamente!")
    
    return conn_ok

if __name__ == "__main__":
    main()