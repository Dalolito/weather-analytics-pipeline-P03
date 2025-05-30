#!/usr/bin/env python3
"""
Script de verificación completa del setup inicial del proyecto
"""
import os
import sys
import json
import subprocess
import boto3
import requests
import mysql.connector
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

class ProjectSetupVerifier:
    def __init__(self):
        self.issues = []
        self.success_count = 0
        self.total_checks = 0
        
    def check(self, name, func):
        """Ejecutar una verificación y registrar resultado"""
        self.total_checks += 1
        print(f"\n🔍 Verificando: {name}")
        try:
            result = func()
            if result:
                print(f"✅ {name}: OK")
                self.success_count += 1
                return True
            else:
                print(f"❌ {name}: FALLÓ")
                self.issues.append(name)
                return False
        except Exception as e:
            print(f"❌ {name}: ERROR - {e}")
            self.issues.append(f"{name}: {str(e)}")
            return False

    def verify_environment_variables(self):
        """Verificar variables de entorno críticas"""
        required_vars = ['AWS_DEFAULT_REGION']
        optional_vars = ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 
                        'DB_HOST', 'DB_USER', 'DB_PASSWORD', 'DB_NAME']
        
        missing_required = [var for var in required_vars if not os.getenv(var)]
        if missing_required:
            print(f"   ❌ Variables requeridas faltantes: {missing_required}")
            return False
        
        missing_optional = [var for var in optional_vars if not os.getenv(var)]
        if missing_optional:
            print(f"   ⚠️ Variables opcionales faltantes: {missing_optional}")
        
        return True

    def verify_aws_connection(self):
        """Verificar conexión AWS"""
        try:
            sts = boto3.client('sts', region_name=os.getenv('AWS_DEFAULT_REGION'))
            identity = sts.get_caller_identity()
            print(f"   👤 Usuario: {identity.get('Arn', 'Unknown')}")
            return True
        except Exception as e:
            print(f"   ❌ Error AWS: {e}")
            return False

    def verify_s3_buckets(self):
        """Verificar buckets S3"""
        try:
            # Cargar configuración de buckets
            if not os.path.exists('config/buckets.json'):
                print("   ❌ config/buckets.json no existe")
                return False
            
            with open('config/buckets.json', 'r') as f:
                buckets = json.load(f)
            
            s3_client = boto3.client('s3', region_name=os.getenv('AWS_DEFAULT_REGION'))
            
            for bucket_type, bucket_name in buckets.items():
                try:
                    s3_client.head_bucket(Bucket=bucket_name)
                    print(f"   ✅ {bucket_type}: {bucket_name}")
                except Exception as e:
                    print(f"   ❌ {bucket_type}: {bucket_name} - {e}")
                    return False
            
            return True
        except Exception as e:
            print(f"   ❌ Error S3: {e}")
            return False

    def verify_openmeteo_api(self):
        """Verificar API OpenMeteo"""
        try:
            url = "https://api.open-meteo.com/v1/forecast"
            params = {
                'latitude': 4.6097,
                'longitude': -74.0817,
                'current_weather': 'true'
            }
            
            response = requests.get(url, params=params, timeout=10)
            response.raise_for_status()
            
            data = response.json()
            if 'current_weather' in data:
                print(f"   ✅ API responde correctamente")
                return True
            else:
                print(f"   ❌ Respuesta API inválida")
                return False
                
        except Exception as e:
            print(f"   ❌ Error API: {e}")
            return False

    def verify_database_connection(self):
        """Verificar conexión a base de datos"""
        db_config = {
            'host': os.getenv('DB_HOST'),
            'user': os.getenv('DB_USER', 'admin'),
            'password': os.getenv('DB_PASSWORD'),
            'database': os.getenv('DB_NAME', 'weather_data'),
            'port': int(os.getenv('DB_PORT', 3306))
        }
        
        if not all([db_config['host'], db_config['password']]):
            print("   ⚠️ Credenciales DB no configuradas - saltando")
            return True  # No fallar si no está configurado
        
        try:
            connection = mysql.connector.connect(**db_config, connect_timeout=5)
            if connection.is_connected():
                cursor = connection.cursor()
                cursor.execute("SHOW TABLES")
                tables = cursor.fetchall()
                print(f"   ✅ BD conectada, {len(tables)} tablas encontradas")
                cursor.close()
                connection.close()
                return True
            else:
                print("   ❌ No se pudo conectar a BD")
                return False
        except Exception as e:
            print(f"   ⚠️ Error BD (opcional): {e}")
            return True  # No fallar si la BD no está lista aún

    def verify_project_structure(self):
        """Verificar estructura del proyecto"""
        required_files = [
            'requirements.txt',
            'config/config.py',
            'ingesta/fetch_openmeteo_api.py',
            'ingesta/fetch_database.py',
            'spark_jobs/etl_weather_data.py',
            'tests/test_connections.py',
            'run_project.py'
        ]
        
        missing_files = []
        for file_path in required_files:
            if not os.path.exists(file_path):
                missing_files.append(file_path)
        
        if missing_files:
            print(f"   ❌ Archivos faltantes: {missing_files}")
            return False
        else:
            print(f"   ✅ Todos los archivos críticos presentes")
            return True

    def verify_dependencies(self):
        """Verificar dependencias Python"""
        try:
            import boto3
            import requests
            import pandas
            import mysql.connector
            print("   ✅ Dependencias principales instaladas")
            return True
        except ImportError as e:
            print(f"   ❌ Dependencia faltante: {e}")
            return False

    def test_data_ingestion(self):
        """Probar ingesta de datos básica"""
        try:
            # Intentar ingesta de API solo si todo lo anterior funciona
            if os.path.exists('config/buckets.json'):
                print("   🔄 Probando ingesta de API...")
                result = subprocess.run([
                    sys.executable, 'ingesta/fetch_openmeteo_api.py'
                ], capture_output=True, text=True, timeout=60)
                
                if result.returncode == 0:
                    print("   ✅ Ingesta de API funcionó")
                    return True
                else:
                    print(f"   ❌ Error en ingesta: {result.stderr}")
                    return False
            else:
                print("   ⚠️ No se puede probar ingesta sin buckets configurados")
                return True
        except Exception as e:
            print(f"   ❌ Error probando ingesta: {e}")
            return False

    def generate_report(self):
        """Generar reporte final"""
        print("\n" + "="*60)
        print("📊 REPORTE DE VERIFICACIÓN DEL PROYECTO")
        print("="*60)
        
        success_rate = (self.success_count / self.total_checks) * 100 if self.total_checks > 0 else 0
        
        print(f"\n📈 Puntuación: {self.success_count}/{self.total_checks} ({success_rate:.1f}%)")
        
        if success_rate >= 90:
            status = "🎉 EXCELENTE"
            message = "Tu proyecto está perfectamente configurado!"
        elif success_rate >= 70:
            status = "✅ BUENO"
            message = "Tu proyecto está bien configurado con algunos detalles menores."
        elif success_rate >= 50:
            status = "⚠️ ACEPTABLE"
            message = "Tu proyecto funciona pero necesita algunas correcciones."
        else:
            status = "❌ NECESITA TRABAJO"
            message = "Tu proyecto requiere correcciones importantes."
        
        print(f"\n{status}")
        print(f"💬 {message}")
        
        if self.issues:
            print(f"\n🔧 PROBLEMAS ENCONTRADOS:")
            for i, issue in enumerate(self.issues, 1):
                print(f"   {i}. {issue}")
            
            print(f"\n💡 SUGERENCIAS:")
            print("   1. Revisa el checklist de verificación")
            print("   2. Ejecuta scripts individuales para más detalles:")
            print("      - python test_aws_connection.py")
            print("      - python verify_db_credentials.py") 
            print("      - python infrastructure/setup_s3_buckets.py")
            print("   3. Verifica variables de entorno en .env")
        
        return success_rate >= 70

    def run_verification(self):
        """Ejecutar verificación completa"""
        print("🚀 VERIFICACIÓN COMPLETA DEL PROYECTO WEATHER ANALYTICS")
        print("="*60)
        print(f"⏰ Timestamp: {datetime.now().isoformat()}")
        
        # Ejecutar todas las verificaciones
        self.check("Variables de entorno", self.verify_environment_variables)
        self.check("Estructura del proyecto", self.verify_project_structure)
        self.check("Dependencias Python", self.verify_dependencies)
        self.check("Conexión AWS", self.verify_aws_connection)
        self.check("Buckets S3", self.verify_s3_buckets)
        self.check("API OpenMeteo", self.verify_openmeteo_api)
        self.check("Base de datos", self.verify_database_connection)
        self.check("Ingesta de datos", self.test_data_ingestion)
        
        # Generar reporte
        return self.generate_report()

def main():
    """Función principal"""
    verifier = ProjectSetupVerifier()
    success = verifier.run_verification()
    
    if success:
        print(f"\n🎯 PRÓXIMOS PASOS:")
        print("   1. Crear clúster EMR: python infrastructure/setup_emr_cluster.py")
        print("   2. Ejecutar pipeline: python run_project.py --step full-pipeline")
        print("   3. Configurar Athena: python applications/setup_athena.py")
        
        # Guardar resultado exitoso
        try:
            with open('verification_passed.txt', 'w') as f:
                f.write(f"Verification passed at {datetime.now().isoformat()}")
        except:
            pass
    
    return 0 if success else 1

if __name__ == "__main__":
    sys.exit(main())