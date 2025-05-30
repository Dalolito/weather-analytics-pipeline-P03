"""
Script de validación completa del pipeline
"""
import boto3
import json
import time
import os
from datetime import datetime
import logging
from dotenv import load_dotenv

# Cargar variables de entorno
load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineValidator:
    def __init__(self):
        # Usar región desde variables de entorno
        aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.athena_client = boto3.client('athena', region_name=aws_region)
        
        # Cargar configuración
        try:
            with open('config/buckets.json', 'r') as f:
                self.buckets = json.load(f)
        except FileNotFoundError:
            logger.error("❌ config/buckets.json not found. Please run setup first.")
            self.buckets = {}
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid JSON in buckets.json: {e}")
            self.buckets = {}

    def validate_prerequisites(self):
        """Validar prerequisitos antes de ejecutar validación"""
        issues = []
        
        # Verificar archivo de configuración
        if not self.buckets:
            issues.append("Missing or invalid buckets.json configuration")
        
        # Verificar variables de entorno críticas
        if not os.getenv('AWS_DEFAULT_REGION'):
            issues.append("AWS_DEFAULT_REGION not set in environment")
        
        # Verificar credenciales AWS
        try:
            sts = boto3.client('sts', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
            sts.get_caller_identity()
        except Exception as e:
            issues.append(f"AWS credentials issue: {e}")
        
        return issues

    def validate_s3_data(self):
        """Validar datos en S3"""
        logger.info("🔍 Validando datos en S3...")
        
        if not self.buckets:
            return [{'zone': 'configuration', 'status': 'ERROR: No bucket configuration'}]
        
        validations = []
        
        zones = [
            ('raw', 'weather-api/'),
            ('trusted', 'weather_data/'),
            ('refined', 'temperature_trends_monthly/')
        ]
        
        for zone, prefix in zones:
            if zone not in self.buckets:
                validations.append({
                    'zone': zone,
                    'files_count': 0,
                    'status': f'ERROR: {zone} bucket not configured'
                })
                continue
            
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.buckets[zone],
                    Prefix=prefix,
                    MaxKeys=100  # Limitar para evitar timeouts
                )
                files = response.get('Contents', [])
                validations.append({
                    'zone': zone,
                    'files_count': len(files),
                    'status': 'OK' if len(files) > 0 else 'EMPTY'
                })
            except Exception as e:
                validations.append({
                    'zone': zone,
                    'files_count': 0,
                    'status': f'ERROR: {str(e)[:100]}...'
                })
        
        return validations

    def validate_athena_tables(self):
        """Validar tablas en Athena"""
        logger.info("🔍 Validando tablas en Athena...")
        
        if not self.buckets or 'refined' not in self.buckets:
            return [{'table': 'configuration', 'status': 'ERROR: No refined bucket configured'}]
        
        database_name = 'weather_analytics_db'
        tables_to_check = [
            'integrated_weather_data',
            'temperature_trends_monthly',
            'extreme_weather_events',
            'annual_weather_summary'
        ]
        
        validations = []
        
        for table in tables_to_check:
            try:
                # Contar registros en la tabla con timeout
                query = f"SELECT COUNT(*) as total FROM {database_name}.{table} LIMIT 1"
                
                response = self.athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={'Database': database_name},
                    ResultConfiguration={
                        'OutputLocation': f"s3://{self.buckets['refined']}/athena-results/"
                    }
                )
                
                query_id = response['QueryExecutionId']
                
                # Esperar resultado con timeout
                max_wait = 30  # 30 segundos máximo
                wait_time = 0
                
                while wait_time < max_wait:
                    result = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                    status = result['QueryExecution']['Status']['State']
                    
                    if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                        break
                    
                    time.sleep(2)
                    wait_time += 2
                
                if status == 'SUCCEEDED':
                    # Obtener resultado
                    results = self.athena_client.get_query_results(QueryExecutionId=query_id)
                    if len(results['ResultSet']['Rows']) > 1:
                        count = results['ResultSet']['Rows'][1]['Data'][0]['VarCharValue']
                        validations.append({
                            'table': table,
                            'record_count': int(count),
                            'status': 'OK'
                        })
                    else:
                        validations.append({
                            'table': table,
                            'record_count': 0,
                            'status': 'NO_DATA'
                        })
                elif wait_time >= max_wait:
                    validations.append({
                        'table': table,
                        'record_count': 0,
                        'status': 'TIMEOUT'
                    })
                else:
                    validations.append({
                        'table': table,
                        'record_count': 0,
                        'status': f'QUERY_FAILED: {status}'
                    })
                    
            except Exception as e:
                validations.append({
                    'table': table,
                    'record_count': 0,
                    'status': f'ERROR: {str(e)[:50]}...'
                })
        
        return validations

    def validate_basic_data_quality(self):
        """Validar calidad básica de datos (sin Athena si no está disponible)"""
        logger.info("🔍 Validando calidad básica de datos...")
        
        quality_checks = []
        
        # Check básico: verificar estructura de archivos en S3
        if 'raw' in self.buckets:
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.buckets['raw'],
                    Prefix='weather-api/',
                    MaxKeys=5
                )
                
                files = response.get('Contents', [])
                
                if files:
                    # Intentar leer un archivo JSON para verificar estructura
                    try:
                        obj = self.s3_client.get_object(
                            Bucket=self.buckets['raw'],
                            Key=files[0]['Key']
                        )
                        content = obj['Body'].read().decode('utf-8')
                        data = json.loads(content)
                        
                        # Verificar estructura básica
                        has_city_info = 'city_info' in data
                        has_daily_data = 'daily' in data
                        has_timestamp = 'ingestion_timestamp' in data
                        
                        quality_checks.append({
                            'check': 'data_structure',
                            'has_city_info': has_city_info,
                            'has_daily_data': has_daily_data,
                            'has_timestamp': has_timestamp,
                            'status': 'OK' if all([has_city_info, has_daily_data, has_timestamp]) else 'ISSUES'
                        })
                        
                    except Exception as e:
                        quality_checks.append({
                            'check': 'data_structure',
                            'status': f'READ_ERROR: {str(e)[:50]}...'
                        })
                else:
                    quality_checks.append({
                        'check': 'data_structure',
                        'status': 'NO_FILES'
                    })
                    
            except Exception as e:
                quality_checks.append({
                    'check': 'data_structure',
                    'status': f'S3_ERROR: {str(e)[:50]}...'
                })
        else:
            quality_checks.append({
                'check': 'data_structure',
                'status': 'NO_RAW_BUCKET'
            })
        
        return quality_checks

    def generate_validation_report(self):
        """Generar reporte completo de validación"""
        logger.info("📋 Generando reporte de validación...")
        
        # Verificar prerequisitos
        prereq_issues = self.validate_prerequisites()
        
        report = {
            'validation_timestamp': datetime.now().isoformat(),
            'prerequisites': {
                'status': 'OK' if not prereq_issues else 'ISSUES',
                'issues': prereq_issues
            }
        }
        
        # Solo ejecutar validaciones si los prerequisitos están OK
        if not prereq_issues:
            report.update({
                's3_validation': self.validate_s3_data(),
                'athena_validation': self.validate_athena_tables(),
                'data_quality': self.validate_basic_data_quality()
            })
        else:
            logger.warning("⚠️ Skipping detailed validation due to prerequisite issues")
            report.update({
                's3_validation': [],
                'athena_validation': [],
                'data_quality': []
            })
        
        # Calcular puntuación general
        total_checks = 0
        passed_checks = 0
        
        if not prereq_issues:
            for validation_type in ['s3_validation', 'athena_validation', 'data_quality']:
                for check in report[validation_type]:
                    total_checks += 1
                    status = str(check.get('status', ''))
                    if 'OK' in status or 'EMPTY' in status:  # EMPTY es aceptable para datos nuevos
                        passed_checks += 1
        
        report['overall_score'] = {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'success_rate': (passed_checks / total_checks) * 100 if total_checks > 0 else 0
        }
        
        # Guardar reporte si es posible
        if 'refined' in self.buckets:
            try:
                report_json = json.dumps(report, indent=2, default=str)
                self.s3_client.put_object(
                    Bucket=self.buckets['refined'],
                    Key=f"validation_reports/validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    Body=report_json,
                    ContentType='application/json'
                )
                logger.info("✅ Reporte guardado en S3")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo guardar reporte en S3: {e}")
        
        return report

def main():
    """Función principal"""
    try:
        validator = PipelineValidator()
        report = validator.generate_validation_report()
        
        print("\n" + "="*60)
        print("📋 REPORTE DE VALIDACIÓN DEL PIPELINE")
        print("="*60)
        
        print(f"\n🕐 Timestamp: {report['validation_timestamp']}")
        
        # Mostrar prerequisitos
        prereq = report['prerequisites']
        prereq_icon = "✅" if prereq['status'] == 'OK' else "❌"
        print(f"\n🔧 Prerequisitos: {prereq_icon} {prereq['status']}")
        if prereq['issues']:
            for issue in prereq['issues']:
                print(f"   ⚠️ {issue}")
        
        # Solo mostrar detalles si prerequisitos están OK
        if prereq['status'] == 'OK':
            print(f"\n📊 Puntuación General:")
            score = report['overall_score']
            print(f"   • Checks totales: {score['total_checks']}")
            print(f"   • Checks exitosos: {score['passed_checks']}")
            print(f"   • Tasa de éxito: {score['success_rate']:.1f}%")
            
            print(f"\n🗂️ Validación S3:")
            for validation in report['s3_validation']:
                status = validation['status']
                status_icon = "✅" if status == 'OK' else "⚠️" if status == 'EMPTY' else "❌"
                print(f"   {status_icon} {validation['zone']}: {validation['files_count']} archivos - {status}")
            
            print(f"\n🔍 Validación Athena:")
            for validation in report['athena_validation']:
                status = validation['status']
                status_icon = "✅" if status == 'OK' else "❌"
                print(f"   {status_icon} {validation['table']}: {validation['record_count']} registros - {status}")
            
            print(f"\n📈 Calidad de Datos:")
            for check in report['data_quality']:
                status = check['status']
                status_icon = "✅" if status == 'OK' else "❌"
                print(f"   {status_icon} {check['check']}: {status}")
                if 'has_city_info' in check:
                    print(f"      - Estructura ciudad: {'✅' if check['has_city_info'] else '❌'}")
                    print(f"      - Datos diarios: {'✅' if check['has_daily_data'] else '❌'}")
                    print(f"      - Timestamp: {'✅' if check['has_timestamp'] else '❌'}")

            print("\n" + "="*60)
            
            if score['success_rate'] >= 80:
                print("🎉 ¡Pipeline validado exitosamente!")
            elif score['success_rate'] >= 50:
                print("⚠️ Pipeline parcialmente funcional - revisa los warnings")
            else:
                print("❌ Pipeline requiere atención - revisa los errores arriba")
        else:
            print("\n" + "="*60)
            print("🔧 Por favor corrige los prerequisitos antes de continuar:")
            print("   1. Ejecuta: python infrastructure/setup_s3_buckets.py")
            print("   2. Configura variables de entorno en .env")
            print("   3. Verifica credenciales AWS")
        
        return report
        
    except Exception as e:
        print(f"\n❌ Error ejecutando validación: {e}")
        return None

if __name__ == "__main__":
    main()