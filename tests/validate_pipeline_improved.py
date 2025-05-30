"""
Script de validación mejorado del pipeline
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

class ImprovedPipelineValidator:
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

    def validate_s3_data_detailed(self):
        """Validación detallada de datos en S3"""
        logger.info("🔍 Validación detallada de S3...")
        
        if not self.buckets:
            return [{'zone': 'configuration', 'status': 'ERROR: No bucket configuration'}]
        
        validations = []
        
        zones_to_check = [
            ('raw', [
                'weather-api/',
                'database/weather_stations/',
                'database/weather_thresholds/'
            ]),
            ('trusted', ['weather_data/', 'weather_stations/']),
            ('refined', ['temperature_trends_monthly/', 'athena-results/'])
        ]
        
        for zone, prefixes in zones_to_check:
            if zone not in self.buckets:
                validations.append({
                    'zone': zone,
                    'total_files': 0,
                    'non_empty_files': 0,
                    'status': f'ERROR: {zone} bucket not configured'
                })
                continue
            
            zone_total_files = 0
            zone_non_empty_files = 0
            zone_details = []
            
            for prefix in prefixes:
                try:
                    response = self.s3_client.list_objects_v2(
                        Bucket=self.buckets[zone],
                        Prefix=prefix,
                        MaxKeys=100
                    )
                    
                    files = response.get('Contents', [])
                    non_empty_files = [f for f in files if f['Size'] > 0]
                    
                    zone_total_files += len(files)
                    zone_non_empty_files += len(non_empty_files)
                    
                    zone_details.append({
                        'prefix': prefix,
                        'total_files': len(files),
                        'non_empty_files': len(non_empty_files),
                        'total_size': sum(f['Size'] for f in files)
                    })
                    
                except Exception as e:
                    zone_details.append({
                        'prefix': prefix,
                        'error': str(e)[:100]
                    })
            
            # Determinar status de la zona
            if zone_non_empty_files > 0:
                status = 'OK'
            elif zone_total_files > 0:
                status = 'EMPTY_FILES'
            else:
                status = 'NO_FILES'
            
            validations.append({
                'zone': zone,
                'total_files': zone_total_files,
                'non_empty_files': zone_non_empty_files,
                'status': status,
                'details': zone_details
            })
        
        return validations

    def validate_data_content(self):
        """Validar contenido específico de los datos"""
        logger.info("🔍 Validando contenido de datos...")
        
        validations = []
        
        if 'raw' not in self.buckets:
            return [{'check': 'data_content', 'status': 'NO_RAW_BUCKET'}]
        
        # Verificar datos de API
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.buckets['raw'],
                Prefix='weather-api/',
                MaxKeys=10
            )
            
            api_files = [f for f in response.get('Contents', []) if f['Size'] > 0]
            
            if api_files:
                # Leer un archivo de muestra
                sample_file = api_files[0]
                obj = self.s3_client.get_object(
                    Bucket=self.buckets['raw'],
                    Key=sample_file['Key']
                )
                content = obj['Body'].read().decode('utf-8')
                data = json.loads(content)
                
                # Validar estructura
                has_city_info = 'city_info' in data
                has_daily_data = 'daily' in data
                has_timestamp = 'ingestion_timestamp' in data
                
                # Validar datos meteorológicos
                daily_data = data.get('daily', {})
                has_temperature = 'temperature_2m_max' in daily_data
                has_precipitation = 'precipitation_sum' in daily_data
                
                # Contar días de datos
                days_of_data = len(daily_data.get('time', []))
                
                validations.append({
                    'check': 'api_data_structure',
                    'files_found': len(api_files),
                    'cities_covered': len(set(f['Key'].split('/')[1] for f in api_files)),
                    'has_city_info': has_city_info,
                    'has_daily_data': has_daily_data,
                    'has_timestamp': has_timestamp,
                    'has_temperature': has_temperature,
                    'has_precipitation': has_precipitation,
                    'days_of_data': days_of_data,
                    'status': 'OK' if all([has_city_info, has_daily_data, has_timestamp, has_temperature]) else 'ISSUES'
                })
            else:
                validations.append({
                    'check': 'api_data_structure',
                    'status': 'NO_API_DATA'
                })
                
        except Exception as e:
            validations.append({
                'check': 'api_data_structure',
                'status': f'READ_ERROR: {str(e)[:50]}...'
            })
        
        # Verificar datos de BD
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.buckets['raw'],
                Prefix='database/',
                MaxKeys=10
            )
            
            db_files = [f for f in response.get('Contents', []) if f['Size'] > 0]
            
            if db_files:
                # Analizar tipos de datos de BD
                stations_files = [f for f in db_files if 'weather_stations' in f['Key']]
                events_files = [f for f in db_files if 'climate_events' in f['Key']]
                thresholds_files = [f for f in db_files if 'weather_thresholds' in f['Key']]
                
                validations.append({
                    'check': 'database_data',
                    'total_files': len(db_files),
                    'stations_files': len(stations_files),
                    'events_files': len(events_files),
                    'thresholds_files': len(thresholds_files),
                    'status': 'OK' if len(db_files) > 0 else 'NO_DATA'
                })
            else:
                validations.append({
                    'check': 'database_data',
                    'status': 'NO_DB_DATA'
                })
                
        except Exception as e:
            validations.append({
                'check': 'database_data',
                'status': f'READ_ERROR: {str(e)[:50]}...'
            })
        
        return validations

    def check_athena_readiness(self):
        """Verificar si Athena está listo"""
        logger.info("🔍 Verificando estado de Athena...")
        
        # Por ahora, solo verificar si las tablas existen sería complejo
        # En su lugar, verificamos si tenemos los datos necesarios para Athena
        validations = []
        
        if 'trusted' in self.buckets:
            try:
                response = self.s3_client.list_objects_v2(
                    Bucket=self.buckets['trusted'],
                    MaxKeys=10
                )
                
                trusted_files = response.get('Contents', [])
                
                validations.append({
                    'check': 'athena_readiness',
                    'trusted_files': len(trusted_files),
                    'status': 'READY_FOR_ETL' if len(trusted_files) > 0 else 'NEEDS_ETL',
                    'next_step': 'Configure Athena' if len(trusted_files) > 0 else 'Run ETL first'
                })
            except Exception as e:
                validations.append({
                    'check': 'athena_readiness',
                    'status': f'ERROR: {str(e)[:50]}...'
                })
        else:
            validations.append({
                'check': 'athena_readiness',
                'status': 'NO_TRUSTED_BUCKET'
            })
        
        return validations

    def generate_improved_report(self):
        """Generar reporte mejorado de validación"""
        logger.info("📋 Generando reporte mejorado...")
        
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
                's3_detailed_validation': self.validate_s3_data_detailed(),
                'data_content_validation': self.validate_data_content(),
                'athena_readiness': self.check_athena_readiness()
            })
        else:
            logger.warning("⚠️ Skipping detailed validation due to prerequisite issues")
            report.update({
                's3_detailed_validation': [],
                'data_content_validation': [],
                'athena_readiness': []
            })
        
        # Calcular puntuación mejorada
        total_checks = 0
        passed_checks = 0
        
        if not prereq_issues:
            # Contar checks de S3
            for validation in report['s3_detailed_validation']:
                total_checks += 1
                if validation['status'] == 'OK':
                    passed_checks += 1
            
            # Contar checks de contenido
            for validation in report['data_content_validation']:
                total_checks += 1
                if validation['status'] == 'OK':
                    passed_checks += 1
            
            # Contar checks de Athena readiness
            for validation in report['athena_readiness']:
                total_checks += 1
                if 'READY' in validation['status'] or validation['status'] == 'OK':
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
                    Key=f"validation_reports/improved_validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    Body=report_json,
                    ContentType='application/json'
                )
                logger.info("✅ Reporte mejorado guardado en S3")
            except Exception as e:
                logger.warning(f"⚠️ No se pudo guardar reporte en S3: {e}")
        
        return report

    def print_improved_report(self, report):
        """Mostrar reporte mejorado en consola"""
        print("\n" + "="*70)
        print("📋 REPORTE MEJORADO DE VALIDACIÓN DEL PIPELINE")
        print("="*70)
        
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
            print(f"\n📊 Puntuación Mejorada:")
            score = report['overall_score']
            print(f"   • Checks totales: {score['total_checks']}")
            print(f"   • Checks exitosos: {score['passed_checks']}")
            print(f"   • Tasa de éxito: {score['success_rate']:.1f}%")
            
            print(f"\n🗂️ Validación Detallada S3:")
            for validation in report['s3_detailed_validation']:
                status = validation['status']
                status_icon = "✅" if status == 'OK' else "⚠️" if 'EMPTY' in status else "❌"
                print(f"   {status_icon} {validation['zone']}: {validation['non_empty_files']}/{validation['total_files']} archivos con datos - {status}")
                
                # Mostrar detalles por prefix
                for detail in validation.get('details', []):
                    if 'error' not in detail:
                        print(f"      • {detail['prefix']}: {detail['non_empty_files']} archivos ({detail['total_size']} bytes)")
            
            print(f"\n📈 Validación de Contenido:")
            for validation in report['data_content_validation']:
                status = validation['status']
                status_icon = "✅" if status == 'OK' else "❌"
                print(f"   {status_icon} {validation['check']}: {status}")
                
                if validation['check'] == 'api_data_structure' and status == 'OK':
                    print(f"      • Archivos: {validation['files_found']}")
                    print(f"      • Ciudades: {validation['cities_covered']}")
                    print(f"      • Días de datos: {validation['days_of_data']}")
                
                if validation['check'] == 'database_data' and status == 'OK':
                    print(f"      • Estaciones: {validation['stations_files']} archivos")
                    print(f"      • Umbrales: {validation['thresholds_files']} archivos")
            
            print(f"\n🔍 Estado de Athena:")
            for validation in report['athena_readiness']:
                status = validation['status']
                status_icon = "✅" if 'READY' in status else "⚠️"
                print(f"   {status_icon} {validation['check']}: {status}")
                print(f"      💡 Siguiente paso: {validation['next_step']}")

            print("\n" + "="*70)
            
            if score['success_rate'] >= 80:
                print("🎉 ¡Pipeline en excelente estado!")
                print("💡 Siguiente paso: Configurar Athena o ejecutar ETL")
            elif score['success_rate'] >= 60:
                print("✅ Pipeline en buen estado con datos reales")
                print("💡 Siguiente paso: Ejecutar ETL para procesar datos")
            elif score['success_rate'] >= 40:
                print("⚠️ Pipeline funcional pero necesita procesamiento")
                print("💡 Siguiente paso: Ejecutar ingesta y ETL")
            else:
                print("❌ Pipeline requiere atención")
                print("💡 Siguiente paso: Verificar ingesta de datos")
        else:
            print("\n" + "="*70)
            print("🔧 Por favor corrige los prerequisitos antes de continuar")

def main():
    """Función principal"""
    try:
        validator = ImprovedPipelineValidator()
        report = validator.generate_improved_report()
        validator.print_improved_report(report)
        
        return report['overall_score']['success_rate'] >= 70
        
    except Exception as e:
        print(f"\n❌ Error ejecutando validación mejorada: {e}")
        return None

if __name__ == "__main__":
    main()