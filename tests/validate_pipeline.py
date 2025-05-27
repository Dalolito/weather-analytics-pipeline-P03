"""
Script de validación completa del pipeline
"""
import boto3
import json
import time
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class PipelineValidator:
    def __init__(self):
        self.s3_client = boto3.client('s3')
        self.athena_client = boto3.client('athena')
        
        # Cargar configuración
        with open('config/buckets.json', 'r') as f:
            self.buckets = json.load(f)

    def validate_s3_data(self):
        """Validar datos en S3"""
        logger.info("🔍 Validando datos en S3...")
        
        validations = []
        
        # Verificar zona Raw
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.buckets['raw'],
                Prefix='weather-api/'
            )
            raw_files = response.get('Contents', [])
            validations.append({
                'zone': 'raw',
                'files_count': len(raw_files),
                'status': 'OK' if len(raw_files) > 0 else 'EMPTY'
            })
        except Exception as e:
            validations.append({
                'zone': 'raw',
                'files_count': 0,
                'status': f'ERROR: {e}'
            })
        
        # Verificar zona Trusted
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.buckets['trusted'],
                Prefix='weather_data/'
            )
            trusted_files = response.get('Contents', [])
            validations.append({
                'zone': 'trusted',
                'files_count': len(trusted_files),
                'status': 'OK' if len(trusted_files) > 0 else 'EMPTY'
            })
        except Exception as e:
            validations.append({
                'zone': 'trusted',
                'files_count': 0,
                'status': f'ERROR: {e}'
            })
        
        # Verificar zona Refined
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.buckets['refined'],
                Prefix='temperature_trends_monthly/'
            )
            refined_files = response.get('Contents', [])
            validations.append({
                'zone': 'refined',
                'files_count': len(refined_files),
                'status': 'OK' if len(refined_files) > 0 else 'EMPTY'
            })
        except Exception as e:
            validations.append({
                'zone': 'refined',
                'files_count': 0,
                'status': f'ERROR: {e}'
            })
        
        return validations

    def validate_athena_tables(self):
        """Validar tablas en Athena"""
        logger.info("🔍 Validando tablas en Athena...")
        
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
                # Contar registros en la tabla
                query = f"SELECT COUNT(*) as total FROM {database_name}.{table}"
                
                response = self.athena_client.start_query_execution(
                    QueryString=query,
                    QueryExecutionContext={'Database': database_name},
                    ResultConfiguration={
                        'OutputLocation': f"s3://{self.buckets['refined']}/athena-results/"
                    }
                )
                
                query_id = response['QueryExecutionId']
                
                # Esperar resultado
                while True:
                    result = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                    status = result['QueryExecution']['Status']['State']
                    
                    if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                        break
                    time.sleep(2)
                
                if status == 'SUCCEEDED':
                    # Obtener resultado
                    results = self.athena_client.get_query_results(QueryExecutionId=query_id)
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
                        'status': f'QUERY_FAILED: {status}'
                    })
                    
            except Exception as e:
                validations.append({
                    'table': table,
                    'record_count': 0,
                    'status': f'ERROR: {e}'
                })
        
        return validations

    def validate_data_quality(self):
        """Validar calidad de datos"""
        logger.info("🔍 Validando calidad de datos...")
        
        quality_checks = []
        database_name = 'weather_analytics_db'
        
        # Check 1: Verificar datos nulos en campos críticos
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(city_name) as non_null_city,
                COUNT(temp_avg) as non_null_temp,
                COUNT(date) as non_null_date
            FROM {database_name}.integrated_weather_data
            """
            
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database_name},
                ResultConfiguration={
                    'OutputLocation': f"s3://{self.buckets['refined']}/athena-results/"
                }
            )
            
            query_id = response['QueryExecutionId']
            
            # Esperar resultado
            while True:
                result = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                status = result['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(2)
            
            if status == 'SUCCEEDED':
                results = self.athena_client.get_query_results(QueryExecutionId=query_id)
                data_row = results['ResultSet']['Rows'][1]['Data']
                
                total_records = int(data_row[0]['VarCharValue'])
                non_null_city = int(data_row[1]['VarCharValue'])
                non_null_temp = int(data_row[2]['VarCharValue'])
                non_null_date = int(data_row[3]['VarCharValue'])
                
                quality_checks.append({
                    'check': 'null_values',
                    'total_records': total_records,
                    'completeness_city': (non_null_city / total_records) * 100,
                    'completeness_temp': (non_null_temp / total_records) * 100,
                    'completeness_date': (non_null_date / total_records) * 100,
                    'status': 'OK'
                })
                
        except Exception as e:
            quality_checks.append({
                'check': 'null_values',
                'status': f'ERROR: {e}'
            })
        
        # Check 2: Verificar rangos de temperatura razonables
        try:
            query = f"""
            SELECT 
                COUNT(*) as total_records,
                COUNT(CASE WHEN temp_avg BETWEEN -50 AND 60 THEN 1 END) as reasonable_temps,
                MIN(temp_avg) as min_temp,
                MAX(temp_avg) as max_temp
            FROM {database_name}.integrated_weather_data
            WHERE temp_avg IS NOT NULL
            """
            
            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database_name},
                ResultConfiguration={
                    'OutputLocation': f"s3://{self.buckets['refined']}/athena-results/"
                }
            )
            
            query_id = response['QueryExecutionId']
            
            # Esperar resultado
            while True:
                result = self.athena_client.get_query_execution(QueryExecutionId=query_id)
                status = result['QueryExecution']['Status']['State']
                
                if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                time.sleep(2)
            
            if status == 'SUCCEEDED':
                results = self.athena_client.get_query_results(QueryExecutionId=query_id)
                data_row = results['ResultSet']['Rows'][1]['Data']
                
                total_records = int(data_row[0]['VarCharValue'])
                reasonable_temps = int(data_row[1]['VarCharValue'])
                min_temp = float(data_row[2]['VarCharValue'])
                max_temp = float(data_row[3]['VarCharValue'])
                
                quality_checks.append({
                    'check': 'temperature_ranges',
                    'total_records': total_records,
                    'reasonable_percentage': (reasonable_temps / total_records) * 100,
                    'min_temperature': min_temp,
                    'max_temperature': max_temp,
                    'status': 'OK'
                })
                
        except Exception as e:
            quality_checks.append({
                'check': 'temperature_ranges',
                'status': f'ERROR: {e}'
            })
        
        return quality_checks

    def generate_validation_report(self):
        """Generar reporte completo de validación"""
        logger.info("📋 Generando reporte de validación...")
        
        report = {
            'validation_timestamp': datetime.now().isoformat(),
            's3_validation': self.validate_s3_data(),
            'athena_validation': self.validate_athena_tables(),
            'data_quality': self.validate_data_quality()
        }
        
        # Calcular puntuación general
        total_checks = 0
        passed_checks = 0
        
        for validation_type in ['s3_validation', 'athena_validation', 'data_quality']:
            for check in report[validation_type]:
                total_checks += 1
                if 'OK' in str(check.get('status', '')):
                    passed_checks += 1
        
        report['overall_score'] = {
            'total_checks': total_checks,
            'passed_checks': passed_checks,
            'success_rate': (passed_checks / total_checks) * 100 if total_checks > 0 else 0
        }
        
        # Guardar reporte
        report_json = json.dumps(report, indent=2, default=str)
        
        try:
            self.s3_client.put_object(
                Bucket=self.buckets['refined'],
                Key=f"validation_reports/validation_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                Body=report_json,
                ContentType='application/json'
            )
            logger.info("✅ Reporte guardado en S3")
        except Exception as e:
            logger.error(f"❌ Error guardando reporte: {e}")
        
        return report

def main():
    """Función principal"""
    validator = PipelineValidator()
    report = validator.generate_validation_report()
    
    print("\n" + "="*60)
    print("📋 REPORTE DE VALIDACIÓN DEL PIPELINE")
    print("="*60)
    
    print(f"\n🕐 Timestamp: {report['validation_timestamp']}")
    
    print(f"\n📊 Puntuación General:")
    score = report['overall_score']
    print(f"   • Checks totales: {score['total_checks']}")
    print(f"   • Checks exitosos: {score['passed_checks']}")
    print(f"   • Tasa de éxito: {score['success_rate']:.1f}%")
    
    print(f"\n🗂️ Validación S3:")
    for validation in report['s3_validation']:
        status_icon = "✅" if validation['status'] == 'OK' else "❌"
        print(f"   {status_icon} {validation['zone']}: {validation['files_count']} archivos - {validation['status']}")
    
    print(f"\n🔍 Validación Athena:")
    for validation in report['athena_validation']:
        status_icon = "✅" if validation['status'] == 'OK' else "❌"
        print(f"   {status_icon} {validation['table']}: {validation['record_count']} registros - {validation['status']}")
    
    print(f"\n📈 Calidad de Datos:")
    for check in report['data_quality']:
        status_icon = "✅" if check['status'] == 'OK' else "❌"
        print(f"   {status_icon} {check['check']}: {check['status']}")
        if 'completeness_temp' in check:
            print(f"      - Completitud temperatura: {check['completeness_temp']:.1f}%")
        if 'reasonable_percentage' in check:
            print(f"      - Temperaturas razonables: {check['reasonable_percentage']:.1f}%")

    print("\n" + "="*60)
    
    if score['success_rate'] >= 80:
        print("🎉 ¡Pipeline validado exitosamente!")
    else:
        print("⚠️ Pipeline requiere atención - revisa los errores arriba")
    
    return report

if __name__ == "__main__":
    main()