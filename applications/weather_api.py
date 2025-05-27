import json
import boto3
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Cliente Athena
athena_client = boto3.client('athena')

def lambda_handler(event, context):
    """Función Lambda para API Gateway"""
    
    try:
        # Obtener parámetros de la consulta
        http_method = event.get('httpMethod', 'GET')
        path = event.get('path', '/')
        query_params = event.get('queryStringParameters') or {}
        
        # Enrutar según el path
        if path == '/weather/current':
            return get_current_weather(query_params)
        elif path == '/weather/trends':
            return get_weather_trends(query_params)
        elif path == '/weather/extreme-events':
            return get_extreme_events(query_params)
        elif path == '/weather/summary':
            return get_weather_summary(query_params)
        else:
            return create_response(404, {'error': 'Endpoint no encontrado'})
            
    except Exception as e:
        logger.error(f"Error en lambda_handler: {str(e)}")
        return create_response(500, {'error': 'Error interno del servidor'})

def execute_athena_query(query, database='weather_analytics_db'):
    """Ejecutar consulta en Athena"""
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database},
            ResultConfiguration={
                'OutputLocation': 's3://weather-analytics-pipeline-refined-20250527/athena-results/'
            }
        )
        
        query_id = response['QueryExecutionId']
        
        # Esperar a que complete la consulta
        while True:
            result = athena_client.get_query_execution(QueryExecutionId=query_id)
            status = result['QueryExecution']['Status']['State']
            
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
            
        if status == 'SUCCEEDED':
            # Obtener resultados
            results = athena_client.get_query_results(QueryExecutionId=query_id)
            return parse_query_results(results)
        else:
            raise Exception(f"Query falló con status: {status}")
            
    except Exception as e:
        logger.error(f"Error ejecutando consulta Athena: {str(e)}")
        raise e

def parse_query_results(results):
    """Parsear resultados de Athena"""
    rows = results['ResultSet']['Rows']
    
    if len(rows) == 0:
        return []
    
    # Primera fila contiene los headers
    headers = [col['VarCharValue'] for col in rows[0]['Data']]
    
    # Parsear datos
    data = []
    for row in rows[1:]:
        row_data = {}
        for i, col in enumerate(row['Data']):
            value = col.get('VarCharValue')
            row_data[headers[i]] = value
        data.append(row_data)
    
    return data

def get_current_weather(params):
    """Obtener datos meteorológicos actuales"""
    city = params.get('city', 'all')
    limit = int(params.get('limit', 10))
    
    if city == 'all':
        query = f"""
        SELECT city_name, date, temp_avg, temp_max, temp_min, precipitation, humidity
        FROM integrated_weather_data 
        WHERE date >= current_date - interval '7' day
        ORDER BY date DESC 
        LIMIT {limit}
        """
    else:
        query = f"""
        SELECT city_name, date, temp_avg, temp_max, temp_min, precipitation, humidity
        FROM integrated_weather_data 
        WHERE city_name = '{city}' AND date >= current_date - interval '7' day
        ORDER BY date DESC 
        LIMIT {limit}
        """
    
    try:
        results = execute_athena_query(query)
        return create_response(200, {
            'data': results,
            'total_records': len(results),
            'query_params': params
        })
    except Exception as e:
        return create_response(500, {'error': str(e)})

def get_weather_trends(params):
    """Obtener tendencias meteorológicas"""
    city = params.get('city')
    year = params.get('year', '2024')
    
    if city:
        query = f"""
        SELECT city_name, year, month, avg_temperature, max_temperature, 
               min_temperature, avg_precipitation, avg_humidity
        FROM temperature_trends_monthly 
        WHERE city_name = '{city}' AND year = {year}
        ORDER BY month
        """
    else:
        query = f"""
        SELECT city_name, year, month, avg_temperature, max_temperature, 
               min_temperature, avg_precipitation, avg_humidity
        FROM temperature_trends_monthly 
        WHERE year = {year}
        ORDER BY city_name, month
        LIMIT 100
        """
    
    try:
        results = execute_athena_query(query)
        return create_response(200, {
            'data': results,
            'total_records': len(results),
            'query_params': params
        })
    except Exception as e:
        return create_response(500, {'error': str(e)})

def get_extreme_events(params):
    """Obtener eventos meteorológicos extremos"""
    event_type = params.get('event_type', 'all')
    year = params.get('year', '2024')
    
    if event_type == 'all':
        query = f"""
        SELECT city_name, event_type, year, event_count, 
               avg_temp_during_event, max_precipitation, max_wind_speed
        FROM extreme_weather_events 
        WHERE year = {year}
        ORDER BY event_count DESC
        LIMIT 50
        """
    else:
        query = f"""
        SELECT city_name, event_type, year, event_count, 
               avg_temp_during_event, max_precipitation, max_wind_speed
        FROM extreme_weather_events 
        WHERE event_type = '{event_type}' AND year = {year}
        ORDER BY event_count DESC
        LIMIT 50
        """
    
    try:
        results = execute_athena_query(query)
        return create_response(200, {
            'data': results,
            'total_records': len(results),
            'query_params': params
        })
    except Exception as e:
        return create_response(500, {'error': str(e)})

def get_weather_summary(params):
    """Obtener resumen meteorológico anual"""
    year = params.get('year', '2024')
    climate_category = params.get('climate_category', 'all')
    
    if climate_category == 'all':
        query = f"""
        SELECT city_name, year, annual_avg_temp, annual_max_temp, annual_min_temp,
               annual_precipitation, annual_avg_humidity, climate_category
        FROM annual_weather_summary 
        WHERE year = {year}
        ORDER BY annual_avg_temp DESC
        LIMIT 20
        """
    else:
        query = f"""
        SELECT city_name, year, annual_avg_temp, annual_max_temp, annual_min_temp,
               annual_precipitation, annual_avg_humidity, climate_category
        FROM annual_weather_summary 
        WHERE year = {year} AND climate_category = '{climate_category}'
        ORDER BY annual_avg_temp DESC
        """
    
    try:
        results = execute_athena_query(query)
        return create_response(200, {
            'data': results,
            'total_records': len(results),
            'query_params': params
        })
    except Exception as e:
        return create_response(500, {'error': str(e)})

def create_response(status_code, body):
    """Crear respuesta HTTP"""
    return {
        'statusCode': status_code,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
        'body': json.dumps(body, ensure_ascii=False, default=str)
    }