#!/usr/bin/env python3
"""
Script principal para ejecutar el proyecto Weather Analytics Big Data
"""

import os
import sys
import json
import time
import argparse
from datetime import datetime

def main():
    parser = argparse.ArgumentParser(description='Weather Analytics Big Data Project')
    parser.add_argument('--step', choices=[
        'setup-infra', 'create-database', 'ingest-data', 
        'create-emr', 'run-etl', 'run-analytics', 'run-ml',
        'setup-athena', 'deploy-api', 'full-pipeline'
    ], required=True, help='Paso a ejecutar')
    
    parser.add_argument('--cluster-id', help='ID del clúster EMR (para steps de Spark)')
    
    args = parser.parse_args()
    
    print(f"🚀 Ejecutando paso: {args.step}")
    print(f"⏰ Timestamp: {datetime.now().isoformat()}")
    
    if args.step == 'setup-infra':
        setup_infrastructure()
    elif args.step == 'create-database':
        create_database()
    elif args.step == 'ingest-data':
        run_data_ingestion()
    elif args.step == 'create-emr':
        create_emr_cluster()
    elif args.step == 'run-etl':
        run_spark_etl(args.cluster_id)
    elif args.step == 'run-analytics':
        run_spark_analytics(args.cluster_id)
    elif args.step == 'run-ml':
        run_spark_ml(args.cluster_id)
    elif args.step == 'setup-athena':
        setup_athena()
    elif args.step == 'deploy-api':
        deploy_api()
    elif args.step == 'full-pipeline':
        run_full_pipeline(args.cluster_id)

def setup_infrastructure():
    """Configurar infraestructura AWS"""
    print("🏗️ Configurando infraestructura...")
    os.system("python infrastructure/setup_s3_buckets.py")
    print("✅ Infraestructura configurada")

def create_database():
    """Crear y configurar base de datos"""
    print("🗄️ Configurando base de datos...")
    os.system("python infrastructure/setup_database.py")
    print("✅ Base de datos configurada")

def run_data_ingestion():
    """Ejecutar ingesta de datos"""
    print("📥 Ejecutando ingesta de datos...")
    
    # Ingesta desde API
    print("  📡 Ingesta desde API...")
    os.system("python ingesta/fetch_openmeteo_api.py")
    
    # Ingesta desde BD
    print("  🗄️ Ingesta desde base de datos...")
    os.system("python ingesta/fetch_database.py")
    
    print("✅ Ingesta completada")

def create_emr_cluster():
    """Crear clúster EMR"""
    print("🚀 Creando clúster EMR...")
    os.system("python infrastructure/setup_emr_cluster.py")
    print("✅ Clúster EMR creado")

def run_spark_etl(cluster_id):
    """Ejecutar ETL con Spark"""
    if not cluster_id:
        print("❌ Se requiere cluster-id para ejecutar ETL")
        sys.exit(1)
    
    print("⚙️ Ejecutando ETL con Spark...")
    from automation.orchestrator import WeatherAnalyticsOrchestrator
    
    orchestrator = WeatherAnalyticsOrchestrator()
    
    # Cargar buckets
    with open('config/buckets.json', 'r') as f:
        buckets = json.load(f)
    
    step_id = orchestrator.submit_emr_step(
        cluster_id,
        "ETL Weather Data",
        "etl_weather_data.py",
        [buckets['raw'], buckets['trusted']]
    )
    
    status = orchestrator.wait_for_step_completion(cluster_id, step_id)
    print(f"✅ ETL completado con status: {status}")

def run_spark_analytics(cluster_id):
    """Ejecutar análisis con Spark"""
    if not cluster_id:
        print("❌ Se requiere cluster-id para ejecutar analytics")
        sys.exit(1)
    
    print("📊 Ejecutando análisis con Spark...")
    from automation.orchestrator import WeatherAnalyticsOrchestrator
    
    orchestrator = WeatherAnalyticsOrchestrator()
    
    # Cargar buckets
    with open('config/buckets.json', 'r') as f:
        buckets = json.load(f)
    
    step_id = orchestrator.submit_emr_step(
        cluster_id,
        "Analytics Weather Trends",
        "analytics_weather_trends.py",
        [buckets['trusted'], buckets['refined']]
    )
    
    status = orchestrator.wait_for_step_completion(cluster_id, step_id)
    print(f"✅ Analytics completado con status: {status}")

def run_spark_ml(cluster_id):
    """Ejecutar ML con Spark"""
    if not cluster_id:
        print("❌ Se requiere cluster-id para ejecutar ML")
        sys.exit(1)
    
    print("🤖 Ejecutando ML con Spark...")
    from automation.orchestrator import WeatherAnalyticsOrchestrator
    
    orchestrator = WeatherAnalyticsOrchestrator()
    
    # Cargar buckets
    with open('config/buckets.json', 'r') as f:
        buckets = json.load(f)
    
    step_id = orchestrator.submit_emr_step(
        cluster_id,
        "ML Weather Prediction",
        "ml_weather_prediction.py",
        [buckets['trusted'], buckets['refined']]
    )
    
    status = orchestrator.wait_for_step_completion(cluster_id, step_id)
    print(f"✅ ML completado con status: {status}")

def setup_athena():
    """Configurar Athena"""
    print("🔍 Configurando Athena...")
    os.system("python applications/setup_athena.py")
    print("✅ Athena configurado")

def deploy_api():
    """Desplegar API Gateway"""
    print("🌐 Desplegando API...")
    print("📝 Para desplegar la API, sigue estos pasos:")
    print("   1. Crear función Lambda con el código en applications/weather_api.py")
    print("   2. Configurar API Gateway con los endpoints:")
    print("      - GET /weather/current")
    print("      - GET /weather/trends") 
    print("      - GET /weather/extreme-events")
    print("      - GET /weather/summary")
    print("   3. Conectar API Gateway con Lambda")
    print("   4. Configurar permisos IAM apropiados")
    print("✅ Instrucciones de API mostradas")

def run_full_pipeline(cluster_id):
    """Ejecutar pipeline completo"""
    if not cluster_id:
        print("❌ Se requiere cluster-id para ejecutar pipeline completo")
        sys.exit(1)
    
    print("🏭 Ejecutando pipeline completo...")
    from automation.orchestrator import WeatherAnalyticsOrchestrator
    
    orchestrator = WeatherAnalyticsOrchestrator()
    results = orchestrator.run_full_pipeline(cluster_id)
    
    print("🎉 Pipeline completado!")
    print(json.dumps(results, indent=2))

if __name__ == "__main__":
    main()