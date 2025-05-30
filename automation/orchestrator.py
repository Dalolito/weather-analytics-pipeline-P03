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

class WeatherAnalyticsOrchestrator:
    def __init__(self):
        # Usar región desde variables de entorno
        aws_region = os.getenv('AWS_DEFAULT_REGION', 'us-east-1')
        
        self.emr_client = boto3.client('emr', region_name=aws_region)
        self.s3_client = boto3.client('s3', region_name=aws_region)
        
        # Cargar configuración
        with open('config/buckets.json', 'r') as f:
            self.buckets = json.load(f)

    def run_ingestion_jobs(self):
        """Ejecutar trabajos de ingesta"""
        logger.info("🚀 Iniciando trabajos de ingesta...")
        
        # Ejecutar ingesta de API
        from ingesta.fetch_openmeteo_api import OpenMeteoAPIIngester
        api_ingester = OpenMeteoAPIIngester(self.buckets['raw'])
        api_results = api_ingester.run_ingestion()
        
        # Ejecutar ingesta de BD
        from ingesta.fetch_database import DatabaseIngester
        db_ingester = DatabaseIngester(self.buckets['raw'])
        db_results = db_ingester.run_ingestion()
        
        return {"api_results": api_results, "db_results": db_results}

    def submit_emr_step(self, cluster_id, step_name, script_path, args):
        """Enviar step a EMR"""
        step = {
            'Name': step_name,
            'ActionOnFailure': 'CONTINUE',
            'HadoopJarStep': {
                'Jar': 'command-runner.jar',
                'Args': [
                    'spark-submit',
                    '--deploy-mode', 'cluster',
                    f"s3://{self.buckets['scripts']}/spark-jobs/{script_path}"
                ] + args
            }
        }
        
        response = self.emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step]
        )
        
        return response['StepIds'][0]

    def wait_for_step_completion(self, cluster_id, step_id):
        """Esperar a que complete un step"""
        while True:
            response = self.emr_client.describe_step(
                ClusterId=cluster_id,
                StepId=step_id
            )
            
            status = response['Step']['Status']['State']
            logger.info(f"Step {step_id} status: {status}")
            
            if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                return status
            
            time.sleep(30)

    def run_full_pipeline(self, cluster_id):
        """Ejecutar pipeline completo"""
        logger.info("🏭 Iniciando pipeline completo...")
        
        # 1. Ejecutar ingesta
        ingestion_results = self.run_ingestion_jobs()
        logger.info("✅ Ingesta completada")
        
        # 2. Ejecutar ETL
        etl_step_id = self.submit_emr_step(
            cluster_id,
            "ETL Weather Data",
            "etl_weather_data.py",
            [self.buckets['raw'], self.buckets['trusted']]
        )
        
        etl_status = self.wait_for_step_completion(cluster_id, etl_step_id)
        if etl_status != 'COMPLETED':
            raise Exception(f"ETL falló con status: {etl_status}")
        
        logger.info("✅ ETL completado")
        
        # 3. Ejecutar análisis
        analytics_step_id = self.submit_emr_step(
            cluster_id,
            "Analytics Weather Trends",
            "analytics_weather_trends.py",
            [self.buckets['trusted'], self.buckets['refined']]
        )
        
        analytics_status = self.wait_for_step_completion(cluster_id, analytics_step_id)
        if analytics_status != 'COMPLETED':
            raise Exception(f"Analytics falló con status: {analytics_status}")
        
        logger.info("✅ Analytics completado")
        
        # 4. Ejecutar ML (opcional)
        try:
            ml_step_id = self.submit_emr_step(
                cluster_id,
                "ML Weather Prediction",
                "ml_weather_prediction.py",
                [self.buckets['trusted'], self.buckets['refined']]
            )
            
            ml_status = self.wait_for_step_completion(cluster_id, ml_step_id)
            logger.info(f"ML completado con status: {ml_status}")
        except Exception as e:
            logger.warning(f"ML step falló: {e}")
        
        return {
            "ingestion": ingestion_results,
            "etl_status": etl_status,
            "analytics_status": analytics_status,
            "timestamp": datetime.now().isoformat()
        }

if __name__ == "__main__":
    orchestrator = WeatherAnalyticsOrchestrator()
    
    # Obtener cluster ID (puedes pasarlo como parámetro)
    cluster_id = input("Ingresa el ID del clúster EMR: ")
    
    try:
        results = orchestrator.run_full_pipeline(cluster_id)
        logger.info("🎉 Pipeline completado exitosamente!")
        print(json.dumps(results, indent=2))
    except Exception as e:
        logger.error(f"❌ Pipeline falló: {e}")