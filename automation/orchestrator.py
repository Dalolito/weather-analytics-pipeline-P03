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
        
        results = {"api_results": [], "db_results": []}
        
        # Ejecutar ingesta de API
        try:
            from ingesta.fetch_openmeteo_api import OpenMeteoAPIIngesterImproved
            api_ingester = OpenMeteoAPIIngesterImproved(self.buckets['raw'])
            api_results = api_ingester.run_ingestion()
            results["api_results"] = api_results
            logger.info(f"✅ Ingesta de API completada: {len(api_results)} ciudades procesadas")
        except ImportError as e:
            logger.error(f"❌ Error importando OpenMeteoAPIIngester: {e}")
            # Ejecutar script directamente como fallback
            import subprocess
            import sys
            try:
                result = subprocess.run([
                    sys.executable, 'ingesta/fetch_openmeteo_api.py'
                ], capture_output=True, text=True, timeout=120)
                
                if result.returncode == 0:
                    logger.info("✅ Ingesta de API completada (via subprocess)")
                    results["api_results"] = [{"status": "completed_via_subprocess"}]
                else:
                    logger.error(f"❌ Error en ingesta API: {result.stderr}")
                    results["api_results"] = [{"status": "failed", "error": result.stderr}]
            except Exception as e2:
                logger.error(f"❌ Error ejecutando ingesta API: {e2}")
                results["api_results"] = [{"status": "failed", "error": str(e2)}]
        except Exception as e:
            logger.error(f"❌ Error en ingesta API: {e}")
            results["api_results"] = [{"status": "failed", "error": str(e)}]
        
        # Ejecutar ingesta de BD
        try:
            from ingesta.fetch_database import DatabaseIngesterImproved
            db_ingester = DatabaseIngesterImproved(self.buckets['raw'])
            db_results = db_ingester.run_ingestion()
            results["db_results"] = db_results
            logger.info(f"✅ Ingesta de BD completada: {len(db_results)} tablas procesadas")
        except ImportError as e:
            logger.error(f"❌ Error importando DatabaseIngester: {e}")
            # Ejecutar script directamente como fallback
            import subprocess
            import sys
            try:
                result = subprocess.run([
                    sys.executable, 'ingesta/fetch_database.py'
                ], capture_output=True, text=True, timeout=120)
                
                if result.returncode == 0:
                    logger.info("✅ Ingesta de BD completada (via subprocess)")
                    results["db_results"] = [{"status": "completed_via_subprocess"}]
                else:
                    logger.warning(f"⚠️ Ingesta BD (opcional): {result.stderr}")
                    results["db_results"] = [{"status": "skipped", "reason": "database_optional"}]
            except Exception as e2:
                logger.warning(f"⚠️ Error ejecutando ingesta BD (opcional): {e2}")
                results["db_results"] = [{"status": "skipped", "reason": "database_optional"}]
        except Exception as e:
            logger.warning(f"⚠️ Error en ingesta BD (opcional): {e}")
            results["db_results"] = [{"status": "skipped", "reason": "database_optional"}]
        
        return results

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
                    '--conf', 'spark.sql.adaptive.enabled=true',
                    '--conf', 'spark.sql.adaptive.coalescePartitions.enabled=true',
                    f"s3://{self.buckets['scripts']}/spark-jobs/{script_path}"
                ] + args
            }
        }
        
        response = self.emr_client.add_job_flow_steps(
            JobFlowId=cluster_id,
            Steps=[step]
        )
        
        step_id = response['StepIds'][0]
        logger.info(f"📝 Step '{step_name}' enviado con ID: {step_id}")
        return step_id

    def wait_for_step_completion(self, cluster_id, step_id, timeout_minutes=30):
        """Esperar a que complete un step con timeout"""
        start_time = time.time()
        timeout_seconds = timeout_minutes * 60
        
        while True:
            try:
                response = self.emr_client.describe_step(
                    ClusterId=cluster_id,
                    StepId=step_id
                )
                
                status = response['Step']['Status']['State']
                elapsed_time = int((time.time() - start_time) / 60)
                
                logger.info(f"⏳ Step {step_id} status: {status} (⏱️ {elapsed_time}min)")
                
                if status in ['COMPLETED', 'FAILED', 'CANCELLED']:
                    if status == 'FAILED':
                        failure_reason = response['Step']['Status'].get('FailureDetails', {}).get('Reason', 'Unknown')
                        logger.error(f"❌ Step falló: {failure_reason}")
                    return status
                
                # Verificar timeout
                if time.time() - start_time > timeout_seconds:
                    logger.warning(f"⏰ Step {step_id} timeout después de {timeout_minutes} minutos")
                    return 'TIMEOUT'
                
                time.sleep(30)  # Esperar 30 segundos antes de verificar de nuevo
                
            except Exception as e:
                logger.error(f"❌ Error verificando step {step_id}: {e}")
                return 'ERROR'

    def check_cluster_status(self, cluster_id):
        """Verificar estado del clúster"""
        try:
            response = self.emr_client.describe_cluster(ClusterId=cluster_id)
            cluster = response['Cluster']
            
            status = cluster['Status']['State']
            logger.info(f"🖥️ Clúster {cluster_id} status: {status}")
            
            if status not in ['WAITING', 'RUNNING']:
                logger.warning(f"⚠️ Clúster no está listo. Estado actual: {status}")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Error verificando clúster: {e}")
            return False

    def run_full_pipeline(self, cluster_id):
        """Ejecutar pipeline completo"""
        logger.info("🏭 Iniciando pipeline completo...")
        
        # Verificar estado del clúster
        if not self.check_cluster_status(cluster_id):
            raise Exception(f"Clúster {cluster_id} no está en estado WAITING o RUNNING")
        
        pipeline_results = {
            "cluster_id": cluster_id,
            "start_time": datetime.now().isoformat(),
            "steps": {}
        }
        
        try:
            # 1. Ejecutar ingesta
            logger.info("📥 Paso 1: Ejecutando ingesta de datos...")
            ingestion_results = self.run_ingestion_jobs()
            pipeline_results["steps"]["ingestion"] = {
                "status": "completed",
                "results": ingestion_results,
                "timestamp": datetime.now().isoformat()
            }
            logger.info("✅ Ingesta completada")
            
            # 2. Ejecutar ETL
            logger.info("⚙️ Paso 2: Ejecutando ETL con Spark...")
            etl_step_id = self.submit_emr_step(
                cluster_id,
                "ETL Weather Data",
                "etl_weather_data.py",
                [self.buckets['raw'], self.buckets['trusted']]
            )
            
            etl_status = self.wait_for_step_completion(cluster_id, etl_step_id, timeout_minutes=20)
            pipeline_results["steps"]["etl"] = {
                "status": etl_status,
                "step_id": etl_step_id,
                "timestamp": datetime.now().isoformat()
            }
            
            if etl_status != 'COMPLETED':
                logger.warning(f"⚠️ ETL no completó exitosamente: {etl_status}")
                # Continuar con el pipeline aunque ETL falle
            else:
                logger.info("✅ ETL completado")
            
            # 3. Ejecutar análisis
            logger.info("📊 Paso 3: Ejecutando análisis con Spark...")
            analytics_step_id = self.submit_emr_step(
                cluster_id,
                "Analytics Weather Trends",
                "analytics_weather_trends.py",
                [self.buckets['trusted'], self.buckets['refined']]
            )
            
            analytics_status = self.wait_for_step_completion(cluster_id, analytics_step_id, timeout_minutes=20)
            pipeline_results["steps"]["analytics"] = {
                "status": analytics_status,
                "step_id": analytics_step_id,
                "timestamp": datetime.now().isoformat()
            }
            
            if analytics_status != 'COMPLETED':
                logger.warning(f"⚠️ Analytics no completó exitosamente: {analytics_status}")
            else:
                logger.info("✅ Analytics completado")
            
            # 4. Ejecutar ML (opcional, con timeout más corto)
            logger.info("🤖 Paso 4: Ejecutando ML con Spark...")
            try:
                ml_step_id = self.submit_emr_step(
                    cluster_id,
                    "ML Weather Prediction",
                    "ml_weather_prediction.py",
                    [self.buckets['trusted'], self.buckets['refined']]
                )
                
                ml_status = self.wait_for_step_completion(cluster_id, ml_step_id, timeout_minutes=15)
                pipeline_results["steps"]["ml"] = {
                    "status": ml_status,
                    "step_id": ml_step_id,
                    "timestamp": datetime.now().isoformat()
                }
                
                if ml_status == 'COMPLETED':
                    logger.info("✅ ML completado")
                else:
                    logger.warning(f"⚠️ ML step: {ml_status}")
                    
            except Exception as e:
                logger.warning(f"⚠️ ML step falló (opcional): {e}")
                pipeline_results["steps"]["ml"] = {
                    "status": "failed",
                    "error": str(e),
                    "timestamp": datetime.now().isoformat()
                }
            
            # Agregar timestamp de finalización
            pipeline_results["end_time"] = datetime.now().isoformat()
            
            # Calcular estadísticas finales
            completed_steps = sum(1 for step in pipeline_results["steps"].values() 
                                if step.get("status") == "completed" or step.get("status") == "COMPLETED")
            total_steps = len(pipeline_results["steps"])
            
            logger.info(f"🎯 Pipeline completado: {completed_steps}/{total_steps} pasos exitosos")
            
            return pipeline_results
            
        except Exception as e:
            logger.error(f"❌ Error en pipeline: {e}")
            pipeline_results["error"] = str(e)
            pipeline_results["end_time"] = datetime.now().isoformat()
            raise e

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