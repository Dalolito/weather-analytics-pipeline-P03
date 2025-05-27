import boto3
import json
import os
from datetime import datetime
from dotenv import load_dotenv
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class EMRClusterManager:
    def __init__(self):
        self.emr_client = boto3.client('emr', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))
        self.s3_client = boto3.client('s3')
        
        # Cargar configuración de buckets
        with open('config/buckets.json', 'r') as f:
            self.buckets = json.load(f)

    def create_cluster(self):
        """Crear clúster EMR para procesamiento Spark"""
        cluster_name = f"weather-analytics-cluster-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        cluster_config = {
            'Name': cluster_name,
            'LogUri': f"s3://{self.buckets['scripts']}/logs/emr/",
            'ReleaseLabel': 'emr-6.15.0',
            'Applications': [
                {'Name': 'Spark'},
                {'Name': 'Hadoop'},
                {'Name': 'Hive'},
                {'Name': 'Livy'}
            ],
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': 'Master',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': 'm5.xlarge',
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'CORE',
                        'InstanceType': 'm5.large',
                        'InstanceCount': 2,
                    }
                ],
                'Ec2KeyName': 'weather-analytics-key',
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': False,
            },
            'ServiceRole': 'EMR_DefaultRole',
            'JobFlowRole': 'EMR_EC2_DefaultRole',
            'VisibleToAllUsers': True,
            'BootstrapActions': [
                {
                    'Name': 'Install Python packages',
                    'ScriptBootstrapAction': {
                        'Path': f"s3://{self.buckets['scripts']}/bootstrap/install_packages.sh"
                    }
                }
            ],
            'Configurations': [
                {
                    'Classification': 'spark-defaults',
                    'Properties': {
                        'spark.sql.adaptive.enabled': 'true',
                        'spark.sql.adaptive.coalescePartitions.enabled': 'true',
                        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
                        'spark.sql.hive.metastore.version': '2.3.9'
                    }
                }
            ]
        }

        try:
            response = self.emr_client.run_job_flow(**cluster_config)
            cluster_id = response['JobFlowId']
            logger.info(f"✅ Clúster EMR creado: {cluster_id}")
            return cluster_id
        except Exception as e:
            logger.error(f"❌ Error creando clúster EMR: {e}")
            return None

    def upload_spark_jobs(self):
        """Subir jobs de Spark a S3"""
        jobs = [
            'spark_jobs/etl_weather_data.py',
            'spark_jobs/analytics_weather_trends.py',
            'spark_jobs/ml_weather_prediction.py'
        ]
        
        for job in jobs:
            try:
                self.s3_client.upload_file(
                    job,
                    self.buckets['scripts'],
                    f"spark-jobs/{os.path.basename(job)}"
                )
                logger.info(f"📁 Job subido: {job}")
            except Exception as e:
                logger.error(f"❌ Error subiendo {job}: {e}")

    def create_bootstrap_script(self):
        """Crear script de bootstrap para EMR"""
        bootstrap_script = '''#!/bin/bash
sudo pip3 install pandas
sudo pip3 install boto3
sudo pip3 install python-dotenv
sudo pip3 install mysql-connector-python
sudo pip3 install scikit-learn
'''
        
        try:
            self.s3_client.put_object(
                Bucket=self.buckets['scripts'],
                Key='bootstrap/install_packages.sh',
                Body=bootstrap_script,
                ContentType='text/plain'
            )
            logger.info("📦 Script de bootstrap creado")
        except Exception as e:
            logger.error(f"❌ Error creando bootstrap: {e}")

if __name__ == "__main__":
    manager = EMRClusterManager()
    
    # Crear script de bootstrap
    manager.create_bootstrap_script()
    
    # Subir jobs de Spark
    manager.upload_spark_jobs()
    
    # Crear clúster
    cluster_id = manager.create_cluster()
    
    if cluster_id:
        print(f"🎉 Clúster EMR creado exitosamente: {cluster_id}")
        print("⏳ Esperando que el clúster esté listo...")