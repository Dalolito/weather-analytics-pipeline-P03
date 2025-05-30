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
        self.ec2_client = boto3.client('ec2')
        
        # Cargar configuración de buckets
        with open('config/buckets.json', 'r') as f:
            self.buckets = json.load(f)

    def get_available_instance_types(self):
        """Detectar tipos de instancia disponibles en AWS Academy"""
        # Lista de tipos de instancia en orden de preferencia (de mejor a peor)
        preferred_instances = [
            'm5.xlarge', 'm5.large', 'm4.xlarge', 'm4.large', 
            'm3.xlarge', 'm3.large', 't3.medium', 't2.medium'
        ]
        
        available_instances = []
        
        try:
            # Verificar qué tipos están disponibles
            for instance_type in preferred_instances:
                try:
                    response = self.ec2_client.describe_instance_type_offerings(
                        LocationType='availability-zone',
                        Filters=[
                            {'Name': 'instance-type', 'Values': [instance_type]}
                        ]
                    )
                    if response['InstanceTypeOfferings']:
                        available_instances.append(instance_type)
                        logger.info(f"✅ {instance_type} disponible")
                except Exception as e:
                    logger.debug(f"⚠️ {instance_type} no disponible: {e}")
            
            if not available_instances:
                # Fallback a tipos básicos que generalmente están disponibles
                available_instances = ['t2.medium', 't3.medium']
                logger.warning("Usando tipos de instancia básicos como fallback")
            
            return available_instances
            
        except Exception as e:
            logger.warning(f"No se pudo verificar tipos de instancia: {e}")
            # Tipos seguros para AWS Academy
            return ['m4.large', 't2.medium']

    def create_cluster_config(self, instance_types):
        """Crear configuración del clúster con tipos de instancia detectados"""
        cluster_name = f"weather-analytics-cluster-{datetime.now().strftime('%Y%m%d-%H%M%S')}"
        
        # Usar el mejor tipo disponible para master y worker
        master_instance = instance_types[0]
        worker_instance = instance_types[0] if len(instance_types) > 0 else master_instance
        
        logger.info(f"🎯 Usando Master: {master_instance}, Worker: {worker_instance}")
        
        cluster_config = {
            'Name': cluster_name,
            'LogUri': f"s3://{self.buckets['scripts']}/logs/emr/",
            'ReleaseLabel': 'emr-6.13.0',  # Versión más estable para AWS Academy
            'Applications': [
                {'Name': 'Spark'},
                {'Name': 'Hadoop'},
                {'Name': 'Hive'}
            ],
            'Instances': {
                'InstanceGroups': [
                    {
                        'Name': 'Master',
                        'Market': 'ON_DEMAND',
                        'InstanceRole': 'MASTER',
                        'InstanceType': master_instance,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'Worker',
                        'Market': 'ON_DEMAND', 
                        'InstanceRole': 'CORE',
                        'InstanceType': worker_instance,
                        'InstanceCount': 1,  # Solo 1 worker para AWS Academy
                    }
                ],
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
                        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer'
                    }
                },
                {
                    'Classification': 'spark-env',
                    'Properties': {},
                    'Configurations': [
                        {
                            'Classification': 'export',
                            'Properties': {
                                'PYSPARK_PYTHON': '/usr/bin/python3'
                            }
                        }
                    ]
                }
            ]
        }
        
        return cluster_config

    def create_cluster(self):
        """Crear clúster EMR para procesamiento Spark"""
        logger.info("🚀 Iniciando creación de clúster EMR...")
        
        # Detectar tipos de instancia disponibles
        available_instances = self.get_available_instance_types()
        logger.info(f"📋 Tipos de instancia disponibles: {available_instances}")
        
        # Crear configuración del clúster
        cluster_config = self.create_cluster_config(available_instances)
        
        try:
            logger.info("⏳ Creando clúster EMR...")
            response = self.emr_client.run_job_flow(**cluster_config)
            cluster_id = response['JobFlowId']
            
            logger.info(f"✅ Clúster EMR creado exitosamente: {cluster_id}")
            logger.info(f"📝 Nombre: {cluster_config['Name']}")
            logger.info(f"🖥️ Master: {cluster_config['Instances']['InstanceGroups'][0]['InstanceType']}")
            logger.info(f"⚙️ Worker: {cluster_config['Instances']['InstanceGroups'][1]['InstanceType']}")
            
            # Guardar información del clúster
            cluster_info = {
                'cluster_id': cluster_id,
                'cluster_name': cluster_config['Name'],
                'created_at': datetime.now().isoformat(),
                'master_instance': cluster_config['Instances']['InstanceGroups'][0]['InstanceType'],
                'worker_instance': cluster_config['Instances']['InstanceGroups'][1]['InstanceType'],
                'status': 'STARTING'
            }
            
            with open('cluster_info.json', 'w') as f:
                json.dump(cluster_info, f, indent=2)
            
            logger.info("📄 Información del clúster guardada en cluster_info.json")
            
            return cluster_id
            
        except Exception as e:
            logger.error(f"❌ Error creando clúster EMR: {e}")
            
            # Si el error es de permisos o roles, dar sugerencias
            if 'EMR_DefaultRole' in str(e):
                logger.info("💡 SOLUCIÓN: Los roles EMR no están configurados")
                logger.info("   Ejecuta en AWS CLI:")
                logger.info("   aws emr create-default-roles")
            elif 'ValidationException' in str(e):
                logger.info("💡 SOLUCIÓN: Problema con tipos de instancia")
                logger.info(f"   Tipos disponibles detectados: {available_instances}")
            elif 'UnauthorizedOperation' in str(e):
                logger.info("💡 SOLUCIÓN: Permisos insuficientes en AWS Academy")
                logger.info("   Verifica que tengas permisos para crear clústeres EMR")
            
            return None

    def upload_spark_jobs(self):
        """Subir jobs de Spark a S3"""
        jobs = [
            'spark_jobs/etl_weather_data.py',
            'spark_jobs/analytics_weather_trends.py',
            'spark_jobs/ml_weather_prediction.py'
        ]
        
        uploaded_jobs = []
        
        for job in jobs:
            if os.path.exists(job):
                try:
                    self.s3_client.upload_file(
                        job,
                        self.buckets['scripts'],
                        f"spark-jobs/{os.path.basename(job)}"
                    )
                    uploaded_jobs.append(job)
                    logger.info(f"📁 Job subido: {job}")
                except Exception as e:
                    logger.error(f"❌ Error subiendo {job}: {e}")
            else:
                logger.warning(f"⚠️ Archivo no encontrado: {job}")
        
        return uploaded_jobs

    def create_bootstrap_script(self):
        """Crear script de bootstrap para EMR"""
        bootstrap_script = '''#!/bin/bash
# Bootstrap script para EMR
set -e

echo "🚀 Iniciando bootstrap de EMR..."

# Actualizar pip
sudo python3 -m pip install --upgrade pip

# Instalar paquetes Python necesarios
echo "📦 Instalando paquetes Python..."
sudo python3 -m pip install pandas
sudo python3 -m pip install boto3
sudo python3 -m pip install python-dotenv
sudo python3 -m pip install mysql-connector-python
sudo python3 -m pip install scikit-learn

echo "✅ Bootstrap completado!"
'''
        
        try:
            self.s3_client.put_object(
                Bucket=self.buckets['scripts'],
                Key='bootstrap/install_packages.sh',
                Body=bootstrap_script,
                ContentType='text/plain'
            )
            logger.info("📦 Script de bootstrap creado")
            return True
        except Exception as e:
            logger.error(f"❌ Error creando bootstrap: {e}")
            return False

    def check_cluster_status(self, cluster_id):
        """Verificar estado del clúster"""
        try:
            response = self.emr_client.describe_cluster(ClusterId=cluster_id)
            cluster = response['Cluster']
            
            status = cluster['Status']['State']
            status_reason = cluster['Status'].get('StateChangeReason', {}).get('Message', '')
            
            logger.info(f"📊 Estado del clúster {cluster_id}: {status}")
            if status_reason:
                logger.info(f"📝 Razón: {status_reason}")
            
            return status
            
        except Exception as e:
            logger.error(f"❌ Error verificando estado: {e}")
            return None

    def setup_complete_emr(self):
        """Setup completo de EMR"""
        logger.info("🏗️ CONFIGURACIÓN COMPLETA DE EMR")
        logger.info("="*50)
        
        # Paso 1: Crear script de bootstrap
        logger.info("📝 Paso 1: Creando script de bootstrap...")
        bootstrap_ok = self.create_bootstrap_script()
        
        # Paso 2: Subir jobs de Spark
        logger.info("📝 Paso 2: Subiendo jobs de Spark...")
        uploaded_jobs = self.upload_spark_jobs()
        
        # Paso 3: Crear clúster
        logger.info("📝 Paso 3: Creando clúster EMR...")
        cluster_id = self.create_cluster()
        
        # Resumen
        logger.info(f"\n📊 RESUMEN DEL SETUP")
        logger.info("="*30)
        logger.info(f"📦 Bootstrap: {'✅ OK' if bootstrap_ok else '❌ ERROR'}")
        logger.info(f"📁 Jobs subidos: {len(uploaded_jobs)}/3")
        logger.info(f"🚀 Clúster: {'✅ CREADO' if cluster_id else '❌ ERROR'}")
        
        if cluster_id:
            logger.info(f"\n🎉 ¡EMR configurado exitosamente!")
            logger.info(f"🆔 Cluster ID: {cluster_id}")
            logger.info(f"⏳ Estado inicial: STARTING")
            logger.info(f"💡 Monitorear estado: python -c \"from infrastructure.setup_emr_cluster_improved import EMRClusterManager; mgr = EMRClusterManager(); mgr.check_cluster_status('{cluster_id}')\"")
            
            return cluster_id
        else:
            logger.error("❌ No se pudo crear el clúster EMR")
            return None

if __name__ == "__main__":
    manager = EMRClusterManager()
    cluster_id = manager.setup_complete_emr()
    
    if cluster_id:
        print(f"\n🎯 SIGUIENTE PASO:")
        print(f"   Esperar que el clúster esté WAITING (5-10 minutos)")
        print(f"   Luego ejecutar: python run_project.py --step full-pipeline --cluster-id {cluster_id}")
    else:
        print(f"\n💡 SOLUCIONES ALTERNATIVAS:")
        print(f"   1. Verificar permisos EMR en AWS Academy")
        print(f"   2. Crear roles EMR: aws emr create-default-roles")
        print(f"   3. Usar ETL simplificado: python simple_etl_processor.py")