import boto3
import os
from dotenv import load_dotenv

# Cargar archivo .env
load_dotenv()

def test_aws_connection():
    try:
        # Mostrar credenciales cargadas (parcialmente)
        access_key = os.getenv('AWS_ACCESS_KEY_ID', 'NOT_FOUND')
        region = os.getenv('AWS_DEFAULT_REGION', 'NOT_FOUND')
        
        print(f"🔑 Access Key: {access_key[:10]}...{access_key[-4:]}")
        print(f"🌍 Region: {region}")
        
        # Crear cliente STS
        sts = boto3.client('sts')
        
        # Verificar identidad
        identity = sts.get_caller_identity()
        print(f"✅ AWS Identity verified!")
        print(f"   Account: {identity['Account']}")
        print(f"   User: {identity['Arn']}")
        
        # Probar S3
        s3 = boto3.client('s3')
        buckets = s3.list_buckets()
        print(f"✅ S3 Access verified!")
        print(f"   Available buckets: {len(buckets['Buckets'])}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

if __name__ == "__main__":
    print("🧪 Testing AWS connection...")
    success = test_aws_connection()
    
    if success:
        print("\n🎉 All tests passed! AWS is configured correctly.")
    else:
        print("\n💡 Tip: Make sure your .env file has the correct credentials")