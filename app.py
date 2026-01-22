import os
from dotenv import load_dotenv
import boto3

# Cargar variables de entorno
load_dotenv()

# ===== Cloudflare R2 =====
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET_NAME = os.getenv("R2_BUCKET_NAME")
R2_ENDPOINT = os.getenv("R2_ENDPOINT")

r2_client = boto3.client(
    "s3",
    region_name="auto",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
)

# ===== MinIO =====
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET_NAME = os.getenv("MINIO_BUCKET_NAME")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")

minio_client = boto3.client(
    "s3",
    region_name="us-east-1",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

# ===== Funciones para listar archivos =====
def list_files_s3(client, bucket_name):
    """Lista todos los objetos en un bucket S3"""
    files = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name):
        for obj in page.get("Contents", []):
            files.append(obj["Key"])
    return files

# ===== Listar archivos =====
r2_files = list_files_s3(r2_client, R2_BUCKET_NAME)
minio_files = list_files_s3(minio_client, MINIO_BUCKET_NAME)

# ===== Comparar y mostrar =====
print(f"Archivos en R2 ({R2_BUCKET_NAME}): {len(r2_files)}")
print(f"Archivos en MinIO ({MINIO_BUCKET_NAME}): {len(minio_files)}\n")

r2_set = set(r2_files)
minio_set = set(minio_files)

solo_r2 = r2_set - minio_set
solo_minio = minio_set - r2_set

print(f"Archivos solo en R2 ({len(solo_r2)}):")
for f in solo_r2:
    print(f"  {f}")

print(f"\nArchivos solo en MinIO ({len(solo_minio)}):")
for f in solo_minio:
    print(f"  {f}")
