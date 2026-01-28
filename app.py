import boto3
import json
import os
from datetime import datetime, timezone, timedelta
import tempfile
import mimetypes

CONFIG_FILE = "config.json"
LAST_CONFIG_FILE = "last_config.json"

# ---------- Funciones de helpers ----------

def load_config():
    with open(CONFIG_FILE) as f:
        return json.load(f)

def load_last_config():
    if os.path.exists(LAST_CONFIG_FILE):
        with open(LAST_CONFIG_FILE) as f:
            return json.load(f)
    return {}

def save_last_config(data):
    with open(LAST_CONFIG_FILE, "w") as f:
        json.dump(data, f)

# ---------- Conexiones S3 ----------

def create_s3_client(endpoint_url, access_key, secret_key):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

# ---------- Lógica de sincronización ----------

def sync_r2_to_minio():
    config = load_config()
    last_config = load_last_config()
    
    r2 = config["r2"]
    minio = config["minio"]
    paths = config["paths"]
    dias_ultimos = config.get("dias_ultimos", 7)  # configurable desde config.json
    
    # Crear cliente MinIO
    minio_client = create_s3_client(minio["endpoint_url"], minio["access_key"], minio["secret_key"])

    # ---------- Revisar si dias_ultimos disminuyó ----------
    dias_ultimos_anterior = last_config.get("dias_ultimos", dias_ultimos)
    if dias_ultimos < dias_ultimos_anterior:
        print(f"[INFO] dias_ultimos disminuyó ({dias_ultimos_anterior} -> {dias_ultimos}), eliminando archivos antiguos en MinIO...")
        for path in paths:
            # Listar y eliminar objetos que empiezan con path
            response = minio_client.list_objects_v2(Bucket=minio["bucket"], Prefix=path)
            if "Contents" in response:
                for obj in response["Contents"]:
                    obj_key = obj["Key"]
                    minio_client.delete_object(Bucket=minio["bucket"], Key=obj_key)
                    print(f"[DELETE] {obj_key} eliminado")
        print("[INFO] MinIO limpio ✅")
    
    # Guardar el valor actual de dias_ultimos
    save_last_config({"dias_ultimos": dias_ultimos})
    
    # ---------- Sincronización ----------
    now = datetime.now(timezone.utc)
    fecha_limite = now - timedelta(days=dias_ultimos)
    
    print(f"Sincronizando archivos de los últimos {dias_ultimos} días desde {fecha_limite} hasta {now}")
    
    r2_client = create_s3_client(r2["endpoint_url"], r2["access_key"], r2["secret_key"])
    
    for path in paths:
        response = r2_client.list_objects_v2(Bucket=r2["bucket"], Prefix=path)
        if "Contents" not in response:
            print(f"No se encontraron archivos en la ruta: {path}")
            continue
        
        for obj in response["Contents"]:
            obj_key = obj["Key"]
            last_modified = obj["LastModified"].replace(tzinfo=timezone.utc)
            
            # Filtrar solo archivos de los últimos N días
            if last_modified < fecha_limite:
                print(f"[SKIP] {obj_key} es anterior a los últimos {dias_ultimos} días")
                continue
            
            print(f"[INFO] Archivo encontrado: {obj_key} - LastModified: {last_modified}")
            
            # Descargar archivo a temp
            local_file = os.path.join(tempfile.gettempdir(), os.path.basename(obj_key))
            try:
                print(f"[DOWNLOAD] Descargando {obj_key} a {local_file}")
                r2_client.download_file(r2["bucket"], obj_key, local_file)
            except Exception as e:
                print(f"[ERROR] No se pudo descargar {obj_key}: {e}")
                continue
            
            # Detectar tipo de contenido automáticamente
            content_type, _ = mimetypes.guess_type(local_file)
            if content_type is None:
                content_type = "application/octet-stream"
            
            # Subir a MinIO con ContentType y ContentDisposition=inline
            try:
                print(f"[UPLOAD] Subiendo {obj_key} a MinIO bucket {minio['bucket']}")
                minio_client.upload_file(
                    local_file,
                    minio["bucket"],
                    obj_key,
                    ExtraArgs={
                        "ContentType": content_type,
                        "ContentDisposition": "inline"
                    }
                )
                print(f"[OK] Sincronizado: {obj_key} con ContentType={content_type}")
            except Exception as e:
                print(f"[ERROR] No se pudo subir {obj_key} a MinIO: {e}")
                continue
            finally:
                if os.path.exists(local_file):
                    os.remove(local_file)
    
    print("Sincronización completada ✅")

# ---------- Ejecutar ----------

if __name__ == "__main__":
    sync_r2_to_minio()
