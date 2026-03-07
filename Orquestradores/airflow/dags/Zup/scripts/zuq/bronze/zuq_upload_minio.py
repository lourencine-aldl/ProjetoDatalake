import os
from minio import Minio

def getenv_required(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def upload_csv(client: Minio, bucket: str, local_file: str, object_name: str):
    if not os.path.exists(local_file):
        raise FileNotFoundError(f"File not found: {local_file}")

    client.fput_object(
        bucket,
        object_name,
        local_file,
        content_type="text/csv",
    )

    print(f"✅ Uploaded {local_file} -> s3://{bucket}/{object_name}")

def main():
    host = getenv_required("MINIO_HOST")
    port = os.getenv("MINIO_API_PORT", "9000")
    access = getenv_required("MINIO_ROOT_USER")
    secret = getenv_required("MINIO_ROOT_PASSWORD")
    bucket = os.getenv("MINIO_BUCKET_RAW", "raw-zone")

    endpoint = f"{host}:{port}"
    client = Minio(endpoint, access_key=access, secret_key=secret, secure=False)

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    files_to_upload = [
        (
            "/opt/airflow/data/zuq/vehicle_daily/latest/zuq_vehicle_daily.csv",
            "zuq/vehicle_daily/latest/zuq_vehicle_daily.csv",
        ),
        (
            "/opt/airflow/data/zuq/driver_workjourney/latest/zuq_driver_workjourney.csv",
            "zuq/driver_workjourney/latest/zuq_driver_workjourney.csv",
        ),
    ]

    for local_file, object_name in files_to_upload:
        upload_csv(client, bucket, local_file, object_name)

    print(f"🎉 Upload finalizado para bucket '{bucket}' via {endpoint}")

if __name__ == "__main__":
    main()
