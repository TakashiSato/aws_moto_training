import boto3
import datetime


def upload_to_bucket(file_path: str, file_name: str) -> bool:
    s3_client = boto3.client("s3")

    _ = s3_client.upload_file(file_path, "moto-example", "data/" + file_name)

    return True


def download_from_bucket(file_name: str, file_path: str) -> bool:
    s3_client = boto3.client("s3")

    _ = s3_client.download_file("moto-example", "data/" + file_name, file_path)

    return True
