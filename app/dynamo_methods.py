import boto3
import datetime


def put_to_dynamo(user_id: int, access_count: int, last_accessed_at: datetime.datetime):
    dynamo_client = boto3.client("dynamodb")

    item = {
        "user_id": {"N": str(user_id)},
        "access_count": {"N": str(access_count)},
        "last_accessed_at": {"S": last_accessed_at.isoformat()},
    }

    dynamo_client.put_item(TableName="moto-example", Item=item)


def get_from_dynamo(user_id: int) -> dict:
    dynamo_client = boto3.client("dynamodb")

    key = {"user_id": {"N": str(user_id)}}

    item = dynamo_client.get_item(TableName="moto-example", Key=key)["Item"]

    return {
        "user_id": int(item["user_id"]["N"]),
        "access_count": int(item["access_count"]["N"]),
        "last_accessed_at": datetime.datetime.fromisoformat(item["last_accessed_at"]["S"]),
    }
