import pytest
import datetime
import boto3
from moto import mock_dynamodb


from app.dynamo_methods import (
    put_to_dynamo, get_from_dynamo
)


@mock_dynamodb
class TestDynamoMethods:
    def setup_method(self, method):
        dynamo = boto3.resource("dynamodb", region_name="ap-northeast-1")
        dynamo.create_table(
            TableName="moto-example",
            KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
            AttributeDefinitions=[
                {"AttributeName": "user_id", "AttributeType": "N"}],
            BillingMode="PAY_PER_REQUEST",
        )

    def test_put_get_succeed(self):
        put_to_dynamo(user_id=33, access_count=10,
                      last_accessed_at=datetime.datetime(2020, 3, 21, 10, 30, 15))

        item = get_from_dynamo(user_id=33)
        assert item["user_id"] == 33
        assert item["access_count"] == 10
        assert item["last_accessed_at"] == datetime.datetime(
            2020, 3, 21, 10, 30, 15)


if __name__ == '__main__':
    pytest.main()
