import pytest
import boto3
from botocore.exceptions import ClientError
from moto import mock_s3


from app.s3_methods import (
    upload_to_bucket, download_from_bucket
)


@mock_s3
class TestS3Methods:
    bucket = "moto-example"

    def test_create_bucket(self):
        s3 = boto3.resource("s3", region_name="ap-northeast-1")
        assert [b.name for b in s3.buckets.all()] == []

        s3.create_bucket(Bucket=TestS3Methods.bucket,
                         CreateBucketConfiguration={'LocationConstraint': 'ap-northeast-1'})

        assert [b.name for b in s3.buckets.all()] == [TestS3Methods.bucket]

    def test_upload_succeed(self):
        s3 = boto3.resource("s3", region_name="ap-northeast-1")
        s3.create_bucket(Bucket=TestS3Methods.bucket,
                         CreateBucketConfiguration={'LocationConstraint': 'ap-northeast-1'})

        assert upload_to_bucket("./data/example.txt", "example.txt")

        body = s3.Object(TestS3Methods.bucket,
                         "data/example.txt").get()["Body"].read().decode("utf-8")

        assert body == "Hello, world!\n"

    def test_download_failed(self):
        s3 = boto3.resource("s3")
        s3.create_bucket(Bucket=TestS3Methods.bucket,
                         CreateBucketConfiguration={'LocationConstraint': 'ap-northeast-1'})

        with pytest.raises(ClientError):
            download_from_bucket("nonexist.txt", "output/example.txt")


if __name__ == '__main__':
    pytest.main()
