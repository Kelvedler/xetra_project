import boto3
import os
import unittest
from moto import mock_aws

from src.common.s3 import S3BucketConnector


class TestS3BucketConnectorMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.mock = mock_aws()
        self.mock.start()
        self.s3_access_key = "AWS_ACCESS_KEY_ID"
        self.s3_secret_key = "AWS_SECRET_ACCESS_KEY"
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name = "test-bucket"
        os.environ[self.s3_access_key] = "KEY1"
        os.environ[self.s3_secret_key] = "KEY2"
        self.s3 = boto3.resource("s3", endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        self.s3_bucket_conn = S3BucketConnector(
            self.s3_access_key,
            self.s3_secret_key,
            self.s3_endpoint_url,
            self.s3_bucket_name,
        )

    def tearDown(self) -> None:
        self.mock.stop()

    def test_list_files_in_prefix_ok(self):
        prefix_exp = "prefix/"
        key1_exp = f"{prefix_exp}test1.csv"
        key2_exp = f"{prefix_exp}test2.csv"

        csv_content = """
        col1,col2
        valA,valB
        """
        self.s3_bucket.put_object(Body=csv_content, Key=key1_exp)
        self.s3_bucket.put_object(Body=csv_content, Key=key2_exp)

        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)

        self.assertEqual(len(list_result), 2)
        self.assertIn(key1_exp, list_result)
        self.assertIn(key2_exp, list_result)

        self.s3_bucket.delete_objects(
            Delete={"Objects": [{"Key": key1_exp}, {"Key": key2_exp}]}
        )

    def test_list_files_in_prefix_wrong_prefix(self):
        prefix_exp = "wrong-prefix/"

        list_result = self.s3_bucket_conn.list_files_in_prefix(prefix_exp)

        self.assertEqual(len(list_result), 0)


if __name__ == "__main__":
    unittest.main()
