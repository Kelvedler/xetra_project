from datetime import datetime, timedelta
from io import StringIO
import os
import unittest

import boto3
from moto import mock_aws
import pandas as pd

from src.common.constants import MetaProcessFormat
from src.common.custom_exceptions import WrongMetaFileException
from src.common.meta_process import MetaProcess
from src.common.s3 import S3BucketConnector


class TestMetaProcessMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.mock = mock_aws()
        self.mock.start()
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name = "test-bucket"
        self.s3 = boto3.resource("s3", endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        self.s3_bucket = self.s3.Bucket(self.s3_bucket_name)
        self.s3_bucket_conn = S3BucketConnector(
            self.s3_endpoint_url,
            self.s3_bucket_name,
        )
        self.today = datetime(year=2022, month=3, day=20).date()
        self.dates = [
            (self.today - timedelta(days=day)).strftime(
                MetaProcessFormat.DATE_FORMAT.value
            )
            for day in range(8)
        ]

    def tearDown(self) -> None:
        self.mock.stop()

    def test_update_meta_file_no_meta_file(self):
        date_list_exp = ["2021-04-16", "202104-17"]
        proc_date_list_exp = [datetime.today().date()] * 2

        meta_key = "meta.csv"

        MetaProcess.update_meta_file(self.s3_bucket_conn, meta_key, date_list_exp)

        data = (
            self.s3_bucket.Object(key=meta_key).get().get("Body").read().decode("utf-8")
        )
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.SOURCE_DATE_COL.value])
        proc_date_list_result = list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.PROCESS_COL.value]).dt.date
        )

        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": meta_key}]})

    def test_update_meta_file_empty_date_list(self):
        return_exp = None
        log_exp = "The dataframe is empty! No file will be written!"

        date_list = []
        meta_key = "meta.csv"

        with self.assertLogs() as logm:
            result = MetaProcess.update_meta_file(
                self.s3_bucket_conn, meta_key, date_list
            )
            self.assertIn(log_exp, logm.output[1])
        self.assertEqual(return_exp, result)

    def test_update_meta_file_meta_file_ok(self):
        date_list_old = ["2021-04-12", "2021-04.13"]
        date_list_new = ["2021-04-16", "2021-04-17"]
        date_list_exp = date_list_old + date_list_new
        proc_date_list_exp = [datetime.today().date()] * 4

        meta_key = "meta.csv"
        meta_content = (
            f"{MetaProcessFormat.SOURCE_DATE_COL.value},"
            f"{MetaProcessFormat.PROCESS_COL.value}\n"
            f"{date_list_old[0]},"
            f"{datetime.today().strftime(MetaProcessFormat.PROCESS_DATE_FORMAT.value)}\n"
            f"{date_list_old[1]},"
            f"{datetime.today().strftime(MetaProcessFormat.PROCESS_DATE_FORMAT.value)}"
        )

        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)

        MetaProcess.update_meta_file(self.s3_bucket_conn, meta_key, date_list_new)

        data = (
            self.s3_bucket.Object(key=meta_key).get().get("Body").read().decode("utf-8")
        )
        out_buffer = StringIO(data)
        df_meta_result = pd.read_csv(out_buffer)
        date_list_result = list(df_meta_result[MetaProcessFormat.SOURCE_DATE_COL.value])
        proc_date_list_result = list(
            pd.to_datetime(df_meta_result[MetaProcessFormat.PROCESS_COL.value]).dt.date
        )

        self.assertEqual(date_list_exp, date_list_result)
        self.assertEqual(proc_date_list_exp, proc_date_list_result)

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": meta_key}]})

    def test_update_meta_file_meta_file_wrong(self):
        date_list_old = ["2021-04-12", "2021-04-13"]
        date_list_new = ["2021-04-16", "2021-04-17"]

        meta_key = "meta.csv"
        meta_content = (
            f"wrong_column,{MetaProcessFormat.PROCESS_COL.value}\n"
            f"{date_list_old[0]},"
            f"{datetime.today().strftime(MetaProcessFormat.PROCESS_DATE_FORMAT.value)}\n"
            f"{date_list_old[1]},"
            f"{datetime.today().strftime(MetaProcessFormat.PROCESS_DATE_FORMAT.value)}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)

        with self.assertRaises(WrongMetaFileException):
            MetaProcess.update_meta_file(self.s3_bucket_conn, meta_key, date_list_new)

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": meta_key}]})

    def test_return_date_list_no_meta_file(self):
        date_list_exp = [
            (self.today - timedelta(days=day)).strftime(
                MetaProcessFormat.DATE_FORMAT.value
            )
            for day in range(4)
        ]
        min_date_exp = (self.today - timedelta(days=2)).strftime(
            MetaProcessFormat.DATE_FORMAT.value
        )

        first_date = min_date_exp
        meta_key = "meta.csv"

        min_date_return, date_list_return = MetaProcess.return_date_list(
            self.s3_bucket_conn, meta_key, first_date
        )

        self.assertEqual(set(date_list_exp), set(date_list_return))
        self.assertEqual(min_date_exp, min_date_return)

    def test_return_date_list_meta_file_ok(self):
        min_date_exp = [
            (self.today - timedelta(days=1)).strftime(
                MetaProcessFormat.DATE_FORMAT.value
            ),
            (self.today - timedelta(days=2)).strftime(
                MetaProcessFormat.DATE_FORMAT.value
            ),
            (self.today - timedelta(days=7)).strftime(
                MetaProcessFormat.DATE_FORMAT.value
            ),
        ]
        date_list_exp = [
            [
                (self.today - timedelta(days=day)).strftime(
                    MetaProcessFormat.DATE_FORMAT.value
                )
                for day in range(3)
            ],
            [
                (self.today - timedelta(days=day)).strftime(
                    MetaProcessFormat.DATE_FORMAT.value
                )
                for day in range(4)
            ],
            [
                (self.today - timedelta(days=day)).strftime(
                    MetaProcessFormat.DATE_FORMAT.value
                )
                for day in range(9)
            ],
        ]

        meta_key = "meta.csv"
        meta_content = (
            f"{MetaProcessFormat.SOURCE_DATE_COL.value},"
            f"{MetaProcessFormat.PROCESS_COL.value}\n"
            f"{self.dates[3]},{self.dates[0]}\n"
            f"{self.dates[4]},{self.dates[0]}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date_list = [self.dates[1], self.dates[4], self.dates[7]]

        for count, first_date in enumerate(first_date_list):
            min_date_return, date_list_return = MetaProcess.return_date_list(
                self.s3_bucket_conn, meta_key, first_date
            )
            self.assertEqual(set(date_list_exp[count]), set(date_list_return))
            self.assertEqual(min_date_exp[count], min_date_return)

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": meta_key}]})

    def test_return_date_list_meta_file_wrong(self):
        meta_key = "meta.csv"
        meta_content = (
            f"wrong_column,{MetaProcessFormat.PROCESS_COL.value}\n"
            f"{self.dates[3]},{self.dates[0]}\n"
            f"{self.dates[4]},{self.dates[0]}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[1]

        with self.assertRaises(KeyError):
            MetaProcess.return_date_list(self.s3_bucket_conn, meta_key, first_date)

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": meta_key}]})

    def test_return_date_list_empty_date_list(self):
        min_date_exp = "2200-01-01"
        date_list_exp = []

        meta_key = "meta.csv"
        meta_content = (
            f"{MetaProcessFormat.SOURCE_DATE_COL.value},"
            f"{MetaProcessFormat.PROCESS_COL.value}\n"
            f"{self.dates[0]},{self.dates[0]}\n"
            f"{self.dates[1]},{self.dates[0]}"
        )
        self.s3_bucket.put_object(Body=meta_content, Key=meta_key)
        first_date = self.dates[0]

        min_date_return, date_list_return = MetaProcess.return_date_list(
            self.s3_bucket_conn, meta_key, first_date
        )

        self.assertEqual(date_list_exp, date_list_return)
        self.assertEqual(min_date_exp, min_date_return)

        self.s3_bucket.delete_objects(Delete={"Objects": [{"Key": meta_key}]})


if __name__ == "__main__":
    unittest.main()
