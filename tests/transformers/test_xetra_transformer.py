from io import BytesIO
import os
import unittest
from unittest.mock import patch

import boto3
from moto import mock_aws
import pandas as pd

from src.common.meta_process import MetaProcess
from src.common.s3 import S3BucketConnector
from src.transformers.xetra_transformer import (
    XetraETL,
    XetraSourceConfig,
    XetraTargetConfig,
)


class TextXetraETLMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.mock = mock_aws()
        self.mock.start()
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name_src = "src-bucket"
        self.s3_bucket_name_trg = "trg-bucket"
        self.meta_key = "meta_key"
        self.s3 = boto3.resource("s3", endpoint_url=self.s3_endpoint_url)
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name_src,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        self.s3.create_bucket(
            Bucket=self.s3_bucket_name_trg,
            CreateBucketConfiguration={"LocationConstraint": "eu-central-1"},
        )
        self.src_bucket = self.s3.Bucket(self.s3_bucket_name_src)
        self.trg_bucket = self.s3.Bucket(self.s3_bucket_name_trg)
        self.s3_bucket_src = S3BucketConnector(
            self.s3_endpoint_url,
            self.s3_bucket_name_src,
        )
        self.s3_bucket_trg = S3BucketConnector(
            self.s3_endpoint_url,
            self.s3_bucket_name_trg,
        )
        columns_src = [
            "ISIN",
            "Mnemonic",
            "Date",
            "Time",
            "StartPrice",
            "EndPrice",
            "MinPrice",
            "MaxPrice",
            "TradedVolume",
        ]
        conf_dict_src = {
            "first_extract_date": "2021-04-01",
            "columns": columns_src,
            "col_date": "Date",
            "col_isin": "ISIN",
            "col_time": "Time",
            "col_start_price": "StartPrice",
            "col_min_price": "MinPrice",
            "col_max_price": "MaxPrice",
            "col_traded_volume": "TradedVolume",
        }
        conf_dict_trg = {
            "col_isin": "ISIN",
            "col_date": "Date",
            "col_opening_price": "OpeningPriceEur",
            "col_closing_price": "ClosingPriceEur",
            "col_min_price": "MinimumPriceEur",
            "col_max_price": "MaximumPriceEur",
            "col_daily_trading_volume": "DailyTradedVolume",
            "col_change_previous_closing": "ChangePrevClosing%",
            "key": "report1/xetra_daily_report",
            "key_date_format": "%Y-%m-%d %H:%M:%S",
            "format": "parquet",
        }
        self.source_config = XetraSourceConfig(**conf_dict_src)
        self.target_config = XetraTargetConfig(**conf_dict_trg)

        data = [
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-15",
                "12:00",
                20.19,
                18.45,
                18.20,
                20.33,
                877,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-16",
                "15:00",
                18.27,
                21.19,
                18.27,
                21.34,
                987,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-17",
                "13:00",
                20.21,
                18.27,
                18.21,
                20.42,
                633,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-17",
                "14:00",
                18.27,
                21.19,
                18.27,
                21.34,
                455,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-18",
                "07:00",
                20.58,
                19.21,
                18.89,
                20.58,
                9066,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-18",
                "08:00",
                19.27,
                21.14,
                19.27,
                21.14,
                1220,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-19",
                "07:00",
                23.58,
                23.58,
                23.58,
                23.58,
                1035,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-19",
                "08:00",
                23.58,
                24.22,
                23.31,
                24.34,
                1028,
            ],
            [
                "AT0000A0E9W5",
                "SANT",
                "2021-04-19",
                "09:00",
                24.22,
                22.21,
                22.21,
                25.01,
                1523,
            ],
        ]
        self.df_src = pd.DataFrame(data, columns=columns_src)
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[0:0], "2021-04-15/2021-04-15_BINS_XETR12.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[1:1], "2021-04-16/2021-04-16_BINS_XETR15.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[2:2], "2021-04-17/2021-04-17_BINS_XETR13.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[3:3], "2021-04-17/2021-04-17_BINS_XETR14.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[4:4], "2021-04-18/2021-04-18_BINS_XETR07.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[5:5], "2021-04-18/2021-04-18_BINS_XETR08.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[6:6], "2021-04-19/2021-04-19_BINS_XETR07.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[7:7], "2021-04-19/2021-04-19_BINS_XETR08.csv", "csv"
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[8:8], "2021-04-19/2021-04-19_BINS_XETR09.csv", "csv"
        )
        columns_report = [
            "ISIN",
            "Date",
            "OpeningPriceEur",
            "ClosingPriceEur",
            "MinimumPriceEur",
            "MaximumPriceEur",
            "DailyTradedVolume",
            "ChangePrevClosing%",
        ]
        data_report = [
            ["AT0000A0E9W5", "2021-04-17", 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
            ["AT0000A0E9W5", "2021-04-18", 20.58, 19.27, 18.89, 21.14, 10286, 1.83],
            ["AT0000A0E9W5", "2021-04-19", 23.58, 24.22, 22.21, 25.01, 3586, 14.58],
        ]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self) -> None:
        self.mock.stop()

    def test_extract_no_files(self):
        extract_date = "2200-01-02"
        extract_date_list = []

        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            df_return = xetra_etl.extract()
        self.assertTrue(df_return.empty)

    def test_extract_files(self):
        df_exp = self.df_src.loc[1:8].reset_index(drop=True)

        extract_date = "2021-04-17"
        extract_date_list = [
            "2021-04-16",
            "2021-04-17",
            "2021-04-18",
            "2021-04-19",
            "2021-04-20",
        ]

        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            df_result = xetra_etl.extract()
        self.assertTrue((df_exp.equals(df_result)))

    def test_transform_report1_empty_df(self):
        log_exp = "The dataframe is empty. No transformations will be applied."

        extract_date = "2021-04-17"
        extract_date_list = ["2021-04-16", "2021-04-17", "2021-04-18"]
        df_input = pd.DataFrame()

        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            with self.assertLogs() as logm:
                df_result = xetra_etl.transform_report1(df_input)
                self.assertIn(log_exp, logm.output[0])

            self.assertTrue(df_result.empty)

    def test_transform_report1_ok(self):
        log1_exp = (
            "Applying transformations to Xetra source data for report 1 started..."
        )
        log2_exp = "Applying transformations to Xetra source data finished."
        df_exp = self.df_report

        extract_date = "2021-04-17"
        extract_date_list = ["2021-04-16", "2021-04-17", "2021-04-18", "2021-04-19"]
        df_input = self.df_src.loc[1:8].reset_index(drop=True)

        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            with self.assertLogs() as logm:
                df_result = xetra_etl.transform_report1(df_input)

                self.assertIn(log1_exp, logm.output[0])
                self.assertIn(log2_exp, logm.output[1])

            self.assertTrue(df_exp.equals(df_result))

    def test_load(self):
        log1_exp = "Xetra target data successfully written."
        log2_exp = "Xetra meta file successfully updated."
        df_exp = self.df_report
        meta_exp = ["2021-04-17", "2021-04-18", "2021-04-19"]

        extract_date = "2021-04-17"
        extract_date_list = ["2021-04-16", "2021-04-17", "2021-04-18", "2021-04-19"]
        df_input = self.df_report

        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            with self.assertLogs() as logm:
                xetra_etl.load(df_input)

                self.assertIn(log1_exp, logm.output[1])
                self.assertIn(log2_exp, logm.output[4])

        trg_file = self.s3_bucket_trg.list_files_in_prefix(self.target_config.key)[0]
        data = self.trg_bucket.Object(key=trg_file).get().get("Body").read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_bucket_trg.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result["source_date"]), meta_exp)

        self.trg_bucket.delete_objects(
            Delete={"Objects": [{"Key": trg_file}, {"Key": self.meta_key}]}
        )

    def test_etl_report1(self):
        df_exp = self.df_report
        meta_exp = ["2021-04-17", "2021-04-18", "2021-04-19"]

        extract_date = "2021-04-17"
        extract_date_list = ["2021-04-16", "2021-04-17", "2021-04-18", "2021-04-19"]

        with patch.object(
            MetaProcess,
            "return_date_list",
            return_value=[extract_date, extract_date_list],
        ):
            xetra_etl = XetraETL(
                self.s3_bucket_src,
                self.s3_bucket_trg,
                self.meta_key,
                self.source_config,
                self.target_config,
            )
            xetra_etl.etl_report1()

        trg_file = self.s3_bucket_trg.list_files_in_prefix(self.target_config.key)[0]
        data = self.trg_bucket.Object(key=trg_file).get().get("Body").read()
        out_buffer = BytesIO(data)
        df_result = pd.read_parquet(out_buffer)
        self.assertTrue(df_exp.equals(df_result))
        meta_file = self.s3_bucket_trg.list_files_in_prefix(self.meta_key)[0]
        df_meta_result = self.s3_bucket_trg.read_csv_to_df(meta_file)
        self.assertEqual(list(df_meta_result["source_date"]), meta_exp)

        self.trg_bucket.delete_objects(
            Delete={"Objects": [{"Key": trg_file}, {"Key": self.meta_key}]}
        )


if __name__ == "__main__":
    unittest.main()
