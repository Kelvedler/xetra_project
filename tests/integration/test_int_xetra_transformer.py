from datetime import datetime, timedelta
from io import BytesIO
import unittest

import boto3
import pandas as pd
from src.common.constants import MetaProcessFormat

from src.common.s3 import S3BucketConnector
from src.transformers.xetra_transformer import (
    XetraETL,
    XetraSourceConfig,
    XetraTargetConfig,
)


class IntTextXetraETLMethods(unittest.TestCase):
    def setUp(self) -> None:
        self.s3_endpoint_url = "https://s3.eu-central-1.amazonaws.com"
        self.s3_bucket_name_src = "xetra-kelvedler-int-test-src"
        self.s3_bucket_name_trg = "xetra-kelvedler-int-test-trg"
        self.meta_key = "meta_file.csv"
        self.s3 = boto3.resource("s3", endpoint_url=self.s3_endpoint_url)
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
        self.today = datetime(year=2022, month=3, day=20).date()
        self.dates = [
            (self.today - timedelta(days=day)).strftime(
                MetaProcessFormat.DATE_FORMAT.value
            )
            for day in range(8)
        ]
        conf_dict_src = {
            "first_extract_date": self.dates[3],
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
                self.dates[5],
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
                self.dates[4],
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
                self.dates[3],
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
                self.dates[3],
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
                self.dates[2],
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
                self.dates[2],
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
                self.dates[1],
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
                self.dates[1],
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
                self.dates[1],
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
            self.df_src.loc[0:0],
            f"{self.dates[5]}/{self.dates[5]}_BINS_XETR12.csv",
            "csv",
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[1:1],
            f"{self.dates[4]}/{self.dates[4]}_BINS_XETR15.csv",
            "csv",
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[2:2],
            f"{self.dates[3]}/{self.dates[3]}_BINS_XETR13.csv",
            "csv",
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[3:3],
            f"{self.dates[3]}/{self.dates[3]}_BINS_XETR14.csv",
            "csv",
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[4:4],
            f"{self.dates[2]}/{self.dates[2]}_BINS_XETR07.csv",
            "csv",
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[5:5],
            f"{self.dates[2]}/{self.dates[2]}_BINS_XETR08.csv",
            "csv",
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[6:6],
            f"{self.dates[1]}/{self.dates[1]}_BINS_XETR07.csv",
            "csv",
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[7:7],
            f"{self.dates[1]}/{self.dates[1]}_BINS_XETR08.csv",
            "csv",
        )
        self.s3_bucket_src.write_df_to_s3(
            self.df_src.loc[8:8],
            f"{self.dates[1]}/{self.dates[1]}_BINS_XETR09.csv",
            "csv",
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
            ["AT0000A0E9W5", self.dates[3], 20.21, 18.27, 18.21, 21.34, 1088, 10.62],
            ["AT0000A0E9W5", self.dates[2], 20.58, 19.27, 18.89, 21.14, 10286, 1.83],
            ["AT0000A0E9W5", self.dates[1], 23.58, 24.22, 22.21, 25.01, 3586, 14.58],
        ]
        self.df_report = pd.DataFrame(data_report, columns=columns_report)

    def tearDown(self) -> None:
        for key in self.src_bucket.objects.all():
            key.delete()
        for key in self.trg_bucket.objects.all():
            key.delete()

    def test_int_etl_report1_no_metafile(self):
        df_exp = self.df_report
        meta_exp = [self.dates[3], self.dates[2], self.dates[1], self.dates[0]]

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


if __name__ == "__main__":
    unittest.main()
