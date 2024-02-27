from datetime import datetime
import logging
import pandas as pd
from typing import NamedTuple, List

from ..common.s3 import S3BucketConnector
from ..common.meta_process import MetaProcess


class XetraSourceConfig(NamedTuple):
    first_extract_date: str
    columns: List[str]
    col_date: str
    col_isin: str
    col_time: str
    col_start_price: str
    col_min_price: str
    col_max_price: str
    col_traded_volume: str


class XetraTargetConfig(NamedTuple):
    col_isin: str
    col_date: str
    col_opening_price: str
    col_closing_price: str
    col_min_price: str
    col_max_price: str
    col_daily_trading_volume: str
    col_change_previous_closing: str
    key: str
    key_date_format: str
    format: str


class XetraETL:
    def __init__(
        self,
        s3_bucket_src: S3BucketConnector,
        s3_bucket_trg: S3BucketConnector,
        meta_key: str,
        src_args: XetraSourceConfig,
        trg_args: XetraTargetConfig,
    ) -> None:
        self._logger = logging.getLogger(__name__)
        self.s3_bucket_src = s3_bucket_src
        self.s3_bucket_trg = s3_bucket_trg
        self.meta_key = meta_key
        self.src_args = src_args
        self.trg_args = trg_args
        self.extract_date, self.extract_date_list = MetaProcess.return_date_list(
            self.s3_bucket_trg, self.meta_key, self.src_args.first_extract_date
        )
        self.meta_update_list = [
            date for date in self.extract_date_list if date >= self.extract_date
        ]

    def extract(self) -> pd.DataFrame:
        files = [
            key
            for date in self.extract_date_list
            for key in self.s3_bucket_src.list_files_in_prefix(date)
        ]
        self._logger.info("Extracting Xetra source files started...")
        df_list = []
        for file in files:
            df = self.s3_bucket_src.read_csv_to_df(file)
            if not df.empty:
                df_list.append(df)
        if not df_list:
            df = pd.DataFrame()
        else:
            df = pd.concat(df_list, ignore_index=True)
        self._logger.info("Extracting Xetra source files finished.")
        return df

    def transform_report1(self, df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            self._logger.info(
                "The dataframe is empty. No transformations will be applied."
            )
            return df

        self._logger.info(
            "Applying transformations to Xetra source data for report 1 started..."
        )

        df = df.loc[:, self.src_args.columns]
        df[self.trg_args.col_opening_price] = (
            df.sort_values(by=[self.src_args.col_time])
            .groupby([self.src_args.col_isin, self.src_args.col_date])[
                self.src_args.col_start_price
            ]
            .transform("first")
        )
        df[self.trg_args.col_closing_price] = (
            df.sort_values(by=[self.src_args.col_time])
            .groupby([self.src_args.col_isin, self.src_args.col_date])[
                self.src_args.col_start_price
            ]
            .transform("last")
        )
        df.rename(
            columns={
                self.src_args.col_min_price: self.trg_args.col_min_price,
                self.src_args.col_max_price: self.trg_args.col_max_price,
                self.src_args.col_traded_volume: self.trg_args.col_daily_trading_volume,
            },
            inplace=True,
        )
        df = df.groupby(
            [self.src_args.col_isin, self.src_args.col_date], as_index=False
        ).agg(
            {
                self.trg_args.col_opening_price: "min",
                self.trg_args.col_closing_price: "min",
                self.trg_args.col_min_price: "min",
                self.trg_args.col_max_price: "max",
                self.trg_args.col_daily_trading_volume: "sum",
            }
        )
        df[self.trg_args.col_change_previous_closing] = (
            df.sort_values(by=[self.trg_args.col_date])
            .groupby([self.trg_args.col_isin])[self.trg_args.col_opening_price]
            .shift(1)
        )
        df[self.trg_args.col_change_previous_closing] = (
            (
                df[self.trg_args.col_opening_price]
                - df[self.trg_args.col_change_previous_closing]
            )
            / df[self.trg_args.col_change_previous_closing]
            * 100
        )
        df = df.round(decimals=2)
        df = df[df.Date >= self.extract_date].reset_index(drop=True)
        self._logger.info("Applying transformations to Xetra source data finished.")
        return df

    def load(self, df: pd.DataFrame) -> None:
        key = "{}_{}.{}".format(
            self.trg_args.key,
            datetime.today().strftime(self.trg_args.key_date_format),
            self.trg_args.format,
        )
        self.s3_bucket_trg.write_df_to_s3(df, key, self.trg_args.format)
        self._logger.info("Xetra target data successfully written.")

        MetaProcess.update_meta_file(
            self.s3_bucket_trg, self.meta_key, self.meta_update_list
        )
        self._logger.info("Xetra meta file successfully updated.")

    def etl_report1(self) -> None:
        df = self.extract()
        df = self.transform_report1(df)
        self.load(df)
