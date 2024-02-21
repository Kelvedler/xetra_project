import logging
from typing import NamedTuple, List

from common.s3 import S3BucketConnector


class XetraSourceConfig(NamedTuple):
    first_extract_date: str
    columns: List[str]
    col_date: str
    col_isin: str
    col_time: str
    col_start_price: str
    col_min_price: str
    col_max_price: str
    col_traded_vol: str


class XetraTargetConfig(NamedTuple):
    col_isin: str
    col_date: str
    col_opening_price: str
    col_closing_price: str
    col_min_price: str
    col_max_price: str
    col_daily_trading_vol: str
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

    def extract(self):
        pass

    def transform_report1(self):
        pass

    def load(self):
        pass

    def etl_report1(self):
        pass
