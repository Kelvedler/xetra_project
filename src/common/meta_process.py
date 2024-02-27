import collections
from datetime import datetime, timedelta
from typing import List, Tuple
from botocore.exceptions import ClientError

import pandas as pd

from .constants import MetaProcessFormat, S3FileTypes
from .custom_exceptions import WrongMetaFileException
from .s3 import S3BucketConnector


class MetaProcess:
    @staticmethod
    def get_code_from_client_error(err: ClientError) -> str:
        error_resp = err.response.get("Error")
        if not error_resp:
            return ""
        code = error_resp.get("Code")
        return code if code else ""

    @classmethod
    def update_meta_file(
        cls,
        bucket_connector: S3BucketConnector,
        meta_key: str,
        extract_date_list: List[str],
    ) -> None:
        df_new = pd.DataFrame(
            columns=[
                MetaProcessFormat.SOURCE_DATE_COL.value,
                MetaProcessFormat.PROCESS_COL.value,
            ]
        )
        df_new[MetaProcessFormat.SOURCE_DATE_COL.value] = extract_date_list
        df_new[MetaProcessFormat.PROCESS_COL.value] = datetime.today().strftime(
            MetaProcessFormat.PROCESS_DATE_FORMAT.value
        )
        try:
            df_old = bucket_connector.read_csv_to_df(meta_key)
            if collections.Counter(df_old.columns) != collections.Counter(
                df_new.columns
            ):
                raise WrongMetaFileException
            df_all = pd.concat([df_old, df_new])
        except ClientError as e:
            if cls.get_code_from_client_error(e) == "NoSuchKey":
                df_all = df_new
            else:
                raise
        bucket_connector.write_df_to_s3(df_all, meta_key, S3FileTypes.CSV.value)

    @classmethod
    def return_date_list(
        cls, bucket_connector: S3BucketConnector, meta_key: str, first_date: str
    ) -> Tuple[str, List[str]]:
        start = datetime.strptime(
            first_date, MetaProcessFormat.DATE_FORMAT.value
        ).date() - timedelta(days=1)
        end = datetime(year=2022, month=3, day=20).date()
        try:
            df_meta = bucket_connector.read_csv_to_df(meta_key)
            dates = [
                (start + timedelta(days=x)) for x in range(0, (end - start).days + 1)
            ]
            src_dates = set(
                pd.to_datetime(df_meta[MetaProcessFormat.SOURCE_DATE_COL.value]).dt.date
            )
            dates_missing = set(dates[1:]) - src_dates
            if dates_missing:
                min_date = min(dates_missing) - timedelta(days=1)
                return_min_date = (min_date + timedelta(days=1)).strftime(
                    MetaProcessFormat.DATE_FORMAT.value
                )
                return_dates = [
                    date.strftime(MetaProcessFormat.DATE_FORMAT.value)
                    for date in dates
                    if date >= min_date
                ]
            else:
                return_dates = []
                return_min_date = datetime(year=2200, month=1, day=1).strftime(
                    MetaProcessFormat.DATE_FORMAT.value
                )
        except ClientError as e:
            if cls.get_code_from_client_error(e) == "NoSuchKey":
                return_dates = [
                    (start + timedelta(days=d)).strftime(
                        MetaProcessFormat.DATE_FORMAT.value
                    )
                    for d in range(0, (end - start).days + 1)
                ]
                return_min_date = first_date
            else:
                raise
        return return_min_date, return_dates
