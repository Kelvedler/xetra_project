import os
import boto3
from io import StringIO, BytesIO
import logging
import pandas as pd
from typing import List

from .constants import S3FileTypes
from .custom_exceptions import WrongFormatException


class S3BucketConnector:
    def __init__(self, endpoint_url: str, bucket: str) -> None:
        self._logger = logging.getLogger(__name__)
        self.endpoint_url = endpoint_url
        self.role_credentials = boto3.client("sts").assume_role(
            RoleArn=os.getenv("ROLE_ARN"),
            RoleSessionName="XetraRunnerSession",
        )["Credentials"]

        self._s3 = boto3.resource(
            service_name="s3",
            endpoint_url=endpoint_url,
            aws_access_key_id=self.role_credentials["AccessKeyId"],
            aws_secret_access_key=self.role_credentials["SecretAccessKey"],
            aws_session_token=self.role_credentials["SessionToken"],
        )
        self._bucket = self._s3.Bucket(bucket)

    def list_files_in_prefix(self, prefix: str) -> List[str]:
        files = [obj.key for obj in self._bucket.objects.filter(Prefix=prefix)]
        return files

    def read_csv_to_df(
        self, key: str, encoding: str = "utf-8", delimeter: str = ","
    ) -> pd.DataFrame:
        self._logger.info(f"Reading file {self.endpoint_url}/{self._bucket.name}/{key}")
        csv_obj = self._bucket.Object(key=key).get().get("Body").read().decode(encoding)
        data = StringIO(csv_obj)
        df = pd.read_csv(data, delimiter=delimeter)
        return df

    def write_df_to_s3(
        self,
        df: pd.DataFrame,
        key: str,
        ext: str,
    ) -> None:
        if df.empty:
            self._logger.info("The dataframe is empty! No file will be written!")
            return
        out_buffer = BytesIO()
        if ext == S3FileTypes.PARQUET.value:
            df.to_parquet(out_buffer, index=False)
        elif ext == S3FileTypes.CSV.value:
            df.to_csv(out_buffer, index=False)
        else:
            self._logger.warn(
                f"The file format {ext} is not supported to be written to s3!"
            )
            raise WrongFormatException
        self._logger.info(
            f"Writing file to {self.endpoint_url}/{self._bucket.name}/{key}"
        )
        self._bucket.put_object(Body=out_buffer.getvalue(), Key=key)
