from enum import Enum


class S3FileTypes(Enum):
    CSV = "csv"
    PARQUET = "parquet"


class MetaProcessFormat(Enum):
    DATE_FORMAT = "%Y-%m-%d"
    PROCESS_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    SOURCE_DATE_COL = "source_date"
    PROCESS_COL = "datetime_of_processing"
    FILE_FORMAT = "csv"
