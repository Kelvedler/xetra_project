import logging
import logging.config
from pathlib import Path
import yaml

from src.common.s3 import S3BucketConnector
from src.transformers.xetra_transformer import (
    XetraETL,
    XetraSourceConfig,
    XetraTargetConfig,
)


def main():
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "configs/xetra_report1_config.yml"
    config = yaml.safe_load(open(config_path))

    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)

    s3_config = config["s3"]
    s3_bucket_src = S3BucketConnector(
        endpoint_url=s3_config["src_endpoint_url"], bucket=s3_config["src_bucket"]
    )
    s3_bucket_trg = S3BucketConnector(
        endpoint_url=s3_config["trg_endpoint_url"], bucket=s3_config["trg_bucket"]
    )

    source_config = XetraSourceConfig(**config["source"])
    target_config = XetraTargetConfig(**config["target"])
    meta_config = config["meta"]

    logger.info("Starting Xetra ETL job...")
    xetra_etl = XetraETL(
        s3_bucket_src=s3_bucket_src,
        s3_bucket_trg=s3_bucket_trg,
        meta_key=meta_config["key"],
        src_args=source_config,
        trg_args=target_config,
    )
    xetra_etl.etl_report1()
    logger.info("Xetra ETL job finished.")


if __name__ == "__main__":
    main()
