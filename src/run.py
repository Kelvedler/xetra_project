import logging
import logging.config
from pathlib import Path
import yaml


def main():
    base_dir = Path(__file__).resolve().parent.parent
    config_path = base_dir / "configs/xetra_report1_config.yml"
    config = yaml.safe_load(open(config_path))
    log_config = config["logging"]
    logging.config.dictConfig(log_config)
    logger = logging.getLogger(__name__)


if __name__ == "__main__":
    main()
