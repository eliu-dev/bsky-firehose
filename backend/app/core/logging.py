import logging
import logging.handlers
from string import Formatter
import sys
from pathlib import Path
import os
from dotenv import load_dotenv

root = Path(__file__).parent.parent.parent
print(root)
load_dotenv(root / '.env.dev')
ENVIRONMENT = os.getenv('ENVIRONMENT')
if ENVIRONMENT is None:
    raise ValueError(f'Missing env variable: ENVIRONMENT')

def setup_local_logging() -> None:
    root: logging.Logger = logging.getLogger()
    root.setLevel(logging.DEBUG)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s %(name)s: %(message)s'))
    root.addHandler(stdout_handler)

    log_file: Path = Path(__file__).parent.parent.parent / 'logs' / f'app_{ENVIRONMENT}.log'
    file_handler = logging.handlers.RotatingFileHandler(
        filename= log_file,
        maxBytes=int(25*(10**6)),
        backupCount=10,
    )
    file_handler.setFormatter(logging.Formatter('[%(levelname)s] %(name)s %(asctime)s: %(message)s'))
    root.addHandler(file_handler)

def setup_prod_logging() -> None:
    root: logging.Logger = logging.getLogger()
    root.setLevel(logging.INFO)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(logging.Formatter('{"level": [%(levelname)s], "timestamp": %(asctime)s, "name": %(name)s, "message": %(message)s'))
    root.addHandler(stdout_handler)