import logging
import sys
from logging.handlers import RotatingFileHandler

LOGFILE = "logs/prefect-proxy.log"
logger = logging.getLogger("prefect-proxy")
logger.setLevel(logging.INFO)

handler = RotatingFileHandler(LOGFILE)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] {%(filename)s -> %(funcName)s} [%(message)s]"
)
handler.setFormatter(formatter)
logger.addHandler(handler)
