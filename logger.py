"""set up logging"""
import os
import logging
import sys
from logging.handlers import RotatingFileHandler

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("prefect-proxy")


def setup_logger():
    """set up the global logger"""
    logdir = os.getenv("LOGDIR")
    if not os.path.exists(logdir):
        os.makedirs(logdir)

    logger.setLevel(logging.INFO)

    # log to file
    logfilename = f"{logdir}/prefect-proxy.log"
    handler = RotatingFileHandler(logfilename, maxBytes=1048576, backupCount=5)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] {%(filename)s -> %(funcName)s} [%(message)s] [orgname: %(orgname)s] [caller_name: %(caller_name)s]"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # log to stdout
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] {%(filename)s -> %(funcName)s} [%(message)s] [orgname: %(orgname)s] [caller_name: %(caller_name)s]"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
