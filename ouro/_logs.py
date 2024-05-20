import logging
import os

from ouro.config import Config

logger: logging.Logger = logging.getLogger("ouro")
httpx_logger: logging.Logger = logging.getLogger("httpx")


def _basic_config() -> None:
    # e.g. [2023-10-05 14:12:26 - ouro._base_client:818 - DEBUG] HTTP Request: POST http://127.0.0.1:4010/foo/bar "200 OK"
    logging.basicConfig(
        format="[%(asctime)s - %(name)s:%(lineno)d - %(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def setup_logging() -> None:
    debug = Config.DEBUG
    if debug:
        _basic_config()
        logger.setLevel(logging.DEBUG)
        httpx_logger.setLevel(logging.DEBUG)
    else:
        _basic_config()
        logger.setLevel(logging.INFO)
        httpx_logger.setLevel(logging.INFO)
