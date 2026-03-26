import logging
import os

from ouro.config import Config

logger: logging.Logger = logging.getLogger("ouro")
httpx_logger: logging.Logger = logging.getLogger("httpx")
httpcore_logger: logging.Logger = logging.getLogger("httpcore")


def _http_traffic_verbose() -> bool:
    return os.getenv("OURO_HTTP_LOG", "").strip().lower() in ("1", "true", "yes")


def _basic_config() -> None:
    # e.g. [2023-10-05 14:12:26 - ouro._base_client:818 - DEBUG] HTTP Request: POST http://127.0.0.1:4010/foo/bar "200 OK"
    logging.basicConfig(
        format="[%(asctime)s - %(name)s:%(lineno)d - %(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def setup_logging() -> None:
    debug = Config.DEBUG
    http_verbose = _http_traffic_verbose()
    if debug:
        _basic_config()
        logger.setLevel(logging.DEBUG)
        httpx_logger.setLevel(logging.DEBUG)
        httpcore_logger.setLevel(logging.DEBUG)
    else:
        _basic_config()
        logger.setLevel(logging.INFO)
        if http_verbose:
            httpx_logger.setLevel(logging.INFO)
            httpcore_logger.setLevel(logging.INFO)
        else:
            # Embedded clients (MCP, tests) rarely want per-request httpx lines on stderr.
            httpx_logger.setLevel(logging.WARNING)
            httpcore_logger.setLevel(logging.WARNING)
