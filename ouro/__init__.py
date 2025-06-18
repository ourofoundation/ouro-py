from __future__ import annotations

# from ._models import BaseModel
from ouro.__version__ import __title__, __version__
from ouro.client import Ouro
from ouro.config import Config
from typing_extensions import override

# from ._response import APIResponse as APIResponse, AsyncAPIResponse as AsyncAPIResponse
from ._constants import DEFAULT_CONNECTION_LIMITS, DEFAULT_MAX_RETRIES, DEFAULT_TIMEOUT
from ._exceptions import (
    APIConnectionError,
    APIError,
    APIResponseValidationError,
    APIStatusError,
    APITimeoutError,
    AuthenticationError,
    BadRequestError,
    ConflictError,
    InternalServerError,
    NotFoundError,
    OuroError,
    PermissionDeniedError,
    RateLimitError,
    UnprocessableEntityError,
)
from ._logs import setup_logging
from ._types import NOT_GIVEN, NoneType, ProxiesTypes, Transport

# from ._base_client import DefaultHttpxClient, DefaultAsyncHttpxClient
# from ._utils._logs import setup_logging as _setup_logging

__all__ = [
    # "types",
    "__version__",
    "__title__",
    # "NoneType",
    # "Transport",
    # "ProxiesTypes",
    # "NotGiven",
    # "NOT_GIVEN",
    # "OuroError",
    # "APIError",
    # "APIStatusError",
    # "APITimeoutError",
    # "APIConnectionError",
    # "APIResponseValidationError",
    # "BadRequestError",
    # "AuthenticationError",
    # "PermissionDeniedError",
    # "NotFoundError",
    # "ConflictError",
    # "UnprocessableEntityError",
    # "RateLimitError",
    # "InternalServerError",
    # "Timeout",
    # "RequestOptions",
    # "Client",
    "Ouro",
    # "DEFAULT_TIMEOUT",
    # "DEFAULT_MAX_RETRIES",
    # "DEFAULT_CONNECTION_LIMITS",
]

setup_logging()

# Update the __module__ attribute for exported symbols so that
# error messages point to this module instead of the module
# it was originally defined in, e.g.
# ouro._exceptions.NotFoundError -> ouro.NotFoundError
__locals = locals()
for __name in __all__:
    if not __name.startswith("__"):
        try:
            __locals[__name].__module__ = "ouro"
        except (TypeError, AttributeError):
            # Some of our exported symbols are builtins which we can't set attributes for.
            pass
