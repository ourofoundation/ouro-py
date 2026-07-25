from importlib.metadata import PackageNotFoundError, version

__title__ = "ouro"

try:
    __version__ = version("ouro-py")
except PackageNotFoundError:  # pragma: no cover
    __version__ = "0.0.0"
