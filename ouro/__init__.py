from __future__ import annotations

from ouro.__version__ import __title__, __version__

from .utils.plotly import build_plotly_asset_tags, inject_assets_into_html

__all__ = [
    "__version__",
    "__title__",
    "build_plotly_asset_tags",
    "inject_assets_into_html",
]


def __getattr__(name: str):
    if name == "Ouro":
        from .client import Ouro as _Ouro

        return _Ouro
    raise AttributeError(f"module 'ouro' has no attribute {name!r}")
