from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict

__all__ = ["DictCompatModel"]


class DictCompatModel(BaseModel):
    """Pydantic base for resource models that also behave like dicts on reads.

    Rationale: several resource methods historically returned raw ``dict`` and
    callers use ``model.get("key")`` / ``model["key"]`` access patterns (see
    ``ouro-mcp`` and ``ouro-agents``). Switching to Pydantic for typing wins
    without breaking those callers means supporting dict-style reads as a
    transitional affordance. New code should prefer attribute access
    (``model.key``) and typed fields.

    ``extra="allow"`` keeps us forward-compatible with new backend fields —
    they land in ``model_extra`` and remain reachable via dict access.
    """

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    def __getitem__(self, key: str) -> Any:
        if key in self.__class__.model_fields:
            return getattr(self, key)
        extra = self.__pydantic_extra__ or {}
        if key in extra:
            return extra[key]
        raise KeyError(key)

    def __contains__(self, key: object) -> bool:
        if not isinstance(key, str):
            return False
        if key in self.__class__.model_fields:
            return getattr(self, key, None) is not None or True
        extra = self.__pydantic_extra__ or {}
        return key in extra

    def get(self, key: str, default: Any = None) -> Any:
        """Dict-compatible ``.get`` for transitional back-compat.

        Returns the attribute value if the field is declared, otherwise the
        value from ``model_extra``, otherwise ``default``.
        """
        if key in self.__class__.model_fields:
            return getattr(self, key, default)
        extra = self.__pydantic_extra__ or {}
        return extra.get(key, default)
