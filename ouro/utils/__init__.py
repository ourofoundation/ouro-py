import uuid as _uuid

from .content import description_to_markdown, tiptap_to_markdown
from .openapi import get_custom_openapi, ouro_execution_mode, ouro_field

__all__ = [
    "ouro_field",
    "ouro_execution_mode",
    "get_custom_openapi",
    "generate_uuid",
    "is_valid_uuid",
    "tiptap_to_markdown",
    "description_to_markdown",
]


_uuid7 = getattr(_uuid, "uuid7", None)


def generate_uuid() -> str:
    """Return a new UUID string, preferring v7 (time-ordered) when available."""
    return str(_uuid7() if _uuid7 is not None else _uuid.uuid4())


def is_valid_uuid(uuid_string: str) -> bool:
    try:
        uuid_obj = _uuid.UUID(uuid_string)
        return str(uuid_obj) == uuid_string
    except ValueError:
        return False
