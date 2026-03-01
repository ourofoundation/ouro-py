from uuid import UUID

from .content import description_to_markdown, tiptap_to_markdown
from .openapi import get_custom_openapi, ouro_field

__all__ = [
    "ouro_field",
    "get_custom_openapi",
    "is_valid_uuid",
    "tiptap_to_markdown",
    "description_to_markdown",
]


def is_valid_uuid(uuid_string: str) -> bool:
    try:
        # Attempt to create a UUID object
        uuid_obj = UUID(uuid_string)
        return str(uuid_obj) == uuid_string
    except ValueError:
        return False
