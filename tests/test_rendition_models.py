from datetime import datetime, timezone
from uuid import uuid4

from pydantic import TypeAdapter

from ouro.models import (
    AssetRenditionRecord,
    LanguageToolsPreferences,
    Preferences,
    SpeechAssetRendition,
    TranslationAssetRendition,
)


def _base_rendition() -> dict:
    now = datetime.now(timezone.utc).isoformat()
    return {
        "id": uuid4(),
        "asset_id": uuid4(),
        "route_id": uuid4(),
        "provider_id": uuid4(),
        "status": "success",
        "created_at": now,
        "last_updated": now,
    }


def test_translation_rendition_parses_structured_content() -> None:
    rendition = TypeAdapter(AssetRenditionRecord).validate_python(
        {
            **_base_rendition(),
            "kind": "translation",
            "source_language": "en",
            "target_language": "es",
            "content": {
                "json": {"type": "doc", "content": []},
                "text": "Hola",
            },
        }
    )

    assert isinstance(rendition, TranslationAssetRendition)
    assert rendition.content is not None
    assert rendition.content.text == "Hola"


def test_pending_speech_rendition_does_not_require_audio() -> None:
    rendition = TypeAdapter(AssetRenditionRecord).validate_python(
        {
            **_base_rendition(),
            "kind": "speech",
            "status": "in-progress",
            "language": "en-US",
            "voice": "alloy",
        }
    )

    assert isinstance(rendition, SpeechAssetRendition)
    assert rendition.audio is None


def test_preferences_parse_language_tool_configuration() -> None:
    route_id = uuid4()
    preferences = Preferences(
        id=uuid4(),
        user_id=uuid4(),
        language_tools={
            "language": "es-MX",
            "translation": {
                "route_id": route_id,
                "options": {"formality": "informal"},
            },
            "speech": {
                "route_id": None,
                "voice": "alloy",
                "options": {},
            },
        },
    )

    assert isinstance(preferences.language_tools, LanguageToolsPreferences)
    assert preferences.language_tools.translation.route_id == route_id
    assert preferences.language_tools.translation.options == {"formality": "informal"}
