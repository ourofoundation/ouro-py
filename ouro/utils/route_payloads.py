"""Response payloads for a route's declared output assets.

A service returns each output under its declared response key; Ouro creates
the asset from it in the organization and team the run belongs to, so the
payload carries only what the asset is made from. ``visibility`` overrides
the audience Ouro would give it (left out, it follows the route and input).
``license_id`` (an SPDX id such as ``"CC-BY-4.0"``) and ``attribution`` (the
asset's attribution fields) credit someone else's work: left out, an output
is your original work under All Rights Reserved.
"""

from __future__ import annotations

import base64
import mimetypes
from typing import Any, Dict, List, Optional, Union

__all__ = ["file_payload", "dataset_payload"]


def file_payload(
    content: Union[bytes, str],
    *,
    name: str,
    description: str,
    extension: str,
    content_type: Optional[str] = None,
    visibility: Optional[str] = None,
    license_id: Optional[str] = None,
    attribution: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """A ``file`` output from bytes (or text, encoded as UTF-8).

    ``extension`` is the file type without a dot (``"cif"``); the MIME type is
    guessed from it unless ``content_type`` is given.
    """
    if isinstance(content, str):
        content = content.encode("utf-8")
    extension = extension.lstrip(".").lower()
    payload: Dict[str, Any] = {
        "name": name,
        "description": description,
        "extension": extension,
        "type": content_type
        or mimetypes.guess_type(f"file.{extension}")[0]
        or "application/octet-stream",
        "base64": base64.b64encode(content).decode("ascii"),
    }
    return _with_options(payload, visibility, license_id, attribution)


def dataset_payload(
    rows: List[Dict[str, Any]],
    *,
    name: str,
    description: str,
    visibility: Optional[str] = None,
    license_id: Optional[str] = None,
    attribution: Optional[Dict[str, Any]] = None,
) -> Optional[Dict[str, Any]]:
    """A ``dataset`` output from flat records, or None when there are none
    (a dataset needs a row; a None output is skipped).

    Ouro lowercases column names. The dataset's view comes from the route's
    response chart that names this output as its ``dataset``.
    """
    if not rows:
        return None
    payload: Dict[str, Any] = {"name": name, "description": description, "data": rows}
    return _with_options(payload, visibility, license_id, attribution)


def _with_options(
    payload: Dict[str, Any],
    visibility: Optional[str],
    license_id: Optional[str],
    attribution: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    for key, value in (
        ("visibility", visibility),
        ("license_id", license_id),
        ("attribution", attribution),
    ):
        if value is not None:
            payload[key] = value
    return payload
