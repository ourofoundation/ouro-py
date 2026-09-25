from typing import TYPE_CHECKING, Any, Dict, List, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .asset import Asset

if TYPE_CHECKING:
    from ouro import Ouro
    from ouro.models.action import Action


RouteAssetType = Literal["file", "dataset", "post", "comment"]
RouteCacheScope = Literal["none", "user", "shared"]
RouteInputFilter = Literal[
    "audio",
    "video",
    "image",
    "pdf",
    "3d model",
    "atomic structure",
]


class RouteInputAssetDeclaration(BaseModel):
    """A single keyed declaration in ``routes.input_assets``.

    Plural declarations are the canonical shape; legacy ``input_type`` and
    ``input_file_*`` fields on the route stay in sync as primary
    projections for older clients and indexing.
    """

    model_config = ConfigDict(extra="allow")

    asset_type: RouteAssetType
    asset_types: Optional[List[RouteAssetType]] = Field(default=None, min_length=1)
    primary: Optional[bool] = None
    input_filter: Optional[RouteInputFilter] = None
    file_extensions: Optional[List[str]] = None
    contains_file_extensions: Optional[List[str]] = None

    @model_validator(mode="before")
    @classmethod
    def _validate_asset_types(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value

        declaration = dict(value)
        asset_types = declaration.get("asset_types")
        if (
            isinstance(asset_types, list)
            and asset_types
            and "asset_type" in declaration
            and declaration["asset_type"] not in asset_types
        ):
            raise ValueError(
                "asset_types must include the legacy asset_type projection"
            )
        return declaration


class RouteOutputAssetDeclaration(BaseModel):
    """A single keyed declaration in ``routes.output_assets``."""

    model_config = ConfigDict(extra="allow")

    asset_type: RouteAssetType
    primary: Optional[bool] = None
    file_extensions: Optional[List[str]] = None
    contains_file_extensions: Optional[List[str]] = None


class RouteCapabilityBase(BaseModel):
    """Shared behavior declared by a semantic route capability."""

    model_config = ConfigDict(extra="allow")

    supported_languages: List[str] = Field(min_length=1)
    cache_scope: RouteCacheScope = "none"
    cache_version: str = Field(min_length=1)
    trusted: bool = False
    structured_content: bool = False


class TextTranslationRouteCapability(RouteCapabilityBase):
    """Contract for the ``text.translate.v1`` capability."""


class SpeechVoice(BaseModel):
    """Provider voice advertised by a speech route."""

    model_config = ConfigDict(extra="allow")

    id: str
    name: Optional[str] = None
    language: Optional[str] = None
    languages: Optional[List[str]] = None


class TextSpeechRouteCapability(RouteCapabilityBase):
    """Contract for the ``text.speech.v1`` capability."""

    voices: List[SpeechVoice]


class SpeechTranscribeRouteCapability(RouteCapabilityBase):
    """Contract for the ``speech.transcribe.v1`` capability."""


class RouteCapabilities(BaseModel):
    """Semantic capabilities stored under ``x-ouro-capabilities``."""

    model_config = ConfigDict(extra="allow", populate_by_name=True)

    text_translate_v1: Optional[TextTranslationRouteCapability] = Field(
        default=None, alias="text.translate.v1"
    )
    text_speech_v1: Optional[TextSpeechRouteCapability] = Field(
        default=None, alias="text.speech.v1"
    )
    speech_transcribe_v1: Optional[SpeechTranscribeRouteCapability] = Field(
        default=None, alias="speech.transcribe.v1"
    )


class RouteData(BaseModel):
    description: Optional[str] = None
    path: str
    method: str
    parameters: Optional[List[Dict]] = None
    request_body: Optional[Dict] = {}
    responses: Optional[Dict] = None
    security: Optional[str] = None
    # Canonical plural input declarations keyed by request body field name.
    input_assets: Optional[Dict[str, RouteInputAssetDeclaration]] = None
    # Legacy primary projection — kept synchronized with ``input_assets``.
    input_type: Optional[RouteAssetType] = None
    input_filter: Optional[RouteInputFilter] = None
    input_file_extension: Optional[str] = None
    input_file_extensions: Optional[List[str]] = None
    # Legacy primary projection — kept synchronized with ``output_assets``.
    output_type: Optional[RouteAssetType] = None
    # Canonical plural output declarations keyed by response body field name.
    output_assets: Optional[Dict[str, RouteOutputAssetDeclaration]] = None
    output_file_extension: Optional[str] = None
    capabilities: Optional[RouteCapabilities] = None
    rate_limit: Optional[int] = None
    # Author-declared execution model: 'sync' = upstream returns the result
    # inline; 'async' = upstream returns 202 quickly and webhooks completion.
    # Agents should consult this when deciding whether to wait inline or
    # request the action handle and check back later via action_id.
    execution_mode: Optional[str] = "sync"
    # Empirical mode derived by the platform from recent action history; null
    # until enough samples have been observed. When this differs from
    # ``execution_mode`` the route's declaration is misconfigured.
    observed_execution_mode: Optional[str] = None


class RouteMetrics(BaseModel):
    """Per-route latency aggregates surfaced from ``asset_metrics``.

    All fields are optional because they don't exist until the platform has
    observed at least one completed action for the route.
    """

    # Average HTTP-hop latency in milliseconds: time from request start until
    # upstream returned 200 or 202.
    avg_ack_ms: Optional[int] = None
    p95_ack_ms: Optional[int] = None
    # Average end-to-end latency in milliseconds: started_at to finished_at,
    # includes webhook completion for async routes. This is the value an
    # agent should consider when deciding whether to wait or poll.
    avg_completion_ms: Optional[int] = None
    p95_completion_ms: Optional[int] = None
    latency_sample_count: Optional[int] = None


class Route(Asset):
    route: Optional[RouteData] = None
    metrics: Optional[RouteMetrics] = None
    _ouro: Optional["Ouro"] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ouro = kwargs.get("_ouro")

    def _require_client(self) -> "Ouro":
        if not self._ouro:
            raise RuntimeError("Route object not connected to Ouro client")
        return self._ouro

    def _api_get(self, path: str) -> Any:
        """Make a GET request through the centralized response handler."""
        ouro = self._require_client()
        from ouro._resource import SyncAPIResource

        resource = SyncAPIResource(ouro)
        return resource._handle_response(ouro.client.get(path))

    def read_stats(self) -> Dict:
        """Get stats for a route."""
        return self._api_get(f"/routes/{self.id}/stats")

    def read_actions(self) -> List["Action"]:
        """Get actions for a route."""
        ouro = self._require_client()
        return ouro.routes.list_actions(str(self.id))

    def read_analytics(self) -> Dict:
        """Get analytics for a route."""
        return self._api_get(
            f"/services/{self.parent_id}/routes/{self.id}/analytics"
        )

    def read_cost(self, asset_id: str) -> Dict:
        """Calculate the cost for a route."""
        return self._api_get(
            f"/services/{self.parent_id}/routes/{self.id}/cost?input={asset_id}"
        )

    def execute(
        self,
        *,
        wait: bool = True,
        poll_interval: Optional[float] = None,
        poll_timeout: Optional[float] = None,
        **kwargs,
    ) -> "Action":
        """Execute this route and return the full Action."""
        ouro = self._require_client()
        return ouro.routes.execute(
            str(self.id),
            wait=wait,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
            **kwargs,
        )

    def use(
        self,
        *,
        wait: bool = True,
        poll_interval: Optional[float] = None,
        poll_timeout: Optional[float] = None,
        **kwargs,
    ) -> Union[Dict, "Action"]:
        """Deprecated compatibility wrapper for :meth:`execute`.

        For routes that return 202 (async processing), this method will automatically
        poll for updates until the action completes, unless wait=False.

        Args:
            wait: If True (default), wait for async routes to complete. If False,
                send ``Prefer: respond-async`` to get the action handle back
                immediately and check on it later via ``action.refresh()`` or
                ``ouro.routes.poll_action``.
            poll_interval: Seconds between status checks when waiting. If None
                (default), the SDK derives an interval from this route's
                ``avg_completion_ms`` metric.
            poll_timeout: Maximum seconds to wait for completion. If None
                (default), the SDK derives a timeout from this route's
                ``p95_completion_ms`` metric.
            **kwargs: Additional arguments (body, query, params, output, timeout).
        """
        raise_on_error = kwargs.pop("raise_on_error", wait)
        action = self.execute(
            wait=wait,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
            raise_on_error=raise_on_error,
            **kwargs,
        )
        return action if not wait else action.final_data
