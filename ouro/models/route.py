from typing import Any, Dict, List, Literal, Optional, Union

from pydantic import Field, model_validator

from ._base import OuroModel, Page
from .action import Action
from .asset import Asset, AssetLineageRole


RouteAssetType = Literal["file", "dataset", "post", "comment"]
# Inputs may also be a route (a quest eval route receives the submitted one).
RouteInputAssetType = Literal["file", "dataset", "post", "comment", "route"]
RouteCacheScope = Literal["none", "user", "shared"]
WhenValue = Union[bool, int, float, str]
# Request-body values that switch a declaration on: {field: value | [values]}.
# Every field must match; a list matches any of its values. Output and
# property declarations both take it; see ``ouro.utils.route_outputs``.
RouteWhen = Dict[str, Union[WhenValue, List[WhenValue]]]
RouteInputFilter = Literal[
    "audio",
    "video",
    "image",
    "pdf",
    "3d model",
    "atomic structure",
]


class RouteInputAssetDeclaration(OuroModel):
    """A single keyed declaration in ``routes.input_assets``.

    Plural declarations are the canonical shape; legacy ``input_type`` and
    ``input_file_*`` fields on the route stay in sync as primary
    projections for older clients and indexing.
    """

    asset_type: RouteInputAssetType
    asset_types: Optional[List[RouteInputAssetType]] = Field(default=None, min_length=1)
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


class RouteOutputAssetDeclaration(OuroModel):
    """A single keyed declaration in ``routes.output_assets``."""

    asset_type: RouteAssetType
    primary: Optional[bool] = None
    file_extensions: Optional[List[str]] = None
    contains_file_extensions: Optional[List[str]] = None
    # Request-body values that switch this output on
    when: Optional[RouteWhen] = None
    # May be absent depending on the run's results
    optional: Optional[bool] = None
    # Key of the output this one goes into while that output is created
    embedded_in: Optional[str] = None
    # What the output is to its source: new, version, result or supporting.
    # See ``ouro.utils.route_outputs.resolve_route_output_role``.
    role: Optional[AssetLineageRole] = None


class RoutePropertyDeclaration(OuroModel):
    """An asset property a route fills from its response (``x-ouro-properties``).

    ``value`` and ``uncertainty`` are dot paths into the response. ``subject``
    is ``{"input": name}`` or ``{"output": name}``; omitted means the primary
    input. Each run records the value against that asset, when its request
    matches ``when`` and its response meets ``requires``.
    """

    property: str
    value: str
    unit: Optional[str] = None
    uncertainty: Optional[str] = None
    subject: Optional[Dict[str, str]] = None
    method: Optional[str] = None
    conditions: Optional[Dict[str, str]] = None
    # Request-body values a run must send for this value to be recorded,
    # as on output declarations.
    when: Optional[RouteWhen] = None
    # Response fields that must hold for the value to be recorded: a dot path
    # to a value, a non-empty list of values, or a {"min", "max"} range.
    requires: Optional[Dict[str, Any]] = None

    @model_validator(mode="after")
    def _check_subject_and_requires(self) -> "RoutePropertyDeclaration":
        if self.subject is not None and (
            len(self.subject) != 1 or next(iter(self.subject)) not in ("input", "output")
        ):
            raise ValueError('subject must be {"input": name} or {"output": name}')
        for path, expected in (self.requires or {}).items():
            if isinstance(expected, dict):
                if not set(expected) <= {"min", "max"} or not expected:
                    raise ValueError(
                        f"requires.{path}: a range takes min, max or both"
                    )
                if not all(
                    isinstance(bound, (int, float)) and not isinstance(bound, bool)
                    for bound in expected.values()
                ):
                    raise ValueError(f"requires.{path}: min and max are numbers")
            elif isinstance(expected, list):
                if not expected:
                    raise ValueError(f"requires.{path}: a list needs a value")
            elif not isinstance(expected, (bool, int, float, str)):
                raise ValueError(
                    f"requires.{path}: give a value, a list of values or a range"
                )
        return self


class RouteResponseChart(OuroModel):
    """A chart Ouro draws on every run from a list in the response
    (``x-ouro-response-charts``).

    ``data`` is a dot path to a list of records; ``config`` is a dataset view's
    chart config keyed by the records' own keys. Strings may name response
    values as ``{path}`` or ``{path:decimals}``, and a reference line's value
    may be ``{"field": path}``. ``dataset`` names a dataset output of the same
    route: when a run creates it, the chart becomes its saved view.
    """

    data: str = Field(min_length=1)
    title: str = Field(min_length=1)
    description: Optional[str] = None
    dataset: Optional[str] = None
    # Record key to sort by; "-key" for descending
    sort: Optional[str] = None
    limit: Optional[int] = Field(default=None, gt=0)
    config: Dict[str, Any]


class RouteCapabilityBase(OuroModel):
    """Shared behavior declared by a semantic route capability."""

    supported_languages: List[str] = Field(min_length=1)
    cache_scope: RouteCacheScope = "none"
    cache_version: str = Field(min_length=1)
    trusted: bool = False
    structured_content: bool = False


class TextTranslationRouteCapability(RouteCapabilityBase):
    """Contract for the ``text.translate.v1`` capability."""


class SpeechVoice(OuroModel):
    """Provider voice advertised by a speech route."""

    id: str
    name: Optional[str] = None
    language: Optional[str] = None
    languages: Optional[List[str]] = None


class TextSpeechRouteCapability(RouteCapabilityBase):
    """Contract for the ``text.speech.v1`` capability."""

    voices: List[SpeechVoice]


class SpeechTranscribeRouteCapability(RouteCapabilityBase):
    """Contract for the ``speech.transcribe.v1`` capability."""


class RouteCapabilities(OuroModel):
    """Semantic capabilities stored under ``x-ouro-capabilities``."""

    text_translate_v1: Optional[TextTranslationRouteCapability] = Field(
        default=None, alias="text.translate.v1"
    )
    text_speech_v1: Optional[TextSpeechRouteCapability] = Field(
        default=None, alias="text.speech.v1"
    )
    speech_transcribe_v1: Optional[SpeechTranscribeRouteCapability] = Field(
        default=None, alias="speech.transcribe.v1"
    )


class RouteData(OuroModel):
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
    input_type: Optional[RouteInputAssetType] = None
    input_filter: Optional[RouteInputFilter] = None
    input_file_extension: Optional[str] = None
    input_file_extensions: Optional[List[str]] = None
    # Legacy primary projection — kept synchronized with ``output_assets``.
    output_type: Optional[RouteAssetType] = None
    # Canonical plural output declarations keyed by response body field name.
    output_assets: Optional[Dict[str, RouteOutputAssetDeclaration]] = None
    output_file_extension: Optional[str] = None
    # Asset properties each run records from its response.
    properties: Optional[List[RoutePropertyDeclaration]] = None
    # Charts each run draws from lists in its response.
    response_charts: Optional[List[RouteResponseChart]] = None
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


class RouteMetrics(OuroModel):
    """Per-route latency aggregates surfaced from ``asset_metrics``.

    All fields are optional because they don't exist until the platform has
    observed at least one completed action for the route.
    """

    # Average HTTP-hop latency in milliseconds: time from request start until
    # upstream returned 200 or 202.
    avg_ack_ms: Optional[int] = None
    p50_ack_ms: Optional[int] = None
    p95_ack_ms: Optional[int] = None
    # Average end-to-end latency in milliseconds: started_at to finished_at,
    # includes webhook completion for async routes. This is the value an
    # agent should consider when deciding whether to wait or poll. p50 is the
    # typical run; a long tail pulls the average well above it.
    avg_completion_ms: Optional[int] = None
    p50_completion_ms: Optional[int] = None
    p95_completion_ms: Optional[int] = None
    latency_sample_count: Optional[int] = None


class RouteStats(OuroModel):
    access: Optional[str] = None
    total: int = 0
    user_total: int = Field(default=0, alias="userTotal")
    in_progress: List[Action] = Field(default_factory=list, alias="inProgress")
    monetization: Optional[Dict[str, Any]] = None


class RouteCost(OuroModel):
    """Price of running a variable-cost route on a specific input asset."""

    cost_accounting: Optional[str] = None
    cost_unit: Optional[str] = None
    unit_cost: Optional[float] = None
    # Currency the cost is quoted in ("usd" dollars / "btc" sats)
    currency: Optional[str] = None
    quantity: Optional[float] = None
    total_cost: Optional[float] = None


class Route(Asset):
    route: Optional[RouteData] = None
    metrics: Optional[RouteMetrics] = None

    def read_stats(self) -> RouteStats:
        """Usage counts and in-progress actions for this route."""
        return self._require_client().routes.stats(str(self.id))

    def read_actions(self) -> Page[Action]:
        """List this route's actions."""
        return self._require_client().routes.list_actions(str(self.id))

    def read_cost(self, asset_id: str) -> RouteCost:
        """Price of running this route on ``asset_id``."""
        return self._require_client().routes.cost(str(self.id), asset_id)

    def execute(
        self,
        *,
        wait: bool = True,
        poll_interval: Optional[float] = None,
        poll_timeout: Optional[float] = None,
        **kwargs,
    ) -> Action:
        """Execute this route and return the full Action."""
        return self._require_client().routes.execute(
            str(self.id),
            wait=wait,
            poll_interval=poll_interval,
            poll_timeout=poll_timeout,
            **kwargs,
        )
