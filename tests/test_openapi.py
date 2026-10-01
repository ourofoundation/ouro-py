import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.openapi.utils import get_openapi

from ouro.utils import get_custom_openapi, ouro_execution_mode, ouro_pricing


def test_ouro_fields_reach_routes_in_included_routers() -> None:
    app = fastapi.FastAPI()
    router = fastapi.APIRouter()

    @router.post("/thermo/ehull")
    @ouro_execution_mode("async")
    def ehull() -> None:
        pass

    @app.get("/health")
    @ouro_execution_mode("sync")
    def health() -> None:
        pass

    app.include_router(router, prefix="/v1")
    app.openapi = get_custom_openapi(app, get_openapi)

    paths = app.openapi()["paths"]
    assert paths["/v1/thermo/ehull"]["post"]["x-ouro-execution-mode"] == "async"
    assert paths["/health"]["get"]["x-ouro-execution-mode"] == "sync"


def _pricing(**kwargs):
    return ouro_pricing(**kwargs)(lambda: None).ouro_fields["x-ouro-pricing"]


def test_pricing_reaches_the_operation() -> None:
    app = fastapi.FastAPI()

    @app.post("/simulate")
    @ouro_execution_mode("async")
    @ouro_pricing(per_second=0.0003, max_seconds=3600)
    def simulate() -> None:
        pass

    app.openapi = get_custom_openapi(app, get_openapi)
    operation = app.openapi()["paths"]["/simulate"]["post"]
    assert operation["x-ouro-pricing"] == {
        "model": "per_second",
        "unit_cost": 0.0003,
        "currency": "usd",
        "max_billable_seconds": 3600,
    }
    assert operation["x-ouro-execution-mode"] == "async"


def test_pricing_models() -> None:
    assert _pricing(per_call=21, currency="btc") == {
        "model": "per_call",
        "unit_cost": 21,
        "currency": "btc",
    }
    assert _pricing(free=True) == {"model": "free"}


@pytest.mark.parametrize(
    "kwargs",
    [
        {},
        {"per_second": 0.1, "per_call": 1, "max_seconds": 60},
        {"per_call": 1, "free": True},
        {"per_second": 0.1},
        {"per_second": 0.1, "max_seconds": 90_000},
        {"per_second": 0.1, "max_seconds": 60.5},
        {"per_call": 0},
        {"per_call": "0.05"},
        {"per_call": 1, "max_seconds": 60},
        {"per_call": 1, "currency": "eur"},
    ],
)
def test_pricing_rejects_declarations_ouro_would_refuse(kwargs) -> None:
    with pytest.raises(ValueError, match="ouro_pricing"):
        ouro_pricing(**kwargs)
