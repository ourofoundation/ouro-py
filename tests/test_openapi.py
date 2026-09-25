import pytest

fastapi = pytest.importorskip("fastapi")
from fastapi.openapi.utils import get_openapi

from ouro.utils import get_custom_openapi, ouro_execution_mode


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
