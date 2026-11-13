from __future__ import annotations

import httpx
import pytest

from ouro import ExternalServiceError, OuroError, RouteExecutionError
from ouro.models import Action, ActionLog, Route, Service


@pytest.fixture(scope="module")
def service(ouro, track, mock_service):
    return track.add(
        ouro.services.create(
            name=track.name("service"),
            base_url=mock_service.base_url,
            spec_url=mock_service.spec_url,
            visibility="private",
            description="Mock service for SDK integration tests",
        )
    )


@pytest.fixture(scope="module")
def routes(ouro, service) -> dict[str, Route]:
    return {r.route.path: r for r in ouro.services.read_routes(str(service.id))}


def test_service_created_from_spec(service, mock_service):
    assert isinstance(service, Service)
    assert service.metadata.base_url == mock_service.base_url
    assert service.metadata.spec_url == mock_service.spec_url


def test_routes_parsed_from_spec(routes):
    assert set(routes) == {
        "/echo",
        "/slow-echo",
        "/slow-fail",
        "/fail",
        "/items/{item_id}",
        "/inspect-file",
        "/inspect-file-id",
        "/make-report",
    }
    assert routes["/slow-echo"].route.execution_mode == "async"
    assert routes["/echo"].route.execution_mode == "sync"
    assert routes["/inspect-file"].route.input_assets["structure"].asset_type == "file"
    assert routes["/make-report"].route.output_assets["report"].asset_type == "post"


def test_retrieve_by_id_and_slug(ouro, routes):
    echo = routes["/echo"]
    by_id = ouro.routes.retrieve(str(echo.id))
    by_slug = ouro.routes.retrieve(echo.slug.removeprefix("/routes/"))
    assert by_id.id == by_slug.id == echo.id
    assert by_id.route.method.upper() == "POST"


def test_read_spec(ouro, service):
    spec = ouro.services.read_spec(str(service.id))
    assert "/echo" in spec["paths"]


def test_execute_sync(ouro, routes, mock_service):
    action = ouro.routes.execute(str(routes["/echo"].id), body={"text": "hello"})
    assert isinstance(action, Action)
    assert action.is_success
    assert action.response["echo"] == "hello"
    forwarded = mock_service.last("/echo")
    assert forwarded["headers"]["ouro-action-id"] == str(action.id)
    assert forwarded["headers"]["ouro-user-id"]
    assert ouro.routes.retrieve_action(str(action.id)).status == "success"


def test_execute_with_path_and_query_params(ouro, routes):
    action = ouro.routes.execute(
        str(routes["/items/{item_id}"].id), params={"item_id": "abc"}, query={"q": "find"}
    )
    assert action.response == {"item_id": "abc", "q": "find"}


def test_execute_async_waits_for_webhook(ouro, routes):
    action = ouro.routes.execute(str(routes["/slow-echo"].id), body={"text": "later"}, poll_interval=0.5)
    assert action.is_success
    assert action.response["echo"] == "later"


def test_execute_async_without_waiting(ouro, routes):
    action = ouro.routes.execute(str(routes["/slow-echo"].id), body={"text": "bg"}, wait=False)
    assert action.is_pending
    finished = action.wait(poll_interval=0.5, timeout=30)
    assert finished.is_success
    assert finished.response["echo"] == "bg"
    assert action.refresh().is_success


def test_sync_failure_returns_errored_action_by_default(ouro, routes):
    action = ouro.routes.execute(str(routes["/fail"].id), body={})
    assert action.is_error


def test_sync_failure_raises_when_requested(ouro, routes):
    with pytest.raises(ExternalServiceError) as info:
        ouro.routes.execute(str(routes["/fail"].id), body={}, raise_on_error=True)
    assert info.value.action_id
    assert info.value.status_code == 500


def test_async_failure_raises_route_execution_error(ouro, routes):
    with pytest.raises(RouteExecutionError) as info:
        ouro.routes.execute(str(routes["/slow-fail"].id), body={}, raise_on_error=True, poll_interval=0.5)
    assert "async mock failure" in str(info.value)


def test_input_asset_is_resolved_for_the_service(ouro, track, routes, mock_service):
    file = track.add(
        ouro.files.create(name=track.name("route-input"), visibility="private", file_content=b"data_x", file_name="x.cif")
    )
    action = ouro.routes.execute(str(routes["/inspect-file"].id), input_assets={"structure": str(file.id)})
    assert action.is_success
    assert "structure" in action.response["keys"]
    assert "url" in action.response["structure_keys"] or "id" in action.response["structure_keys"]

    linked = ouro.assets.actions(str(file.id), role="input")
    assert any(a.id == action.id for a in linked.as_input)


CIF = "data_Fe2P\n_cell_length_a 5.865\n"


def test_upload_input_reaches_the_service_without_an_asset(ouro, me, routes, tmp_path):
    """A local path is uploaded as scratch and passed by upload_id."""
    path = tmp_path / "Fe2P.cif"
    path.write_text(CIF)

    action = ouro.routes.execute(
        str(routes["/inspect-file"].id), input_assets={"structure": path}
    )

    assert action.is_success, action.error
    response = action.response
    assert response["content"] == CIF
    assert response["filename"] == "Fe2P.cif"
    assert response["structure_id"] is None
    assert response["upload_id"].startswith(f"files/{me.user_id}/scratch/")
    assert action.input_asset_id is None

    # The run records the upload, not an asset
    action = ouro.routes.retrieve_action(str(action.id))
    recorded = action.metadata["input_assets"]["structure"]
    assert recorded["uploadId"] == response["upload_id"]
    assert recorded["fileName"] == "Fe2P.cif"
    assert recorded["size"] == len(CIF)


def test_one_upload_serves_many_runs(ouro, routes):
    ref = ouro.files.upload_input(CIF.encode(), file_name="reused.cif")
    for note in ("first", "second"):
        action = ouro.routes.execute(
            str(routes["/inspect-file"].id), input_assets={"structure": ref}
        )
        assert action.is_success, action.error
        assert action.response["content"] == CIF
        assert action.response["filename"] == "reused.cif"


def test_bare_upload_id_keeps_its_file_name(ouro, routes):
    """An agent passing only the upload_id still hands the service its name."""
    reserved = ouro.files.create_upload_url("Fe2P conventional.cif", purpose="input")
    httpx.request(
        reserved["method"], reserved["upload_url"], content=CIF.encode(),
        headers=reserved["headers"],
    ).raise_for_status()

    action = ouro.routes.execute(
        str(routes["/inspect-file"].id),
        input_assets={"structure": {"upload_id": reserved["upload_id"]}},
    )

    assert action.is_success, action.error
    assert action.response["filename"] == "Fe2P_conventional.cif"
    assert action.response["content"] == CIF


def test_upload_input_can_still_become_an_asset(ouro, track):
    ref = ouro.files.upload_input(CIF.encode(), file_name="keeper.cif")
    file = track.add(
        ouro.files.create(
            name=track.name("claimed-input"),
            upload_id=ref["upload_id"],
            visibility="private",
        )
    )
    # Claiming keeps the scratch path, so the sweep can tell it was kept
    assert "/scratch/" in file.metadata.path


def test_upload_refused_where_the_route_takes_an_asset_id(ouro, routes):
    ref = ouro.files.upload_input(CIF.encode(), file_name="x.cif")
    with pytest.raises(OuroError, match="takes an Ouro asset id"):
        ouro.routes.execute(
            str(routes["/inspect-file-id"].id), input_assets={"structure": ref}
        )


def test_someone_elses_upload_is_refused(ouro, routes):
    with pytest.raises(OuroError, match="not an upload of yours"):
        ouro.routes.execute(
            str(routes["/inspect-file"].id),
            input_assets={"structure": {"upload_id": "files/someone-else/scratch/x.cif"}},
        )


def test_output_asset_is_created(ouro, track, routes):
    action = ouro.routes.execute(str(routes["/make-report"].id), body={"title": track.name("report")})
    assert action.is_success
    assert action.output_asset and action.output_asset.asset_type == "post"
    report = track.add(ouro.posts.retrieve(str(action.output_asset.id)))
    assert "Generated by the mock service" in report.content.text
    assert action.final_data["report"]["id"] == str(report.id)
    created_by = ouro.assets.actions(str(report.id), role="output").created_by
    assert created_by.id == action.id


def test_action_logs(ouro, routes):
    action = ouro.routes.execute(str(routes["/echo"].id), body={"text": "log me"})
    action.log("hello from the SDK", level="info")
    logs = action.read_logs(chronological=True)
    assert all(isinstance(entry, ActionLog) for entry in logs)
    assert any(entry.message == "hello from the SDK" for entry in logs)


def test_list_actions(ouro, routes):
    actions = ouro.routes.list_actions(str(routes["/echo"].id), limit=5)
    assert actions and all(isinstance(a, Action) for a in actions)
    page = ouro.routes.list_actions(str(routes["/echo"].id), limit=1)
    assert len(page) == 1 and page.has_more


def test_route_model_helpers(ouro, routes):
    route = ouro.routes.retrieve(str(routes["/echo"].id))
    stats = route.read_stats()
    assert stats.total >= 1 and stats.user_total >= 1
    assert route.execute(body={"text": "via model"}).response["echo"] == "via model"


def test_service_model_execute_route(ouro, service, routes):
    svc = ouro.services.retrieve(str(service.id))
    assert svc.execute_route(str(routes["/echo"].id), body={"text": "svc"}).response["echo"] == "svc"


def test_route_crud_on_bare_service(ouro, track, mock_service):
    bare = track.add(
        ouro.services.create(
            name=track.name("bare"),
            base_url=mock_service.base_url + "/bare",
            visibility="private",
        )
    )
    assert ouro.services.read_routes(str(bare.id)) == []
    route = ouro.routes.create(str(bare.id), method="POST", path="/echo", name="bare-echo")
    assert route.parent_id == bare.id
    assert route.visibility in ("inherit", "private")

    updated = ouro.routes.update(str(route.id), description="Now described")
    assert updated.name == route.name
    assert updated.description.text.startswith("Now described")

    summary = ouro.services.delete(str(bare.id), dry_run=True)
    assert [c.id for c in summary.deleted_children] == [route.id]


def test_update_service_metadata_and_refresh_spec(ouro, service):
    renamed = ouro.services.update(str(service.id), version="2.0.0")
    assert renamed.name == service.name
    assert renamed.metadata.version == "2.0.0"
    refreshed = ouro.services.update(str(service.id), refresh_spec=True)
    assert refreshed.metadata.spec_url == service.metadata.spec_url


def test_set_authentication_is_idempotent(ouro, service):
    first = ouro.services.set_authentication(str(service.id), secret="s3cret", method="Ouro")
    second = ouro.services.set_authentication(str(service.id), secret="s3cret", method="Ouro")
    assert second.rotated is False
    assert first.secret_id == second.secret_id


def test_compatible_routes_for_file(ouro, track, routes):
    file = track.add(
        ouro.files.create(name=track.name("compat"), visibility="private", file_content=b"x", file_name="c.cif")
    )
    compatible = ouro.assets.compatible_routes(str(file.id), limit=200)
    assert all(isinstance(route, Route) for route in compatible)

    searched = ouro.assets.compatible_routes(str(file.id), query="relax the structure", limit=5)
    assert all(isinstance(route, Route) for route in searched)
    assert len(list(searched)) <= 5


def test_other_user_cannot_run_private_route(other, routes):
    with pytest.raises(OuroError):
        other.routes.execute(str(routes["/echo"].id), body={"text": "nope"})
