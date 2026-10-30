import json
import logging
import os
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest
from kubernetes.client.exceptions import ApiException

from stac_harvester_ingester.__main__ import (
    create_event_source_spec,
    create_sensor_spec,
    create_stac_harvester_custom_resource_spec,
    define_catalogue,
    define_resource_names,
    get_name,
    reconcile_sensors,
)


@pytest.fixture(autouse=True)
def mock_environment_variables() -> Generator[None]:
    with mock.patch.dict(os.environ, {"STAC_HARVESTER_IMAGE_URL": "path-to.com/image-name"}):
        yield


@pytest.fixture
def mock_catalogue() -> dict[str, Any]:
    contents = {
        "url": "https://www.my-url.com/path",
        "path": "path",
        "source": "source",
        "target": "target",
        "schedule": "*/10 * * * *",
    }
    return define_catalogue(json.dumps(contents), "my_workspace")


@pytest.mark.parametrize(
    ("url", "expected_name"),
    [
        ("https://www.test-domain.com/path/1", "ws-www-test-domain-com-path-1-e28785af"),
        (
            "https://www.test-domain.com/this/is/a/very/very/very/very/very/very/very/very/very/very/very/very/long/path",
            "ws-www-test-domain-com-this-is-a-very-very-very-15446c6a",
        ),
        (
            "https://www.test-domain.com/THIS.PATH.HAS.DOTS.AND.CAPITALS",
            "ws-www-test-domain-com-this-path-has-dots-and-c-c0fc3bd8",
        ),
    ],
)
def test_get_name(url: str, expected_name: str, mock_catalogue: dict[str, Any]) -> None:
    workspace = "ws"
    actual_name = get_name(workspace, url)

    assert actual_name == expected_name
    assert len(actual_name) <= 63


def test_get_name_distinguishes_sources_sharing_a_long_prefix() -> None:
    # Regression test: sources differing only after a long shared prefix (e.g. per-collection
    # URLs under the same STAC API) must not truncate to the same name.
    base = "https://api.stac.ceda.ac.uk/collections/"
    names = {
        get_name("default_workspace", base + collection)
        for collection in [
            "eocis-aerosol-slstr-daily-s3a",
            "eocis-aerosol-slstr-daily-s3b",
            "eocis-aerosol-slstr-monthly-s3a",
            "eocis-chuk-geospatial-builtarea",
            "eocis-chuk-geospatial-elevation",
            "sentinel1",
            "sentinel1_ard",
            "sentinel2_ard",
        ]
    }

    assert len(names) == 8
    assert all(len(name) <= 63 for name in names)


def test_create_stac_harvester_custom_resource_spec(mock_catalogue: dict[str, Any]) -> None:
    mock_catalogue["workspace"] = "test_workspace"
    mock_catalogue["bucket_name"] = "test_bucket"

    resource = create_stac_harvester_custom_resource_spec(mock_catalogue)

    assert len(resource.keys()) == 4
    assert len(resource["spec"].keys()) == 6


def test_create_event_source_spec(mock_catalogue: dict[str, Any]) -> None:
    mock_uid = "123"
    resource = create_event_source_spec(mock_catalogue, mock_uid)

    assert len(resource.keys()) == 4
    assert len(resource["spec"].keys()) == 3


def test_create_sensor_spec(mock_catalogue: dict[str, Any]) -> None:
    mock_uid = "123"
    resource = create_sensor_spec(mock_catalogue, mock_uid)

    assert len(resource.keys()) == 4
    assert len(resource["spec"].keys()) == 4


def test_define_catalogue() -> None:
    contents = {"url": "https://www.my-url.com/path", "path": "path", "schedule": "*/10 * * * *"}
    catalogue = define_catalogue(json.dumps(contents), "my_workspace")

    assert len(catalogue.keys()) == 8


LONG_URL = (
    "https://www.test-domain.com/this/is/a/very/very/very/very/very/very/very/very/very/very/very/very/long/path"
)
CEDA_URLS = [
    "https://api.stac.ceda.ac.uk/collections/eocis-aerosol-slstr-daily-s3a",
    "https://api.stac.ceda.ac.uk/collections/eocis-chuk-geospatial-elevation",
    "https://api.stac.ceda.ac.uk/collections/sentinel1_ard",
]
NAME_KEYS = ["name", "sensor_name", "stac_harvester_name", "job_name", "event_source_name"]


@pytest.mark.parametrize("url", ["https://www.my-url.com/path", LONG_URL, *CEDA_URLS])
def test_get_name_leaves_room_for_resource_prefixes(url: str) -> None:
    name = get_name("default_workspace", url)

    assert len(name) <= 56
    assert len(f"sensor-{name}") <= 63


@pytest.mark.parametrize("url", ["https://www.my-url.com/path", LONG_URL, *CEDA_URLS])
def test_define_resource_names_matches_define_catalogue(url: str) -> None:
    catalogue = define_catalogue(json.dumps({"url": url}), "default_workspace")
    derived = define_resource_names({}, catalogue["stac_harvester_name"].removeprefix("sh-"))

    assert {key: derived[key] for key in NAME_KEYS} == {key: catalogue[key] for key in NAME_KEYS}


def test_define_resource_names_old_style_name() -> None:
    name = "default-workspace-api-stac-ceda-ac-uk-collections-sentin"
    derived = define_resource_names({}, name)

    assert derived["name"] == name
    assert derived["sensor_name"] == f"sensor-{name}"
    assert derived["stac_harvester_name"] == f"sh-{name}"
    assert derived["job_name"] == f"job-{name}"
    assert derived["event_source_name"] == f"es-{name}"


def make_harvester(url: str, uid: str) -> dict[str, Any]:
    catalogue = define_catalogue(json.dumps({"url": url}), "my_workspace")
    return {"metadata": {"name": catalogue["stac_harvester_name"], "uid": uid}}


def stored_sensor(harvester: dict[str, Any], uid: str | None = None) -> dict[str, Any]:
    catalogue = define_resource_names({}, harvester["metadata"]["name"].removeprefix("sh-"))
    sensor = create_sensor_spec(catalogue, uid or harvester["metadata"]["uid"])
    sensor["spec"]["loggingFields"] = None
    return sensor


def mock_api(harvesters: list[dict[str, Any]], sensors: list[dict[str, Any]]) -> mock.MagicMock:
    api = mock.MagicMock()
    items = {"stac-harvesters": harvesters, "sensors": sensors}
    api.list_namespaced_custom_object.side_effect = lambda **kwargs: {"items": items[kwargs["plural"]]}
    return api


def patched_container(api: mock.MagicMock) -> dict[str, Any]:
    spec = api.patch_namespaced_custom_object.call_args.kwargs["body"]["spec"]
    job = spec["triggers"][0]["template"]["k8s"]["source"]["resource"]["spec"]
    return job["template"]["spec"]["containers"][0]


def test_reconcile_sensors_updates_outdated_image() -> None:
    harvester = make_harvester("https://www.my-url.com/path", "uid-1")
    with mock.patch.dict(os.environ, {"STAC_HARVESTER_IMAGE_URL": "old"}):
        sensor = stored_sensor(harvester)
    api = mock_api([harvester], [sensor])

    reconcile_sensors(api)

    api.patch_namespaced_custom_object.assert_called_once()
    kwargs = api.patch_namespaced_custom_object.call_args.kwargs
    assert kwargs["name"] == sensor["metadata"]["name"]
    assert kwargs["plural"] == "sensors"
    assert kwargs["namespace"] == "rc"
    assert patched_container(api)["image"] == "path-to.com/image-name"


def test_reconcile_sensors_updates_changed_harvester_topic() -> None:
    harvester = make_harvester("https://www.my-url.com/path", "uid-1")
    with mock.patch.dict(os.environ, {"HARVESTER_TOPIC": "old"}):
        sensor = stored_sensor(harvester)
    api = mock_api([harvester], [sensor])

    with mock.patch.dict(os.environ, {"HARVESTER_TOPIC": "bulk"}):
        reconcile_sensors(api)

    api.patch_namespaced_custom_object.assert_called_once()
    env = {e["name"]: e["value"] for e in patched_container(api)["env"]}
    assert env["TOPIC"] == "bulk"


def test_reconcile_sensors_skips_up_to_date_sensor() -> None:
    harvester = make_harvester("https://www.my-url.com/path", "uid-1")
    api = mock_api([harvester], [stored_sensor(harvester)])

    reconcile_sensors(api)

    api.patch_namespaced_custom_object.assert_not_called()


def test_reconcile_sensors_nulls_extra_spec_keys() -> None:
    harvester = make_harvester("https://www.my-url.com/path", "uid-1")
    sensor = stored_sensor(harvester)
    sensor["spec"]["replicas"] = 2
    api = mock_api([harvester], [sensor])

    reconcile_sensors(api)

    api.patch_namespaced_custom_object.assert_called_once()
    spec = api.patch_namespaced_custom_object.call_args.kwargs["body"]["spec"]
    assert "replicas" in spec
    assert spec["replicas"] is None
    assert "loggingFields" not in spec


def test_reconcile_sensors_logs_missing_sensor(caplog: pytest.LogCaptureFixture) -> None:
    harvester = make_harvester("https://www.my-url.com/path", "uid-1")
    api = mock_api([harvester], [])

    with caplog.at_level(logging.WARNING):
        reconcile_sensors(api)

    api.patch_namespaced_custom_object.assert_not_called()
    api.create_namespaced_custom_object.assert_not_called()
    assert "is missing" in caplog.text


def test_reconcile_sensors_skips_sensor_with_other_owner() -> None:
    harvester = make_harvester("https://www.my-url.com/path", "uid-1")
    with mock.patch.dict(os.environ, {"STAC_HARVESTER_IMAGE_URL": "old"}):
        sensor = stored_sensor(harvester, uid="other-uid")
    api = mock_api([harvester], [sensor])

    reconcile_sensors(api)

    api.patch_namespaced_custom_object.assert_not_called()


def test_reconcile_sensors_continues_after_error() -> None:
    harvesters = [
        make_harvester("https://www.my-url.com/path", "uid-1"),
        make_harvester("https://www.other-url.com/path", "uid-2"),
    ]
    with mock.patch.dict(os.environ, {"STAC_HARVESTER_IMAGE_URL": "old"}):
        sensors = [stored_sensor(harvester) for harvester in harvesters]
    api = mock_api(harvesters, sensors)
    api.patch_namespaced_custom_object.side_effect = [ApiException(status=500), None]

    reconcile_sensors(api)

    assert api.patch_namespaced_custom_object.call_count == 2


def test_reconcile_sensors_does_not_raise_when_listing_fails() -> None:
    api = mock.MagicMock()
    api.list_namespaced_custom_object.side_effect = ApiException(status=403)

    reconcile_sensors(api)

    api.patch_namespaced_custom_object.assert_not_called()
