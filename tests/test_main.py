import json
import os
from collections.abc import Generator
from typing import Any
from unittest import mock

import pytest

from stac_harvester_ingester.__main__ import (
    create_event_source_spec,
    create_sensor_spec,
    create_stac_harvester_custom_resource_spec,
    define_catalogue,
    get_name,
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
