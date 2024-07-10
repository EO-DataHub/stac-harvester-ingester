import json
import os
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
def mock_environment_variables():
    with mock.patch.dict(os.environ, {"STAC_HARVESTER_IMAGE_URL": "path-to.com/image-name"}):
        yield


@pytest.fixture
def mock_catalogue():
    contents = {
        "url": "https://www.my-url.com/path",
        "catalog": "catalog",
        "path": "path",
        "schedule": "*/10 * * * *",
    }
    return define_catalogue(json.dumps(contents), "my_workspace")


@pytest.mark.parametrize(
    "url, expected_name",
    [
        ("https://www.test-domain.com/path/1", "ws-www-test-domain-com-path-1"),
        (
            "https://www.test-domain.com/this/is/a/very/very/very/very/very/very/very/very/very/very/very/very/long/path",
            "ws-www-test-domain-com-this-is-a-very-very-very-very-ver",
        ),
        (
            "https://www.test-domain.com/THIS.PATH.HAS.DOTS.AND.CAPITALS",
            "ws-www-test-domain-com-this-path-has-dots-and-capitals",
        ),
    ],
)
def test_get_name(url, expected_name, mock_catalogue):
    workspace = "ws"
    actual_name = get_name(workspace, url)

    if len(actual_name) <= 63:
        assert actual_name == expected_name
    else:
        assert actual_name[:-8] == expected_name
    assert len(actual_name) <= 63


def test_create_stac_harvester_custom_resource_spec(mock_catalogue):
    mock_catalogue["workspace"] = "test_workspace"
    mock_catalogue["bucket_name"] = "test_bucket"

    resource = create_stac_harvester_custom_resource_spec(mock_catalogue)

    assert len(resource.keys()) == 4
    assert len(resource["spec"].keys()) == 5


def test_create_event_source_spec(mock_catalogue):
    mock_uid = "123"
    resource = create_event_source_spec(mock_catalogue, mock_uid)

    assert len(resource.keys()) == 4
    assert len(resource["spec"].keys()) == 3


def test_create_sensor_spec(mock_catalogue):
    mock_uid = "123"
    resource = create_sensor_spec(mock_catalogue, mock_uid)

    assert len(resource.keys()) == 4
    assert len(resource["spec"].keys()) == 4


def test_define_catalogue():
    contents = {"url": "https://www.my-url.com/path", "path": "path", "schedule": "*/10 * * * *"}
    catalogue = define_catalogue(json.dumps(contents), "my_workspace")

    assert len(catalogue.keys()) == 8
