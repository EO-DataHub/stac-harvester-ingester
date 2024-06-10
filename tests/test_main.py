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
    contents = {"url": "https://www.my-url.com/path", "path": "path", "schedule": "*/10 * * * *"}
    return define_catalogue(json.dumps(contents))


def test_get_name(mock_catalogue):
    url = "https://www.test-domain.com/path/1"
    expected_name = "www.test-domain.com-path-1"
    actual_name = get_name(url)

    assert actual_name == expected_name


def test_create_stac_harvester_custom_resource_spec(mock_catalogue):
    mock_catalogue["workspace"] = "test_workspace"
    mock_catalogue["bucket_name"] = "test_bucket"

    resource = create_stac_harvester_custom_resource_spec(mock_catalogue)

    assert len(resource.keys()) == 4
    assert len(resource["spec"].keys()) == 4


def test_create_event_source_spec(mock_catalogue):
    resource = create_event_source_spec(mock_catalogue)

    assert len(resource.keys()) == 4
    assert len(resource["spec"].keys()) == 2


def test_create_sensor_spec(mock_catalogue):
    resource = create_sensor_spec(mock_catalogue)

    assert len(resource.keys()) == 4
    assert len(resource["spec"].keys()) == 3


def test_define_catalogue():
    contents = {"url": "https://www.my-url.com/path", "path": "path", "schedule": "*/10 * * * *"}
    catalogue = define_catalogue(json.dumps(contents))

    assert len(catalogue.keys()) == 8
