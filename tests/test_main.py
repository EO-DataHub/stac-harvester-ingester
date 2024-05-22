from stac_harvester_ingester.__main__ import get_name, create_custom_resource


def test_get_name():
    url = 'https://www.test-domain.com/path/1'
    expected_name = "www.test-domain.com-path-1"
    actual_name = get_name(url)

    assert actual_name == expected_name


def test_create_custom_resource():
    catalogue = {'url': 'www.my-url.com', 'workspace': 'workspace', 'bucket_name': 'bucket', 'path': 'path', 'pod_name': 'pod'}

    resource = create_custom_resource(catalogue)

    assert len(resource.keys()) == 4
    assert len(resource['spec'].keys()) == 4
