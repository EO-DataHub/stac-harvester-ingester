import argparse
import json
import logging
import os
import re
import urllib
from urllib.parse import urlparse
from pulsar import Client

import boto3

from kubernetes import client, config

# Initiate Pulsar
pulsar_url = os.environ.get("PULSAR_URL")
pulsar_client = Client(pulsar_url)
consumer = pulsar_client.subscribe(topic="harvested", subscription_name="transformer-subscription")
producer = pulsar_client.create_producer(topic="transformed", producer_name="transformer")

logging.basicConfig(level=logging.DEBUG, format="%(message)s")

def create_custom_resource(catalogue):
    return {
        "apiVersion": "core.telespazio-uk.io/v1",
        "kind": "STACHarvester",
        "metadata": {"name": catalogue["pod_name"]},
        "spec": {
            "command": f"/bin/sh -c python3 -m stac_harvester {catalogue['url']} {os.environ['WORKSPACE_NAME']} {catalogue['path']} {bucket_name} {catalogue['limit']}",  # noqa: E501
            "image": os.environ["STAC_HARVESTER_IMAGE_URL"],
        },
    }


def get_name(url):
    parsed = urlparse(url)
    name = parsed.hostname + parsed.path if parsed.path else parsed.hostname
    name = name.replace("/", "-")

    return re.sub(r"[^0-9a-z.-]", r"", name)

def create_custom_resource(catalogue, bucket_name):
    return {
        "apiVersion": "core.telespazio-uk.io/v1",
        "kind": "STACHarvester",
        "metadata": {"name": catalogue['pod_name']},
        "spec": {
            "command": f"/bin/sh -c python3 -m stac_harvester {catalogue['url']} {os.environ['WORKSPACE_NAME']} {catalogue['path']} {bucket_name} {catalogue['limit']}",
            "image": os.environ["STAC_HARVESTER_IMAGE_URL"],
        }
    }


def main():
    config.load_kube_config()

    if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        s3 = session.client("s3")

    else:
        s3 = boto3.client("s3")

    pulsar_url = os.environ.get("PULSAR_URL")
    pulsar_client = Client(pulsar_url)
    consumer = pulsar_client.subscribe(
        topic="transformed", subscription_name="stac-harvester-subscription"
    )

    plural = "stac-harvesters"
    namespace = os.environ['NAMESPACE_NAME']

    while True:
        msg = consumer.receive()
        try:
            logging.info(f"Parsing harvested message {msg.data()}")

            data = msg.data().decode("utf-8")
            data_dict = json.loads(data.replace("'", '"'))
            bucket_name = data_dict.get("bucket_name")

            for key in data_dict['added_keys'] + data_dict['updated_keys']:

                data = s3.get_object(Bucket=bucket_name, Key=key)
                contents = data['Body'].read()
                catalogue = contents.decode("utf-8")

                api = client.CustomObjectsApi()

                catalogue['pod_name'] = f"stac-harvester-{get_name(catalogue['url'])}"

                resource = create_custom_resource(catalogue, bucket_name)

                try:
                    api.create_namespaced_custom_object(
                        group="core.telespazio-uk.io",
                        version="v1",
                        namespace=namespace,
                        plural=plural,
                        body=resource,
                    )
                    print("Resource created")
                except client.exceptions.ApiException as e:
                    if e.status == 409:  # Already exists. Recreate
                        api.delete_namespaced_custom_object(
                            group="core.telespazio-uk.io",
                            version="v1",
                            name=catalogue['pod_name'],
                            namespace=namespace,
                            plural=plural,
                            body=client.V1DeleteOptions(),
                        )
                        api.create_namespaced_custom_object(
                            group="core.telespazio-uk.io",
                            version="v1",
                            namespace=namespace,
                            plural=plural,
                            body=resource,
                        )
                        print("Resource created")
                    else:
                        raise
        except Exception as e:
            logging.error(f"Error occurred during harvest: {e}")
        finally:
            consumer.acknowledge(msg)


if __name__ == "__main__":
    main()
