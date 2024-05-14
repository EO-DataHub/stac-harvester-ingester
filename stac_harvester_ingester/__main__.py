import copy
import json
import logging
import os
import re
from urllib.parse import urlparse

import boto3
from kubernetes import client  # , config
from pulsar import Client

logging.basicConfig(level=logging.DEBUG, format="%(message)s")


def create_custom_resource(catalogue, bucket_name):
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


def main():
    if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        s3 = session.resource("s3")

    else:
        s3 = boto3.resource("s3")

    pulsar_url = os.environ.get("PULSAR_URL")
    pulsar_client = Client(pulsar_url)
    consumer = pulsar_client.subscribe(
        topic="transformed", subscription_name="stac-harvester-subscription"
    )

    # config.load_kube_config()

    while True:
        msg = consumer.receive()
        try:
            logging.info(f"Parsing harvested message {msg.data()}")

            data = msg.data().decode("utf-8")
            data_dict = json.loads(data.replace("'", '"'))
            bucket_name = data_dict.get("bucket_name")

            output_data = copy.deepcopy(data_dict)
            output_data["added_keys"] = []
            output_data["updated_keys"] = []
            output_data["deleted_keys"] = []

            bucket = s3.Bucket(bucket_name)
            for key in output_data["updated_keys"] + output_data["added_keys"]:
                print("key: " + key)
                for file in bucket.objects.filter(Prefix=key):
                    print("file: " + file)
                    catalogue = json.loads(file.get()["Body"].read())

                    catalogue["pod_name"] = f"stac-harvester-{get_name(catalogue['url'])}"
                    logging.info(f"Pod: {catalogue['pod_name']}")

                    api = client.CustomObjectsApi()
                    resource = create_custom_resource(catalogue, bucket_name)

                    # create the resource
                    try:
                        api.create_namespaced_custom_object(
                            group="core.telespazio-uk.io",
                            version="v1",
                            namespace="workspaces",
                            plural="stac-harvesters",
                            body=resource,
                        )
                        logging.info("Resource created")
                    except client.exceptions.ApiException as e:
                        if e.status == 409:  # already exists
                            logging.info("Resource already exists. Regenerating")
                            resource = api.get_namespaced_custom_object(
                                group="core.telespazio-uk.io",
                                version="v1",
                                name=catalogue["pod_name"],
                                namespace="workspaces",
                                plural="stac-harvesters",
                            )

                            api.delete_namespaced_custom_object(
                                group="core.telespazio-uk.io",
                                version="v1",
                                name=catalogue["pod_name"],
                                namespace="workspaces",
                                plural="stac-harvesters",
                                body=client.V1DeleteOptions(),
                            )

                            api.create_namespaced_custom_object(
                                group="core.telespazio-uk.io",
                                version="v1",
                                namespace="workspaces",
                                plural="stac-harvesters",
                                body=resource,
                            )
                            logging.info("Resource created")
                        else:
                            raise

        except Exception as e:
            logging.error(f"Error occurred during harvest: {e}")
        finally:
            consumer.acknowledge(msg)


if __name__ == "__main__":
    main()
