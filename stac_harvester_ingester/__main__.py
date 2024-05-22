import json
import logging
import os
import re
import traceback
from urllib.parse import urlparse

import boto3
from eodhp_utils.pulsar.messages import get_data
from kubernetes import client, config
from pulsar import Client

logging.basicConfig(level=logging.DEBUG, format="%(message)s")


def get_name(url):
    parsed = urlparse(url)
    name = parsed.hostname + parsed.path if parsed.path else parsed.hostname
    name = name.replace("/", "-")

    return re.sub(r"[^0-9a-z.-]", r"", name)


def create_custom_resource(catalogue):
    return {
        "apiVersion": "core.telespazio-uk.io/v1",
        "kind": "STACHarvester",
        "metadata": {"name": catalogue["pod_name"]},
        "spec": {
            "source-url": catalogue["url"],
            "catalogue-destination": catalogue["path"],
            "workspace": catalogue["workspace"],
            "bucket-name": catalogue["bucket_name"],
        },
    }


def main():
    config.load_incluster_config()

    if os.getenv("AWS_ACCESS_KEY") and os.getenv("AWS_SECRET_ACCESS_KEY"):
        session = boto3.session.Session(
            aws_access_key_id=os.environ["AWS_ACCESS_KEY"],
            aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        )
        s3 = session.client("s3")

    else:
        s3 = boto3.client("s3")

    if os.getenv("TOPIC"):
        identifier = "_" + os.getenv("TOPIC")
    else:
        identifier = ""

    pulsar_url = os.environ.get("PULSAR_URL")
    pulsar_client = Client(pulsar_url)
    consumer = pulsar_client.subscribe(
        topic=f"transformed{identifier}", subscription_name=f"harvester-ingester{identifier}"
    )

    plural = "stac-harvesters"
    namespace = os.environ["NAMESPACE_NAME"]
    group = "core.telespazio-uk.io"
    version = "v1"

    while True:
        msg = consumer.receive()
        try:
            logging.info(f"Parsing harvested message {msg.data()}")

            data_dict = get_data(msg)
            bucket_name = data_dict.get("bucket_name")
            workspace = data_dict.get("workspace")

            for key in data_dict["added_keys"] + data_dict["updated_keys"]:
                logging.info(f"Key: {key}")
                data = s3.get_object(Bucket=bucket_name, Key=key)
                contents = data["Body"].read().decode("utf-8")
                catalogue = json.loads(contents)

                api = client.CustomObjectsApi()

                catalogue["pod_name"] = f"stac-harvester-{get_name(catalogue['url'])}"
                catalogue["workspace"] = workspace
                catalogue["bucket_name"] = bucket_name

                logging.info(f"Creating resource: {catalogue['pod_name']}")

                resource = create_custom_resource(catalogue)

                try:
                    api.create_namespaced_custom_object(
                        group=group,
                        version=version,
                        namespace=namespace,
                        plural=plural,
                        body=resource,
                    )
                    logging.info("Resource created")
                except client.exceptions.ApiException as e:
                    if e.status == 409:  # Already exists. Recreate
                        api.delete_namespaced_custom_object(
                            group=group,
                            version=version,
                            name=catalogue["pod_name"],
                            namespace=namespace,
                            plural=plural,
                            body=client.V1DeleteOptions(),
                        )
                        api.create_namespaced_custom_object(
                            group=group,
                            version=version,
                            namespace=namespace,
                            plural=plural,
                            body=resource,
                        )
                        logging.info("Resource created")
                    else:
                        raise
        except Exception as e:
            logging.info("Error occurred during harvest")
            traceback.format_exc()
            logging.error(f"Error: {e}")
        finally:
            consumer.acknowledge(msg)


if __name__ == "__main__":
    main()
