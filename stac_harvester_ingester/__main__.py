import argparse
import json
import logging
import os
import re
import urllib
from urllib.parse import urlparse

import boto3
from kubernetes import client, config
from pulsar import Client, Message

parser = argparse.ArgumentParser()
parser.add_argument("--s3_bucket", help="Configuration bucket", type=str)
parser.add_argument("--s3_prefix", help="Prefix", type=str)
args = parser.parse_args()

logging.basicConfig(level=logging.DEBUG, format="%(message)s")


def create_crd():
    crd = {
        "apiVersion": "apiextensions.k8s.io/apiextensions.k8s.io/v1",
        "kind": "CustomResourceDefinition",
        "metadata": {
            "annotations": {"controller-gen.kubebuilder.io/version": "v0.14.0"},
            "name": "stac-harvesters.core.telespazio-uk.io.core.telespazio-uk.io",
        },
        "spec": {
            "group": "core.telespazio-uk.io",
            "versions": [
                {
                    "name": "apiextensions.k8s.io/v1alpha1",
                    "schema": {
                        "openAPIV3Schema": {
                            "description": "STACHarvester is the Schema for the harvester API",
                            "properties": {
                                "apiVersion": {
                                    "description": "APIVersion defines the versioned schema of this representation of an object.\nServers should convert recognized schemas to the latest internal value, and\nmay reject unrecognized values.\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#resources",
                                    "type": "string",
                                },
                                "kind": {
                                    "description": "Kind is a string value representing the REST resource this object represents.\nServers may infer this from the endpoint the client submits requests to.\nCannot be updated.\nIn CamelCase.\nMore info: https://git.k8s.io/community/contributors/devel/sig-architecture/api-conventions.md#types-kinds",
                                    "type": "string",
                                },
                                "metadata": {"type": "object"},
                                "spec": {
                                    "description": "HarvesterSpec defines the desired state of Workspace",
                                    "properties": {
                                        "image": {
                                            "description": "stac-harvester image",
                                            "type": "string",
                                        }
                                    },
                                    "type": "object",
                                },
                                "status": {
                                    "description": "STACHarvesterStatus defines the observed state of STACHarvester",
                                    "type": "object",
                                },
                            },
                            "type": "object",
                        }
                    },
                    "served": True,
                    "storage": True,
                    "subresources": {"status": {}},
                }
            ],
            "names": {
                "kind": "STACHarvester",
                "listKind": "STACHarvesterList",
                "plural": "stac-harvesters",
                "singular": "stac-harvester",
            },
            "scope": "Namespaced",
        },
    }

    api_instance = client.Apiextensionsv1Api()
    try:
        response = api_instance.create_custom_resource_definition(body=crd)
        logging.info("Created STACHarvester CRD")
        logging.info(response.status)
    except client.exceptions.ApiException as e:
        if e.status == 409:  # conflict - resource already exists
            logging.info("STACHarvester CRD already exists")
        else:
            raise
    except Exception as e:
        logging.error("Unable to create STACHarvester CRD")
        raise


def create_custom_resource(catalogue):
    return {
        "apiVersion": "core.telespazio-uk.io/v1",
        "kind": "STACHarvester",
        "metadata": {"name": catalogue["pod_name"]},
        "spec": {
            "command": f"/bin/sh -c python3 -m stac_harvester {catalogue['url']} {os.environ['WORKSPACE_NAME']} {catalogue['path']} {os.environ['CONFIGURATION_BUCKET']} {catalogue['limit']}",
            "image": "my-awesome-cron-image",
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
    client = Client(pulsar_url)
    consumer = client.subscribe(topic="transformed", subscription_name="stac-harvester-subscription")

    config.load_kube_config()

    while True:
        msg = consumer.receive()
        try:
            logging.info(f"Parsing harvested message {msg.data()}")

            bucket = s3.Bucket(args.s3_bucket)
            for file in bucket.objects.filter(Prefix=args.s3_prefix):
                catalogue = json.loads(file.get()["Body"].read())

                catalogue["pod_name"] = f"stac-harvester-{get_name(catalogue['url'])}"

                create_custom_resource(catalogue)

                api = client.CustomObjectsApi()
                resource = create_custom_resource(catalogue)

                # create the resource
                try:
                    api.create_namespaced_custom_object(
                        group="core.telespazio-uk.io",
                        version="v1",
                        namespace="workspaces",
                        plural="stac-harvesters",
                        body=resource,
                    )
                    print("Resource created")
                except client.exceptions.ApiException as e:
                    if e.status == 409:  # already exists
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
                        print("Resource created")
                    else:
                        raise

        except Exception as e:
            logging.error(f"Error occurred during harvest: {e}")
        finally:
            consumer.acknowledge(msg)


if __name__ == "__main__":
    main()
