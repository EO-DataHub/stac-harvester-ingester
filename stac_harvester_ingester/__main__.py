import json
import logging
import os
import re
import traceback
from contextlib import contextmanager
from urllib.parse import urlparse

import boto3
import kubernetes
from eodhp_utils.pulsar.messages import get_message_data
from kubernetes import client, config
from kubernetes.dynamic.exceptions import NotFoundError
from pulsar import Client
from slugify import slugify

logging.basicConfig(level=logging.DEBUG, format="%(message)s")


def get_name(workspace, url):
    parsed = urlparse(url)
    name = parsed.hostname + parsed.path if parsed.path else parsed.hostname
    name = f"{workspace}-{name}"

    name = slugify(name, max_length=56)  # 64-character limit minus length of "sensor-"

    return re.sub(r"[^0-9a-z-]", r"", name)


def create_stac_harvester_custom_resource_spec(catalogue):
    return {
        "apiVersion": "core.telespazio-uk.io/v1",
        "kind": "STACHarvester",
        "metadata": {"name": catalogue["stac_harvester_name"]},
        "spec": {
            "source-url": catalogue["url"],
            "catalogue-destination": catalogue["path"],
            "workspace": catalogue["workspace"],
            "bucket-name": catalogue["bucket_name"],
        },
    }


def create_event_source_spec(catalogue, uid):
    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "EventSource",
        "metadata": {
            "name": catalogue["event_source_name"],
            "namespace": "rc",
            "ownerReferences": [
                {
                    "apiVersion": "core.telespazio-uk.io/v1",
                    "blockOwnerDeletion": False,
                    "controller": False,
                    "kind": "STACHarvester",
                    "name": catalogue["stac_harvester_name"],
                    "uid": uid,
                }
            ],
        },
        "spec": {
            "eventBusName": "default",
            "serviceAccountName": "resource-catalogue",
            "calendar": {"event-schedule": {"schedule": catalogue["schedule"]}},
        },
    }


def create_sensor_spec(catalogue, uid):
    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Sensor",
        "metadata": {
            "name": catalogue["sensor_name"],
            "namespace": "rc",
            "ownerReferences": [
                {
                    "apiVersion": "core.telespazio-uk.io/v1",
                    "blockOwnerDeletion": False,
                    "controller": False,
                    "kind": "STACHarvester",
                    "name": catalogue["stac_harvester_name"],
                    "uid": uid,
                }
            ],
        },
        "spec": {
            "eventBusName": "default",
            "template": {"serviceAccountName": "resource-catalogue"},
            "dependencies": [
                {
                    "name": "dependency",
                    "eventSourceName": f"{catalogue['event_source_name']}",
                    "eventName": "event-schedule",
                }
            ],
            "triggers": [
                {
                    "rateLimit": {"unit": "Hour", "requestsPerUnit": 10},
                    "template": {
                        "name": "create-job-trigger",
                        "k8s": {
                            "group": "batch",
                            "version": "v1",
                            "resource": "jobs",
                            "operation": "create",
                            "source": {
                                "resource": {
                                    "apiVersion": "batch/v1",
                                    "kind": "Job",
                                    "metadata": {
                                        "generateName": catalogue["job_name"],
                                        "namespace": "rc",
                                    },
                                    "spec": {
                                        "template": {
                                            "spec": {
                                                "serviceAccountName": "resource-catalogue",
                                                "containers": [
                                                    {
                                                        "name": "job",
                                                        "image": os.environ[
                                                            "STAC_HARVESTER_IMAGE_URL"
                                                        ],
                                                        "command": [
                                                            "/bin/sh",
                                                            "-c",
                                                            'harvester_name=`echo "'
                                                            + catalogue["stac_harvester_name"]
                                                            + '"`\nsource_url=`kubectl get stac-harvesters -o jsonpath="{.spec[\'source-url\']}" -n rc $harvester_name`\nworkspace=`kubectl get stac-harvesters -o jsonpath="{.spec.workspace}" -n rc $harvester_name`\ncatalogue_destination=`kubectl get stac-harvesters -o jsonpath="{.spec[\'catalogue-destination\']}" -n rc $harvester_name`\nbucket_name=`kubectl get stac-harvesters -o jsonpath="{.spec[\'bucket-name\']}" -n rc $harvester_name`\n\npython3 -m stac_harvester $source_url $workspace $catalogue_destination $bucket_name\n',  # noqa: E501
                                                        ],
                                                        "env": [
                                                            {
                                                                "name": "PULSAR_URL",
                                                                "value": "pulsar://pulsar-broker.pulsar:6650",
                                                            }
                                                        ],
                                                    }
                                                ],
                                                "restartPolicy": "Never",
                                            }
                                        }
                                    },
                                }
                            },
                        },
                    },
                },
            ],
        },
    }


@contextmanager
def aipipe_resource_dclient(kind, api_version="ai-pipeline.org/v1alpha1"):
    """This returns a context-managed k8s Dynamic Client for the specified CRD"""
    with kubernetes.client.ApiClient() as k8s_client:
        dclient = kubernetes.dynamic.DynamicClient(k8s_client)
        api = dclient.resources.get(api_version=api_version, kind=kind)
        yield api


def update_model(resource, version):
    with aipipe_resource_dclient("STACHarvester", api_version=version) as modelapi:
        try:
            existing_resource = modelapi.get(
                name=resource["metadata"]["name"],
                namespace=resource["metadata"]["namespace"],
            )

            updated_resource = existing_resource.to_dict()

            if updated_resource["spec"] != resource["spec"]:
                updated_resource["spec"] = resource["spec"]

                logging.info("Updating Model spec to %s", updated_resource)

                modelapi.replace(updated_resource)
            else:
                logging.debug("No need to update model %s", resource["metadata"]["name"])
        except NotFoundError:
            logging.info("Creating Model with manifest %s", resource)
            modelapi.create(resource)


def create_stac_harvester_custom_resource(api, resource, catalogue):
    plural = "stac-harvesters"
    namespace = "rc"
    group = "core.telespazio-uk.io"
    version = "v1"

    resource["metadata"]["namespace"] = namespace
    logging.info("Checking STACHarvester resource")
    update_model(resource, version)
    logging.info("Resource check complete")

    return api.get_namespaced_custom_object(
        group=group,
        version=version,
        name=catalogue["stac_harvester_name"],
        namespace=namespace,
        plural=plural,
    )


def create_event_source(api, event_source_resource, catalogue):
    plural = "eventsources"
    namespace = "rc"
    group = "argoproj.io"
    version = "v1alpha1"
    try:
        api.create_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            body=event_source_resource,
        )
        logging.info("Eventsource custom resource created")
    except client.exceptions.ApiException as e:
        if e.status == 409:  # Already exists. Recreate
            logging.info("Eventsource custom resource exists. Recreating")
            api.delete_namespaced_custom_object(
                group=group,
                version=version,
                name=catalogue["event_source_name"],
                namespace=namespace,
                plural=plural,
                body=client.V1DeleteOptions(),
            )

            api.create_namespaced_custom_object(
                group=group,
                version=version,
                namespace=namespace,
                plural=plural,
                body=event_source_resource,
            )
            logging.info("Eventsource custom resource created")
        else:
            raise


def create_sensor(api, resource, catalogue):
    namespace = "rc"
    plural = "sensors"
    group = "argoproj.io"
    version = "v1alpha1"

    try:
        api.create_namespaced_custom_object(
            group=group,
            version=version,
            namespace=namespace,
            plural=plural,
            body=resource,
        )
        logging.info("Sensor custom resource created")
    except client.exceptions.ApiException as e:
        if e.status == 409:
            logging.info("Sensor custom resource exists. Recreating")
            api.delete_namespaced_custom_object(
                group=group,
                version=version,
                name=catalogue["sensor_name"],
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
            logging.info("Sensor custom resource created")
        else:
            raise


def define_catalogue(contents, workspace):
    catalogue = json.loads(contents)
    catalogue["name"] = f"{get_name(workspace, catalogue['url'])}"
    catalogue["sensor_name"] = f'sensor-{catalogue["name"]}'
    catalogue["stac_harvester_name"] = f'sh-{catalogue["name"]}'
    catalogue["job_name"] = f"job-{catalogue['name']}"
    catalogue["event_source_name"] = f'es-{catalogue["name"]}'

    for key in catalogue:
        if "name" in key:
            catalogue[key] = catalogue[key][:63]

    return catalogue


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

    while True:
        msg = consumer.receive()
        try:
            logging.info(f"Parsing harvested message {msg.data()}")

            data_dict = get_message_data(msg)
            bucket_name = data_dict.get("bucket_name")
            workspace = data_dict.get("workspace")

            for key in data_dict["added_keys"] + data_dict["updated_keys"]:
                logging.info(f"Key: {key}")
                data = s3.get_object(Bucket=bucket_name, Key=key)
                contents = data["Body"].read().decode("utf-8")
                catalogue = define_catalogue(contents, workspace)

                api = client.CustomObjectsApi()

                catalogue["workspace"] = workspace
                catalogue["bucket_name"] = bucket_name

                logging.info(f"Creating resource: {catalogue['name']}")

                stac_harvester_resource = create_stac_harvester_custom_resource_spec(catalogue)
                stac_harvester = create_stac_harvester_custom_resource(
                    api, stac_harvester_resource, catalogue
                )

                parent_uid = stac_harvester["metadata"]["uid"]

                event_source_resource = create_event_source_spec(catalogue, parent_uid)
                sensor_resource = create_sensor_spec(catalogue, parent_uid)

                create_event_source(api, event_source_resource, catalogue)
                create_sensor(api, sensor_resource, catalogue)

        except Exception as e:
            logging.info("Error occurred during harvest")
            traceback.format_exc()
            logging.error(f"Error: {e}")
        finally:
            consumer.acknowledge(msg)


if __name__ == "__main__":
    main()
