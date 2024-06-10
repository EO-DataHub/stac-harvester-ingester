import json
import logging
import os
import re
import traceback
from urllib.parse import urlparse

import boto3
from eodhp_utils.pulsar.messages import get_message_data
from kubernetes import client, config
from pulsar import Client

logging.basicConfig(level=logging.DEBUG, format="%(message)s")


def get_name(url):
    parsed = urlparse(url)
    name = parsed.hostname + parsed.path if parsed.path else parsed.hostname
    name = name.replace("/", "-")

    return re.sub(r"[^0-9a-z.-]", r"", name)


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


def create_event_source_spec(catalogue):
    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "EventSource",
        "metadata": {"name": catalogue["event_source_name"], "namespace": "argo-events"},
        "spec": {
            "serviceAccountName": "argo-events",
            "calendar": {"event-schedule": {"schedule": catalogue["schedule"]}},
        },
    }


def create_sensor_spec(catalogue):
    return {
        "apiVersion": "argoproj.io/v1alpha1",
        "kind": "Sensor",
        "metadata": {"name": catalogue["sensor_name"], "namespace": "argo-events"},
        "spec": {
            "template": {"serviceAccountName": "argo-events"},
            "dependencies": [
                {
                    "name": "dependency",
                    "eventSourceName": f"{catalogue['event_source_name']}",
                    "eventName": "event-schedule",
                }
            ],
            "triggers": [
                {
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
                                    "metadata": {"name": catalogue["job_name"], "namespace": "rc"},
                                    "spec": {
                                        "template": {
                                            "spec": {
                                                "serviceAccountName": "resource-catalogue",
                                                "containers": [
                                                    {
                                                        "name": "job",
                                                        "image": "public.ecr.aws/n1b3o1k2/stac-harvester:0.1.3",
                                                        "command": [
                                                            "/bin/sh",
                                                            "-c",
                                                            'harvester_name=`echo "'
                                                            + catalogue["stac_harvester_name"]
                                                            + '"`\nsource_url=`kubectl get stac-harvesters -o jsonpath="{.spec[\'source-url\']}" -n rc $harvester_name`\nworkspace=`kubectl get stac-harvesters -o jsonpath="{.spec.workspace}" -n rc $harvester_name`\ncatalogue_destination=`kubectl get stac-harvesters -o jsonpath="{.spec[\'catalogue-destination\']}" -n rc $harvester_name`\nbucket_name=`kubectl get stac-harvesters -o jsonpath="{.spec[\'bucket-name\']}" -n rc $harvester_name`\n\npython3 -m stac_harvester $source_url $workspace $catalogue_destination $bucket_name\n',
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
                    }
                }
            ],
        },
    }


def create_stac_harvester_custom_resource(api, resource):
    try:
        api.create_namespaced_custom_object(
            group="core.telespazio-uk.io",
            version="v1",
            namespace="rc",
            plural="stac-harvesters",
            body=resource,
        )
        logging.info("STACHarvester custom resource created")
    except client.exceptions.ApiException as e:
        if e.status == 409:  # Already exists. Recreate
            pass
        else:
            raise


def create_event_source(api, event_source_resource):
    try:
        api.create_namespaced_custom_object(
            group="argoproj.io",
            version="v1alpha1",
            namespace="argo-events",
            plural="eventsources",
            body=event_source_resource,
        )
    except client.exceptions.ApiException as e:
        if e.status == 409:
            pass
        else:
            raise


def create_sensor(api, sensor_resource):
    try:
        api.create_namespaced_custom_object(
            group="argoproj.io",
            version="v1alpha1",
            namespace="argo-events",
            plural="sensors",
            body=sensor_resource,
        )
    except client.exceptions.ApiException as e:
        if e.status == 409:
            pass
        else:
            raise


def define_catalogue(contents):
    catalogue = json.loads(contents)
    catalogue["name"] = f"{get_name(catalogue['url'])}"
    catalogue["sensor_name"] = f'sensor-{catalogue["name"]}'
    catalogue["stac_harvester_name"] = f'stac-harvester-{catalogue["name"]}'
    catalogue["job_name"] = f"job-{catalogue['name'].replace('.', '')}"
    catalogue["event_source_name"] = f'event-source-{catalogue["name"].replace(".", "")}'

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

            job_exists = False

            for key in data_dict["added_keys"] + data_dict["updated_keys"]:
                logging.info(f"Key: {key}")
                data = s3.get_object(Bucket=bucket_name, Key=key)
                contents = data["Body"].read().decode("utf-8")
                catalogue = define_catalogue(contents)

                with client.ApiClient() as api_client:
                    api_instance = client.CoreV1Api(api_client)
                    namespace = "rc"
                    all_pods = api_instance.list_namespaced_pod(namespace)
                    all_pod_names = [item.metadata.name for item in all_pods.items]
                    for pod_name in all_pod_names:
                        if pod_name.startswith(catalogue["job_name"]):
                            job_exists = True

                if not job_exists:
                    api = client.CustomObjectsApi()

                    catalogue["workspace"] = workspace
                    catalogue["bucket_name"] = bucket_name

                    logging.info(f"Creating resource: {catalogue['pod_name']}")

                    stac_harvester_resource = create_stac_harvester_custom_resource_spec(catalogue)
                    event_source_resource = create_event_source_spec(catalogue)
                    sensor_resource = create_sensor_spec(catalogue)

                    create_stac_harvester_custom_resource(api, stac_harvester_resource)
                    create_event_source(api, event_source_resource)
                    create_sensor(api, sensor_resource)

        except Exception as e:
            logging.info("Error occurred during harvest")
            traceback.format_exc()
            logging.error(f"Error: {e}")
        finally:
            consumer.acknowledge(msg)


if __name__ == "__main__":
    main()
