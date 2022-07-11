import json
import pathlib
import os
import time
import invoke
from google_cloud_pipeline_components.types.artifact_types import BQTable
from google_cloud_pipeline_components.container.v1.gcp_launcher.utils import artifact_util
from google_cloud_pipeline_components.proto.gcp_resources_pb2 import GcpResources
from google.protobuf.json_format import MessageToJson
import requests
import google.auth
import google.auth.transport.requests


def insert_bigquery_job(payload: dict, project: str):
    creds, _ = google.auth.default()
    creds.refresh(google.auth.transport.requests.Request())

    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {creds.token}",
        "User-Agent": "google-cloud-pipeline-components"
    }

    job = requests.post(
        url=f'https://www.googleapis.com/bigquery/v2/projects/{project}/jobs',
        data=json.dumps(payload),
        headers=headers
    ).json()

    # Wait for the job finishing.
    while True:

        if job["status"]["state"] == "DONE":
            break

        if not creds.valid:
            creds.refresh(google.auth.transport.requests.Request())

        job = requests.get(
            url=job["selfLink"],
            headers={
                "Content-type": "application/json",
                "Authorization": f"Bearer {creds.token}",
            }
        ).json()

        time.sleep(3)

    print(job)
    return job


@invoke.task
def query(
    c,
    job_project,
    query,
    destination_project,
    destination_dataset,
    destination_table,
    location="US",
    query_params="[]",
    labels="{}",
    create_disposition="CREATE_IF_NEEDED",
    write_disposition="WRITE_TRUNCATE",
    executor_input='{"outputs": {"outputFile": "tmp/executor_input.json"}}',
    gcp_resources="tmp/gcp_resources.json"
):
    """
    Execute BigQuery query job.

    Parameters
    ----------
    c:
        Invoke context.
    job_project:
        Google Cloud Platform project where the job is executed.
    query:
        SQL string executed on BigQuery.
    query_params:
        JSON string for query parameters.
    labels:
        JSON string for labels.
    create_disposition:
        createDisposition of JobConfigurationQuery.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationquery
    write_disposition:
        writeDisposition of JobConfigurationQuery.
        https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#jobconfigurationquery
    location:
        Location of the dataset that will be queried.
    destination_project:
        Google Cloud Platform project ID of destination.
    destination_dataset:
        BigQuery dataset ID of destination.
    destination_table:
        BigQuery table ID of destination.
    executor_input:
        Automatically passed by Kubeflow Pipelines.
    gcp_resources:
    """
    payload = {
        "configuration": {
            "query": {
                "query": query,
                "destinationTable": {
                    "projectId": destination_project,
                    "datasetId": destination_dataset,
                    "tableId": destination_table,
                },
                "createDisposition": create_disposition,
                "writeDisposition": write_disposition,
                "queryParameters": json.loads(query_params),
            },
            "labels": json.loads(labels),
        },
        "jobReference": {
            "projectId": job_project,
            "location": location,
        }
    }

    job = insert_bigquery_job(payload=payload, project=job_project)

    # Write BQTable artifact.
    bq_table_artifact = BQTable(
        name="destination_table",
        project_id=destination_project,
        dataset_id=destination_dataset,
        table_id=destination_table,
    )
    artifact_util.update_output_artifacts(executor_input, [bq_table_artifact])

    # Write GCP resources.
    bq_resources = GcpResources()
    b = bq_resources.resources.add()
    b.resource_type = "BigQueryJob"
    b.resource_uri = job["selfLink"]

    pathlib.Path(gcp_resources).parent.mkdir(parents=True, exist_ok=True)

    with open(gcp_resources, "w") as f:
        f.write(MessageToJson(bq_resources))


@invoke.task
def extract_artifact(
    c,
    job_project,
    table_uri,
    output_uri,
    output_file_name,
    location="US",
    labels="{}",
    destination_format="NEWLINE_DELIMITED_JSON",
    executor_input='{"outputs": {"outputFile": "tmp/executor_input.json"}}',
    gcp_resources="tmp/gcp_resources.json",
):
    """
    Parameters
    ----------
    table_uri:
        URI of BigQuery table as below:
        https://www.googleapis.com/bigquery/v2/projects/<PROJECT_ID>/datasets/<DATASET_ID>/tables/<TABLE_ID>
    output_uri:
        Google Cloud Storage URI
        gs://hoge/fuga
    gcp_resources:
        Path to which Google Cloud Platform resource information is saved.
    """
    project_id = table_uri.split("/")[-5]
    dataset_id = table_uri.split("/")[-3]
    table_id = table_uri.split("/")[-1]

    payload = {
        "configuration": {
            "extract": {
                "destinationUris": [os.path.join(output_uri, output_file_name)],
                # "printHeader": True,
                # "fieldDelimiter": ",",
                "destinationFormat": destination_format,
                "compression": "NONE",
                # "useAvroLogicalTypes": True,
                "sourceTable": {
                    "projectId": project_id,
                    "datasetId": dataset_id,
                    "tableId": table_id,
                },
            },
            "labels": json.loads(labels),
        },
        "jobReference": {
            "projectId": job_project,
            "location": location,
        }
    }

    job = insert_bigquery_job(payload=payload, project=job_project)

    # Write GCP resources.
    bq_resources = GcpResources()
    b = bq_resources.resources.add()
    b.resource_type = "BigQueryJob"
    b.resource_uri = job["selfLink"]

    pathlib.Path(gcp_resources).parent.mkdir(parents=True, exist_ok=True)

    with open(gcp_resources, "w") as f:
        f.write(MessageToJson(bq_resources))
