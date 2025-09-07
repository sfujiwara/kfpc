from __future__ import annotations

import json
import time
from pathlib import Path

import google.auth
import google.auth.transport.requests
import invoke
import requests
from google.cloud import bigquery
from google.protobuf.json_format import MessageToJson
from google_cloud_pipeline_components.container.utils import artifact_utils
from google_cloud_pipeline_components.proto.gcp_resources_pb2 import GcpResources
from google_cloud_pipeline_components.types.artifact_types import BQTable


def insert_bigquery_job(payload: dict, project: str) -> requests.Response:
    """Insert BigQuery job using REST API."""
    creds, _ = google.auth.default()
    creds.refresh(google.auth.transport.requests.Request())

    headers = {
        "Content-type": "application/json",
        "Authorization": f"Bearer {creds.token}",
        "User-Agent": "google-cloud-pipeline-components",
    }

    job = requests.post(
        url=f"https://www.googleapis.com/bigquery/v2/projects/{project}/jobs",
        data=json.dumps(payload),
        headers=headers,
        timeout=90,
    ).json()

    # Wait for the job finishing.
    while True:

        if job["status"]["state"] == "DONE":
            if "errorResult" in job["status"]:
                raise Exception(job["status"]["errorResult"])
            break

        if not creds.valid:
            creds.refresh(google.auth.transport.requests.Request())

        job = requests.get(
            url=job["selfLink"],
            headers={
                "Content-type": "application/json",
                "Authorization": f"Bearer {creds.token}",
            },
            timeout=90,
        ).json()

        time.sleep(3)

    return job


@invoke.task
def query(
    c: invoke.Context,  # noqa: ARG001
    job_project: str,
    query: str,
    destination_project: str,
    destination_dataset: str,
    destination_table: str,
    location: str = "US",
    query_params: str = "[]",
    labels: str = "{}",
    create_disposition: str = "CREATE_IF_NEEDED",
    write_disposition: str = "WRITE_TRUNCATE",
    executor_input: str = '{"outputs": {"outputFile": "tmp/executor_input.json"}}',
    gcp_resources: str = "tmp/gcp_resources.json",
) -> None:
    """Execute BigQuery query job.

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
        GCP resources output path.

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
                "useLegacySql": False,
            },
            "labels": json.loads(labels),
        },
        "jobReference": {
            "projectId": job_project,
            "location": location,
        },
    }

    job = insert_bigquery_job(payload=payload, project=job_project)

    # Write BQTable artifact.
    bq_table_artifact = BQTable.create(
        name="destination_table",
        project_id=destination_project,
        dataset_id=destination_dataset,
        table_id=destination_table,
    )
    artifact_utils.update_output_artifacts(executor_input, [bq_table_artifact])

    # Write GCP resources.
    bq_resources = GcpResources()
    b = bq_resources.resources.add()
    b.resource_type = "BigQueryJob"
    b.resource_uri = job["selfLink"]

    Path(gcp_resources).parent.mkdir(parents=True, exist_ok=True)

    with Path(gcp_resources).open("w") as f:
        f.write(MessageToJson(bq_resources))


@invoke.task
def extract(
    c: invoke.Context,  # noqa: ARG001
    job_project: str,
    source_project: str,
    source_dataset: str,
    source_table: str,
    destination_uri: str,
    location: str = "US",
    executor_input: str = '{"outputs": {"outputFile": "tmp/executor_input.json"}}',
) -> None:
    """Execute BigQuery extract job."""
    client = bigquery.Client()
    job = client.extract_table(
        project=job_project,
        source=f"{source_project}.{source_dataset}.{source_table}",
        destination_uris=f"{destination_uri.rstrip('/')}/data-*.jsonl",
        location=location,
        job_config=bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
        ),
    )
    job.result()


@invoke.task
def extract_artifact(
    c: invoke.Context,  # noqa: ARG001
    job_project: str,
    table_uri: str,
    destination_uri: str,
    location: str = "US",
    executor_input: str = '{"outputs": {"outputFile": "tmp/executor_input.json"}}',
) -> None:
    """Execute BigQuery extract job."""
    # `table_uri` is
    # https://www.googleapis.com/bigquery/v2/projects/<PROJECT_ID>/datasets/<DATASET_ID>/tables/<TABLE_ID>
    source_project = table_uri.split("/")[-5]
    source_dataset = table_uri.split("/")[-3]
    source_table = table_uri.split("/")[-1]

    client = bigquery.Client()
    job = client.extract_table(
        project=job_project,
        source=f"{source_project}.{source_dataset}.{source_table}",
        destination_uris=f"{destination_uri.rstrip('/')}/data-*.jsonl",
        location=location,
        job_config=bigquery.ExtractJobConfig(
            destination_format=bigquery.DestinationFormat.NEWLINE_DELIMITED_JSON,
        ),
    )
    job.result()


@invoke.task
def load(
    c: invoke.Context,  # noqa: ARG001
    job_project: str,
    destination_project: str,
    destination_dataset: str,
    destination_table: str,
    schema: str,
    source_uri: str,
    source_uri_suffix: str | None = None,
    location: str = "US",
    executor_input: str = '{"outputs": {"outputFile": "tmp/executor_input.json"}}',
) -> None:
    """Execute BigQuery load job."""
    if source_uri_suffix:
        source_uri = f"{source_uri.rstrip('/')}/{source_uri_suffix.lstrip('/')}"

    client = bigquery.Client()
    job = client.load_table_from_uri(
        project=job_project,
        source_uris=source_uri,
        destination=f"{destination_project}.{destination_dataset}.{destination_table}",
        location=location,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            schema=json.loads(schema),
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ),
    )
    job.result()

    # Write BQTable artifact.
    bq_table_artifact = BQTable.create(
        name="destination_table",
        project_id=destination_project,
        dataset_id=destination_dataset,
        table_id=destination_table,
    )
    artifact_utils.update_output_artifacts(executor_input, [bq_table_artifact])
