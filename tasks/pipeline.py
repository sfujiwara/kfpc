"""A simple pipeline."""

import kfp
from google_cloud_pipeline_components.types.artifact_types import BQTable

import kfpc


@kfp.dsl.pipeline(name="simple")
def pipeline_fn(project: str) -> None:
    """Pipeline function."""
    query_select1_task = kfpc.bigquery.Query(name="select-1").task(
        query="SELECT 1",
        job_project=project,
        destination_project=project,
        destination_dataset="sandbox",
        destination_table="select1",
    )

    table_importer_task = kfp.dsl.importer(
        artifact_uri="https://www.googleapis.com/bigquery/v2/projects/bigquery-public-data/datasets/samples/tables/shakespeare",
        artifact_class=BQTable,
        reimport=True,
        metadata={"projectId": "bigquery-public-data", "datasetId": "samples", "tableId": "shakespeare"},
    ).set_display_name("table-importer")

    _ = kfpc.bigquery.Query(name="select-2").task(
        query="SELECT 2",
        job_project=project,
        destination_project=project,
        destination_dataset="sandbox",
        destination_table="select2",
        depend_on=[
            query_select1_task.destination_table,
            table_importer_task.output,
        ],
    )

    extract_task = kfpc.bigquery.Extract(name="extract").task(
        job_project=project,
        source_table_artifact=table_importer_task.output,
    )

    _ = kfpc.bigquery.Load(name="load").task(
        job_project=project,
        source_artifact=extract_task.output_files,
        destination_project=project,
        destination_dataset="sandbox",
        destination_table="load",
        schema=[
            {"name": "word", "type": "STRING"},
            {"name": "word_count", "type": "INTEGER"},
            {"name": "corpus", "type": "STRING"},
            {"name": "corpus_date", "type": "INTEGER"},
        ],
        source_uri_suffix="data-*.jsonl",
    )
