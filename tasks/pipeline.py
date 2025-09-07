"""A simple pipeline."""

import kfp.dsl

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

    query_select2_task = kfpc.bigquery.Query(name="select-2").task(
        query="SELECT 2",
        job_project=project,
        destination_project=project,
        destination_dataset="sandbox",
        destination_table="select2",
    )

    query_select3_task = kfpc.bigquery.Query(name="select-3").task(
        query="SELECT 3",
        job_project=project,
        destination_project=project,
        destination_dataset="sandbox",
        destination_table="select3",
        depend_on=[
            query_select1_task.destination_table,
            query_select2_task.destination_table,
        ],
    )

    extract_task = kfpc.bigquery.ExtractArtifact(name="extract-artifact").task(
        job_project=project,
        source_table_artifact=query_select3_task.destination_table,
    )

    _ = kfpc.bigquery.Load(name="load").task(
        job_project=project,
        source_artifact=extract_task.output_files,
        destination_project=project,
        destination_dataset="sandbox",
        destination_table="load",
        schema=[{"name": "f0_", "type": "INTEGER"}],
        source_uri_suffix="data-*.jsonl",
    )
