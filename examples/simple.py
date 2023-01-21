import argparse
from kfp.v2 import compiler
from google.cloud import aiplatform
import kfpc
import kfp.dsl


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", type=str, required=True)
    args = parser.parse_args()
    return args


@kfp.dsl.pipeline(name="simple")
def pipeline_fn(project: str):
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
        depend_on=[query_select1_task.destination_table, query_select2_task.destination_table],
    )

    extract_task = kfpc.bigquery.ExtractArtifact(name="extract").task(
        job_project=project,
        source_table_artifact=query_select3_task.destination_table,
    )

    load_artifact_task = kfpc.bigquery.Load(name="load").task(
        job_project=project,
        source_artifact=extract_task.output_files,
        destination_project=project,
        destination_dataset="sandbox",
        destination_table="load",
        schema=[{"name": "f0_", "type": "INTEGER"}],
        source_uri_suffix="data-*.jsonl",
    )


def main():
    args = parse()
    project = args.project

    compiler.Compiler().compile(pipeline_func=pipeline_fn, package_path="pipeline.yaml")
    job = aiplatform.PipelineJob(
        project=project,
        display_name="simple",
        enable_caching=False,
        template_path="pipeline.yaml",
        parameter_values={"project": project},
        pipeline_root=f"gs://{project}-vertex-ai/pipeline-root",
    )
    job.submit()


if __name__ == "__main__":
    main()
