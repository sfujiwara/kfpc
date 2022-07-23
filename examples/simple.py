import argparse
import kfp
from kfp.v2 import compiler
from google.cloud import aiplatform
import kfpc


parser = argparse.ArgumentParser()
parser.add_argument("--project", type=str, required=True)
args = parser.parse_args()

PROJECT = args.project


@kfp.dsl.pipeline(name="simple")
def pipeline_fn():
    query_select1_task = kfpc.bigquery.Query(name="select-1").task(
        query="SELECT 1",
        job_project=PROJECT,
        destination_project=PROJECT,
        destination_dataset="sandbox",
        destination_table="select1",
    )

    query_select2_task = kfpc.bigquery.Query(name="select-2").task(
        query="SELECT 2",
        job_project=PROJECT,
        destination_project=PROJECT,
        destination_dataset="sandbox",
        destination_table="select2",
    )

    query_select3_task = kfpc.bigquery.Query(name="select-3").task(
        query="SELECT 3",
        job_project=PROJECT,
        destination_project=PROJECT,
        destination_dataset="sandbox",
        destination_table="select3",
        depend_on=[query_select1_task.destination_table, query_select2_task.destination_table],
    )

    extract_task = kfpc.bigquery.ExtractArtifact(name="extract").task(
        job_project=PROJECT,
        source_table_artifact=query_select3_task.destination_table,
    )

    load_artifact_task = kfpc.bigquery.Load(name="load").task(
        job_project=PROJECT,
        source_artifact=extract_task.output_files,
        destination_project=PROJECT,
        destination_dataset="sandbox",
        destination_table="load",
        schema=[{"name": "f0_", "type": "INTEGER"}],
        source_uri_suffix="out-*.jsonl",
    )


def main():
    compiler.Compiler().compile(pipeline_func=pipeline_fn, package_path="pipeline.json")
    job = aiplatform.PipelineJob(
        project=PROJECT,
        display_name="simple",
        enable_caching=False,
        template_path="pipeline.json",
        parameter_values={},
        pipeline_root=f"gs://{PROJECT}-vertex-ai/pipeline-root",
    )
    job.submit()


if __name__ == "__main__":
    main()
