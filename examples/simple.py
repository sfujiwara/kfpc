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
        location="US",
        destination_project=PROJECT,
        destination_dataset="sandbox",
        destination_table="select1",
    )

    query_select2_task = kfpc.bigquery.Query(name="select-2").task(
        query="SELECT 2",
        job_project=PROJECT,
        location="US",
        destination_project=PROJECT,
        destination_dataset="sandbox",
        destination_table="select2",
    )

    query_select3_task = kfpc.bigquery.Query(name="select-3").task(
        query="SELECT 3",
        job_project=PROJECT,
        location="US",
        destination_project=PROJECT,
        destination_dataset="sandbox",
        destination_table="select3",
        depend_on=[query_select1_task.destination_table, query_select2_task.destination_table],
    )

    extract_task = kfpc.bigquery.Extract(name="extract").task(
        job_project=PROJECT,
        source_table_artifact=query_select3_task.destination_table,
        destination_format="NEWLINE_DELIMITED_JSON",
        location="US",
        output_file_name="sample.jsonl",
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
