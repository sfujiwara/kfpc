import kfp
from kfp.v2 import compiler
from google.cloud import aiplatform
import kfpc


PROJECT = ""


@kfp.dsl.pipeline(name="simple")
def pipeline_fn():
    kfpc.bigquery.Query(name="query").task(
        query="SELECT 1",
        job_project=PROJECT,
        location="US",
        destination_project=PROJECT,
        destination_dataset="sandbox",
        destination_table="tmp",
    )


if __name__ == "__main__":
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
