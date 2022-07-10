# KFPC: Kubeflow Pipelines Components

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Components for Kubeflow Pipelines and Vertex Pipelines.

## Basic Usage

### Components for BigQuery

```python
import kfp
from kfp.v2 import compiler
from google.cloud import aiplatform
import kfpc


@kfp.dsl.pipeline(name="simple")
def pipeline_fn(project: str):
    query_task = kfpc.bigquery.Query(name="query").task(
        job_project=project,
        query="SELECT 1",
        location="US",
        destination_project=project,
        destination_dataset="sandbox",
        destination_table="tmp",
    )


project = "<Your GCP Project>"

compiler.Compiler().compile(pipeline_func=pipeline_fn, package_path="pipeline.json")
job = aiplatform.PipelineJob(
    project=project,
    display_name="simple",
    enable_caching=False,
    template_path="pipeline.json",
    parameter_values={"project": project},
    pipeline_root=f"gs://{project}-vertex/pipeline-root",
)
job.submit()
```
