"""Invoke tasks."""

from __future__ import annotations

from pathlib import Path

import invoke
import kfp
import toml
from google.cloud import aiplatform

from .pipeline import pipeline_fn


def get_version() -> str:
    """Get version from `pyproject.toml`."""
    with Path("pyproject.toml").open() as f:
        pyproject = toml.load(f)

    return pyproject["tool"]["poetry"]["version"]


@invoke.task
def docker_build(c: invoke.Context) -> None:
    """Build Docker image in local environment."""
    c.run("docker build -t kfpc-bigquery ./containers/bigquery")


@invoke.task
def docker_cloudbuild(c: invoke.Context, docker_tag: str | None = None) -> None:
    """Build and push Docker image via Cloud Build."""
    project = c["env"]["project"]
    if docker_tag is None:
        docker_tag = get_version()
    c.run(f"gcloud builds submit --project {project} --config cloudbuild.yaml --substitutions _DOCKER_TAG={docker_tag}")


@invoke.task
def docs_build(c: invoke.Context) -> None:
    """Generate documentations using Sphinx."""
    with c.cd("sphinx"):
        c.run("make html")


@invoke.task
def pipeline_run(c: invoke.Context, project: str) -> None:  # noqa: ARG001
    """Run example pipeline."""
    kfp.compiler.Compiler().compile(pipeline_func=pipeline_fn, package_path="pipeline.yaml")
    job = aiplatform.PipelineJob(
        project=project,
        display_name="simple",
        enable_caching=False,
        template_path="pipeline.yaml",
        parameter_values={"project": project},
        pipeline_root=f"gs://{project}-vertex-ai/pipeline-root",
    )
    job.submit()
