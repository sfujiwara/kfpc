"""Invoke tasks."""

import os
from pathlib import Path

import invoke
import toml
from google.cloud import aiplatform
from kfp.v2 import compiler

from .pipeline import pipeline_fn


def get_version() -> str:
    """Get version from `pyproject.toml`."""
    with Path("pyproject.toml").open() as f:
        pyproject = toml.load(f)

    return pyproject["tool"]["poetry"]["version"]


@invoke.task
def docker_build(c: invoke.Context) -> None:
    """Build Docker image."""
    os.environ["VERSION"] = get_version()
    c.run("docker compose build")


@invoke.task
def docker_push(c: invoke.Context) -> None:
    """Build and push Docker image."""
    c.run("docker compose build")
    c.run("docker compose push")

    os.environ["VERSION"] = get_version()
    c.run("docker compose build")
    c.run("docker compose push")


@invoke.task
def docs_build(c: invoke.Context) -> None:
    """Generate documentations using Sphinx."""
    with c.cd("sphinx"):
        c.run("make html")

@invoke.task
def pipeline_run(c: invoke.Context, project: str) -> None:  # noqa: ARG001
    """Run example pipeline."""
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
