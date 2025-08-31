from __future__ import annotations

from typing import TYPE_CHECKING

import yaml
from kfp.components import load_component_from_text

if TYPE_CHECKING:
    from kfp.dsl.pipeline_channel import PipelineArtifactChannel, PipelineParameterChannel
    from kfp.dsl.pipeline_task import PipelineTask

from kfpc.version import get_version


class LoadTask:
    """Kubeflow Pipelines task for BigQuery load job."""

    def __init__(self, task: PipelineTask) -> None:
        """Initialize ``LoadTask`` instance."""
        self.task = task

    @property
    def destination_table(self) -> PipelineArtifactChannel:
        """Return destination_table artifact."""
        return self.task.outputs["destination_table"]


class Load:
    """Kubeflow Pipelines component for BigQuery load job.

    Parameters
    ----------
    name:
        Name of the component.

    """

    def __init__(self, name: str) -> None:
        """Initialize ``Load`` instance."""
        self.name = name

    def task(
        self,
        job_project: PipelineParameterChannel | str,
        destination_project: PipelineParameterChannel | str,
        destination_dataset: PipelineParameterChannel | str,
        destination_table: PipelineParameterChannel | str,
        schema: PipelineParameterChannel | list[dict],
        source_artifact: PipelineArtifactChannel,
        source_uri_suffix: str = "",
        location: str = "US",
    ) -> LoadTask:
        """Generate a Kubeflow Pipelines task to execute BigQuery load job.

        Parameters
        ----------
        job_project:
            Google Cloud Platform project ID to execute load job.
        source_artifact:
        destination_project:
            Google Cloud Platform project ID of the destination table.
        destination_dataset:
            BigQuery dataset ID of the destination table.
        destination_table:
            BigQuery table ID of the destination table.
        schema:
            BigQuery table schema of the destination table.
        source_artifact:
            Source artifact to be loaded.
        source_uri_suffix:
            Load files matched to ``os.path.join(source_uri, source_uri_suffix)``.
            ``source_uri`` is Kubeflow Pipelines placeholder ``inputPath`` of ``source_artifact``.
        location:
            Location of BigQuery destination table.

        """
        component_dict = {
            "name": self.name,
            "inputs": [
                {"name": "job_project", "type": "String"},
                {"name": "schema", "type": "JsonArray"},
                {"name": "location", "type": "String"},
                {"name": "source_uri_suffix", "type": "String"},
                {"name": "source_artifact", "type": "Artifact"},
                {"name": "destination_project", "type": "String"},
                {"name": "destination_dataset", "type": "String"},
                {"name": "destination_table", "type": "String"},
            ],
            "outputs": [
                {"name": "destination_table", "type": "google.BQTable"},
            ],
            "implementation": {
                "container": {
                    "image": f"gcr.io/sfujiwara/kfpc/bigquery:{get_version()}",
                    "command": ["inv", "load"],
                    "args": [
                        "--job-project", {"inputValue": "job_project"},
                        "--location", {"inputValue": "location"},
                        "--source-uri", {"inputUri": "source_artifact"},
                        "--source-uri-suffix", {"inputValue": "source_uri_suffix"},
                        "--schema", {"inputValue": "schema"},
                        "--destination-project", {"inputValue": "destination_project"},
                        "--destination-dataset", {"inputValue": "destination_dataset"},
                        "--destination-table", {"inputValue": "destination_table"},
                        "--executor-input", {"executorInput": None},
                    ],
                },
            },
        }

        component = load_component_from_text(yaml.dump(component_dict))
        return component(
            job_project=job_project,
            source_artifact=source_artifact,
            destination_project=destination_project,
            destination_dataset=destination_dataset,
            destination_table=destination_table,
            location=location,
            schema=schema,
            source_uri_suffix=source_uri_suffix,
        )
