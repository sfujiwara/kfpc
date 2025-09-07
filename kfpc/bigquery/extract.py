from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kfp.dsl.pipeline_channel import PipelineArtifactChannel, PipelineParameterChannel
    from kfp.dsl.pipeline_task import PipelineTask

import yaml
from kfp.components import load_component_from_text

from kfpc.version import get_version


class ExtractTask:
    """Kubeflow Pipelines task for BigQuery extract job."""

    def __init__(self, task: PipelineTask) -> None:
        """Initialize ``ExtractTask`` instance."""
        self.task = task

    @property
    def output_files(self) -> PipelineArtifactChannel:
        """Return output_files artifact."""
        return self.task.outputs["output_files"]


class Extract:
    """Kubeflow Pipelines component for BigQuery extract job.

    Parameters
    ----------
    name:
        Name of the component.

    """

    def __init__(self, name: str = "extract") -> None:
        """Initialize ``Extract`` instance."""
        self.name = name

    def task(
        self,
        job_project: PipelineParameterChannel | str,
        location: PipelineParameterChannel | str,
        source_project_id: str | None = None,
        source_dataset_id: str | None = None,
        source_table_id: str | None = None,
    ) -> ExtractTask:
        """Generate Kubeflow Pipelines task to submit BigQuery extract job.

        Parameters
        ----------
        job_project:
            Google Cloud Platform project ID to execute query job.
        location:
            Location of BigQuery sources.
        source_project_id:
            Google Cloud Platform project ID of the source table.
        source_dataset_id:
            BigQuery dataset ID of the source table.
        source_table_id:
            BigQuery table ID of the source table.

        Returns
        -------
        self

        """
        component_dict = {
            "name": self.name,
            "inputs": [
                {"name": "job_project", "type": "String"},
                {"name": "destination_format", "type": "String"},
                {"name": "location", "type": "String"},
                {"name": "source_project_id", "type": "String"},
                {"name": "source_dataset_id", "type": "String"},
                {"name": "source_table_id", "type": "String"},
                {"name": "output_file_name", "type": "String"},
            ],
            "outputs": [
                {"name": "output_files", "type": "Artifact"},
                {"name": "gcp_resources", "type": "String"},
            ],
            "implementation": {
                "container": {
                    "image": f"us-central1-docker.pkg.dev/sfujiwara/kfpc/bigquery:{get_version()}",
                    "command": ["inv", "extract"],
                    "args": [
                        "--job-project", {"inputValue": "job_project"},
                        "--location", {"inputValue": "location"},
                        "--source-project", {"inputValue": "source_project_id"},
                        "--source-dataset", {"inputValue": "source_dataset_id"},
                        "--source-table", {"inputValue": "source_table_id"},
                        "--destination-uri", {"outputUri": "output_files"},
                        "--gcp-resources", {"outputPath": "gcp_resources"},
                        "--executor-input", {"executorInput": None},
                    ],
                },
            },
        }

        component = load_component_from_text(yaml.dump(component_dict))
        task = component(
            job_project=job_project,
            source_project_id=source_project_id,
            source_dataset_id=source_dataset_id,
            source_table_id=source_table_id,
            location=location,
        )

        return ExtractTask(task=task)
