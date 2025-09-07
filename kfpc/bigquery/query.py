from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from kfp.dsl.pipeline_channel import PipelineArtifactChannel, PipelineParameterChannel
    from kfp.dsl.pipeline_task import PipelineTask

import yaml
from kfp.components import load_component_from_text

from kfpc.version import get_version


class QueryTask:
    """Kubeflow Pipelines task for BigQuery query job."""

    def __init__(self, task: PipelineTask) -> None:
        """Initialize ``QueryTask``."""
        self.task = task

    @property
    def destination_table(self) -> PipelineArtifactChannel:
        """Return destination_table artifact."""
        return self.task.outputs["destination_table"]

    @property
    def gcp_resources(self) -> PipelineArtifactChannel:
        """Return gcp_resources artifact."""
        return self.task.outputs["gcp_resources"]


class Query:
    """Kubeflow Pipelines component for BigQuery query job.

    Parameters
    ----------
    name:
        Name of the component.

    """

    def __init__(self, name: str) -> None:
        """Initialize ``Query`` instance."""
        self.name = name

    def task(
        self,
        query: PipelineParameterChannel | str,
        job_project: PipelineParameterChannel | str,
        destination_project: PipelineParameterChannel | str,
        destination_dataset: PipelineParameterChannel | str,
        destination_table: PipelineParameterChannel | str,
        location: PipelineParameterChannel | str = "US",
        depend_on: list[PipelineArtifactChannel] | None = None,
    ) -> QueryTask:
        """Generate a Kubeflow Pipelines task.

        Parameters
        ----------
        query:
            SQL string executed on BigQuery.
        job_project:
            Google Cloud Platform project ID to execute query job.
        location:
            Location of BigQuery sources.
        destination_project:
            Google Cloud Platform project ID of the destination table.
        destination_dataset:
            BigQuery dataset ID of the destination table.
        destination_table:
            BigQuery table ID of the destination table.
        depend_on:
            Required table artifacts to execute this query.

        Returns
        -------
        QueryTask

        """
        component_dict = {
            "name": self.name,
            "inputs": [
                {"name": "job_project", "type": "String"},
                {"name": "query", "type": "String"},
                {"name": "location", "type": "String"},
                {"name": "destination_project", "type": "String"},
                {"name": "destination_dataset", "type": "String"},
                {"name": "destination_table", "type": "String"},
            ],
            "outputs": [
                {"name": "destination_table", "type": "google.BQTable"},
                {"name": "gcp_resources", "type": "String"},
            ],
            "implementation": {
                "container": {
                    "image": f"us-central1-docker.pkg.dev/sfujiwara/kfpc/bigquery:{get_version()}",
                    "command": ["inv", "query"],
                    "args": [
                        "--job-project", {"inputValue": "job_project"},
                        "--query", {"inputValue": "query"},
                        "--location", {"inputValue": "location"},
                        "--destination-project", {"inputValue": "destination_project"},
                        "--destination-dataset", {"inputValue": "destination_dataset"},
                        "--destination-table", {"inputValue": "destination_table"},
                        "--gcp-resources", {"outputPath": "gcp_resources"},
                        "--executor-input", {"executorInput": None},
                    ],
                },
            },
        }

        if depend_on:
            additional_inputs = {}
            for i, t in enumerate(depend_on):
                key = f"table{i+1}"
                component_dict["inputs"].append({"name": key, "type": "google.BQTable"})
                additional_inputs[key] = t

            task = load_component_from_text(yaml.dump(component_dict))(
                query=query,
                job_project=job_project,
                location=location,
                destination_project=destination_project,
                destination_dataset=destination_dataset,
                destination_table=destination_table,
                **additional_inputs,
            )
        else:
            task = load_component_from_text(yaml.dump(component_dict))(
                query=query,
                job_project=job_project,
                location=location,
                destination_project=destination_project,
                destination_dataset=destination_dataset,
                destination_table=destination_table,
            )

        return QueryTask(task=task)
