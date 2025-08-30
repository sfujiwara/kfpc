"""Components for BigQuery."""

from __future__ import annotations

import pkgutil
from pathlib import Path
from typing import TYPE_CHECKING

import yaml
from kfp.components import load_component_from_text

if TYPE_CHECKING:
    from kfp.dsl.pipeline_channel import PipelineArtifactChannel, PipelineParameterChannel

from kfpc.version import get_version

KFPC_BIGQUERY_IMAGE = f"gcr.io/sfujiwara/kfpc/bigquery:{get_version()}"


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
        self.op = None
        self.dict = yaml.safe_load(
            pkgutil.get_data(
                package="kfpc", resource=str(Path("specifications") / "query.yaml"),
            ),
        )
        self.dict["name"] = self.name
        self.dict["implementation"]["container"]["image"] = KFPC_BIGQUERY_IMAGE

    def task(
        self,
        query: PipelineParameterChannel | str,
        job_project: PipelineParameterChannel | str,
        destination_project: PipelineParameterChannel | str,
        destination_dataset: PipelineParameterChannel | str,
        destination_table: PipelineParameterChannel | str,
        location: PipelineParameterChannel | str = "US",
        depend_on: list[PipelineArtifactChannel] | None = None,
    ) -> Query:
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
        self

        """
        if depend_on:
            d = {}
            for i, t in enumerate(depend_on):
                key = f"table{i+1}"
                self.dict["inputs"].append({"name": key, "type": "google.BQTable"})
                d[key] = t

            self.op = load_component_from_text(yaml.dump(self.dict))(
                query=query,
                job_project=job_project,
                location=location,
                destination_project=destination_project,
                destination_dataset=destination_dataset,
                destination_table=destination_table,
                **d,
            )
        else:
            self.op = load_component_from_text(yaml.dump(self.dict))(
                query=query,
                job_project=job_project,
                location=location,
                destination_project=destination_project,
                destination_dataset=destination_dataset,
                destination_table=destination_table,
            )

        return self

    @property
    def gcp_resources(self) -> PipelineParameterChannel:
        """GCP resource."""
        return self.op.outputs["gcp_resources"]

    @property
    def destination_table(self) -> PipelineArtifactChannel:
        """Destination table."""
        return self.op.outputs["destination_table"]


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
        self.op = None
        self.dict = yaml.safe_load(
            pkgutil.get_data(
                package="kfpc", resource=str(Path("specifications") / "extract.yaml"),
            ),
        )
        self.dict["name"] = self.name
        self.dict["implementation"]["container"]["image"] = KFPC_BIGQUERY_IMAGE

    def task(
        self,
        job_project: PipelineParameterChannel | str,
        location: PipelineParameterChannel | str,
        source_project_id: str | None = None,
        source_dataset_id: str | None = None,
        source_table_id: str | None = None,
    ) -> Extract:
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
        self.op = load_component_from_text(yaml.dump(self.dict))(
            job_project=job_project,
            source_project_id=source_project_id,
            source_dataset_id=source_dataset_id,
            source_table_id=source_table_id,
            location=location,
        )

        return self

    @property
    def output_files(self) -> PipelineArtifactChannel:
        """Output files."""
        return self.op.outputs["output_files"]


class ExtractArtifact:
    """Kubeflow Pipelines component for BigQuery extract job.

    It's used when source table is a ``google.BQTable`` artifact generated by ``kfpc.bigquery.Query``.

    Parameters
    ----------
    name:
        Name of the component.

    """

    def __init__(self, name: str) -> None:
        """Initialize ``ExtractArtifact`` instance."""
        self.name = name
        self.op = None
        self.dict = yaml.safe_load(
            pkgutil.get_data(
                package="kfpc",
                resource=str(Path("specifications") / "extract_artifact.yaml"),
            ),
        )
        self.dict["name"] = self.name
        self.dict["implementation"]["container"]["image"] = KFPC_BIGQUERY_IMAGE

    def task(
        self,
        job_project: PipelineParameterChannel | str,
        source_table_artifact: PipelineArtifactChannel | None,
        location: PipelineParameterChannel | str = "US",
    ) -> ExtractArtifact:
        """Generate Kubeflow Pipelines task to submit BigQuery extract job.

        Parameters
        ----------
        job_project:
            Google Cloud Platform project ID to execute query job.
        source_table_artifact:
            `google.BQTable` artifact generated by other tasks.
            Typically, ``kfpc.bigquery.Query.destination_table`` is used.
        location:
            Location of BigQuery sources.

        Returns
        -------
        self

        """
        self.op = load_component_from_text(yaml.dump(self.dict))(
            job_project=job_project,
            source_table_artifact=source_table_artifact,
            location=location,
        )
        return self

    @property
    def output_files(self) -> PipelineArtifactChannel:
        """Output files."""
        return self.op.outputs["output_files"]


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
        self.op = None
        self.dict = yaml.safe_load(
            pkgutil.get_data(
                package="kfpc", resource=str(Path("specifications") / "load.yaml"),
            ),
        )
        self.dict["name"] = self.name
        self.dict["implementation"]["container"]["image"] = KFPC_BIGQUERY_IMAGE

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
    ) -> Load:
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
        self.op = load_component_from_text(yaml.dump(self.dict))(
            job_project=job_project,
            source_artifact=source_artifact,
            destination_project=destination_project,
            destination_dataset=destination_dataset,
            destination_table=destination_table,
            location=location,
            schema=schema,
            source_uri_suffix=source_uri_suffix,
        )
        return self
