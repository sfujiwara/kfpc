import os
import pkgutil
from typing import Dict
from typing import List
from typing import Union
from typing import Optional
import yaml
from kfp.components import load_component_from_text
from kfp.dsl import PipelineParam
from kfpc.version import get_version


KFPC_BIGQUERY_IMAGE = f"gcr.io/sfujiwara/kfpc/bigquery:{get_version()}"


class Query:
    """
    Kubeflow Pipelines component for BigQuery query job.

    Parameters
    ----------
    name:
        Name of the component.
    """

    def __init__(self, name: str):
        self.name = name
        self.op = None
        self.dict = yaml.load(
            pkgutil.get_data(package="kfpc", resource=os.path.join("specifications", "query.yaml")),
            yaml.Loader,
        )
        self.dict["name"] = self.name
        self.dict["implementation"]["container"]["image"] = KFPC_BIGQUERY_IMAGE

    def task(
        self,
        query: Union[PipelineParam, str],
        job_project: Union[PipelineParam, str],
        destination_project: Union[PipelineParam, str],
        destination_dataset: Union[PipelineParam, str],
        destination_table: Union[PipelineParam, str],
        location: Union[PipelineParam, str] = "US",
        depend_on: Optional[List[PipelineParam]] = None,
    ):
        """
        Generate a Kubeflow Pipelines task.

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
                d[key] = depend_on[i]

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
    def gcp_resources(self) -> PipelineParam:
        return self.op.outputs["gcp_resources"]

    @property
    def destination_table(self) -> PipelineParam:
        return self.op.outputs["destination_table"]


class Extract:
    """
    Kubeflow Pipelines component for BigQuery extract job.

    Parameters
    ----------
    name:
        Name of the component.
    """

    def __init__(self, name: str = "extract"):
        self.name = name
        self.op = None
        self.dict = yaml.load(
            pkgutil.get_data(package="kfpc", resource=os.path.join("specifications", "extract.yaml")),
            yaml.Loader,
        )
        self.dict["name"] = self.name
        self.dict["implementation"]["container"]["image"] = KFPC_BIGQUERY_IMAGE

    def task(
        self,
        job_project: Union[PipelineParam, str],
        location: Union[PipelineParam, str],
        source_project_id: Optional[str] = None,
        source_dataset_id: Optional[str] = None,
        source_table_id: Optional[str] = None,
    ):
        """
        Generate Kubeflow Pipelines task to submit BigQuery extract job.

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
    def output_files(self) -> PipelineParam:
        return self.op.outputs["output_files"]


class ExtractArtifact:
    """
    Kubeflow Pipelines component for BigQuery extract job.
    It's used when source table is a ``google.BQTable`` artifact generated by ``kfpc.bigquery.Query``.

    Parameters
    ----------
    name:
        Name of the component.
    """

    def __init__(self, name):
        self.name = name
        self.op = None
        self.dict = yaml.load(
            pkgutil.get_data(
                package="kfpc", resource=os.path.join("specifications", "extract_artifact.yaml")
            ),
            yaml.Loader,
        )
        self.dict["name"] = self.name
        self.dict["implementation"]["container"]["image"] = KFPC_BIGQUERY_IMAGE

    def task(
        self,
        job_project: Union[PipelineParam, str],
        source_table_artifact: Optional[PipelineParam],
        location: Union[PipelineParam, str] = "US",
    ):
        """
        Generate Kubeflow Pipelines task to submit BigQuery extract job.

        Parameters
        ----------
        job_project:
            Google Cloud Platform project ID to execute query job.
        source_table_artifact:
            `google.BQTable` artifact generated by other tasks.
            Typically, `kfpc.bigquery.Query.destination_table` is used.
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
    def output_files(self) -> PipelineParam:
        return self.op.outputs["output_files"]


class Load:
    """
    Kubeflow Pipelines component for BigQuery load job.

    Parameters
    ----------
    name:
        Name of the component.
    """

    def __init__(self, name: str):
        self.name = name
        self.op = None
        self.dict = yaml.load(
            pkgutil.get_data(package="kfpc", resource=os.path.join("specifications", "load.yaml")),
            yaml.Loader,
        )
        self.dict["name"] = self.name
        self.dict["implementation"]["container"]["image"] = KFPC_BIGQUERY_IMAGE

    def task(
        self,
        job_project: Union[PipelineParam, str],
        destination_project: Union[PipelineParam, str],
        destination_dataset: Union[PipelineParam, str],
        destination_table: Union[PipelineParam, str],
        schema: Union[PipelineParam, List[Dict]],
        source_artifact: PipelineParam,
        source_uri_suffix="",
        location="US",
    ):
        """
        Generate a Kubeflow Pipelines task to execute BigQuery load job.

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
