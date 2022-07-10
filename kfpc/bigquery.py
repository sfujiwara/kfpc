import os
import pkgutil
from typing import Union
import yaml
from kfp.components import load_component_from_text
from kfp.dsl import PipelineParam


class Query:

    def __init__(self, name: str):
        """
        Kubeflow Pipelines component for BigQuery query job.

        Parameters
        ----------
        name:
            Name of the component.
        """
        self.name = name
        self.op = None
        self.dict = yaml.load(
            pkgutil.get_data(package="kfpc", resource=os.path.join("specifications", "query.yaml")),
            yaml.Loader,
        )
        self.dict["name"] = self.name

    def task(
        self,
        query: Union[PipelineParam, str],
        job_project: Union[PipelineParam, str],
        location: Union[PipelineParam, str],
        destination_project: Union[PipelineParam, str],
        destination_dataset: Union[PipelineParam, str],
        destination_table: Union[PipelineParam, str],
    ):
        """
        Generate a Kubeflow Pipelines task.

        Parameters
        ----------
        query:
            SQL string executed on BigQuery.
        job_project:
            Google Cloud Platform project ID where the job is executed.
        location:
            Location of BigQuery sources.
        destination_project:
            Google Cloud Platform project ID of the destination table.
        destination_dataset:
            BigQuery dataset ID of the destination table.
        destination_table:
            Table ID of the destination table.
        """
        self.op = load_component_from_text(yaml.dump(self.dict))
        self.op(
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

    def __init__(self, name: str):
        self.name = name

    def task(self):
        raise NotImplementedError


class ExtractTableArtifact:

    def __init__(self, name: str):
        self.name = name

    def task(self, table_uri):
        raise NotImplementedError
