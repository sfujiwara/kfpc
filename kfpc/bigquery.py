import os
import pkgutil
from typing import List
from typing import Union
from typing import Optional
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
        depend_on: Optional[List[PipelineParam]]=None,
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
        depend_on:
            Required table artifacts to execute this task.
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

    def __init__(self, name: str = "extract"):
        self.name = name
        self.op = None
        self.dict = None

    def task(
        self,
        job_project: Union[PipelineParam, str],
        location: Union[PipelineParam, str],
        destination_format: Union[PipelineParam, str],
        output_file_name: Union[PipelineParam, str],
        source_table_artifact: Optional[PipelineParam] = None,
        source_project_id: Optional[str] = None,
        source_dataset_id: Optional[str] = None,
        source_table_id: Optional[str] = None,
    ):
        if source_table_artifact:
            self.dict = yaml.load(
                pkgutil.get_data(package="kfpc", resource=os.path.join("specifications", "extract_artifact.yaml")),
                yaml.Loader,
            )
            self.dict["name"] = self.name
            self.op = load_component_from_text(yaml.dump(self.dict))(
                job_project=job_project,
                source_table=source_table,
                location=location,
                destination_format=destination_format,
                output_file_name=output_file_name,
            )
        elif source_project_id and source_dataset_id and source_table_id:
            self.dict = yaml.load(
                pkgutil.get_data(package="kfpc", resource=os.path.join("specifications", "extract.yaml")),
                yaml.Loader,
            )
            self.dict["name"] = self.name
            self.op = load_component_from_text(yaml.dump(self.dict))(
                job_project=job_project,
                source_table=source_table,
                location=location,
                destination_format=destination_format,
                output_file_name=output_file_name,
            )
