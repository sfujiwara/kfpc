from dataclasses import dataclass
import json
import os
import dacite
from google.cloud import aiplatform
from googleapiclient import discovery
import invoke
import requests


@dataclass
class SlackNotificationConfig:
    webhook_url: str
    mentions: list[str]


def project_number_to_id(project_number: str) -> str:
    """Get project ID from project number."""
    crm = discovery.build("cloudresourcemanager", "v3")
    res = crm.projects().get(name=f"projects/{project_number}").execute()
    project_id = res["projectId"]

    return project_id


@invoke.task
def slack(ctx, config, job_resource_name, job_name):
    config = dacite.from_dict(data_class=SlackNotificationConfig, data=json.loads(config))

    # Vertex AI instance has project number in environment variable `CLOUD_ML_PROJECT_ID`.
    project_number = os.getenv("CLOUD_ML_PROJECT_ID")
    project_id = project_number_to_id(project_number=project_number)

    job = aiplatform.PipelineJob.get(resource_name=job_resource_name, project=project_number).to_dict()
    tasks = job["jobDetail"]["taskDetails"]
    succeeded = True
    for t in tasks:
        if t["state"] not in {"SUCCEEDED", "RUNNING"}:
            succeeded = False

    location = job["name"].split("/")[3]

    if succeeded:
        message = "Vertex AI Pipelines run completed successfully."
        color = "#2eb886"
    else:
        message = "Vertex AI Pipalines run failed."
        color = "#A30100"

    job_url = f"https://console.cloud.google.com/vertex-ai/locations/{location}/pipelines/runs/{job_name}?project={project_number}"

    payload = {
        "attachments": [
            {
                "color": color,
                "blocks": [
                    {
                        "type": "section",
                        "text": {"type": "plain_text", "text": message},
                    },
                    {
                        "type": "section",
                        "fields": [
                            {"type": "mrkdwn", "text": f"*Project*\n{project_id}"},
                            {"type": "mrkdwn", "text": f"*Run*\n<{job_url}|{job_name}>"},
                        ],
                    },
                ],
            }
        ],
    }

    if not succeeded:
        mention_to_maintainers = " ".join([f"<@{i}>" for i in config.mentions])
        payload["attachments"][0]["blocks"].append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Mantainers*\n{mention_to_maintainers}"
                }
            }
        )

    requests.post(url=config.webhook_url, json=payload)
