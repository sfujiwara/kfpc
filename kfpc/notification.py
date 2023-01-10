from dataclasses import dataclass
import kfp


@dataclass
class SlackConfig:
    webhook_url: str
    mentions: list[str]


class Slack:
    def __init__(self, name: str, config: SlackConfig):
        self.name = name
        self.op = None
        self.dict = {
            "name": "slack-notifier",
            "inputs": [
                {"name": "config", "type": "JsonObject"},
                {"name": "job_resource_name", "type": "String"},
                {"name": "job_name", "type": "String"},
            ],
            "implementation": {
                "container": {
                    "image": "{{$.inputs.parameters['image']}}",
                    "command": ["inv", "slack"],
                    "args": [
                        "---config",
                        {"inputValue": "config"},
                        "--job-resource-name",
                        {"inputValue": "job_resource_name"},
                        "--job-name",
                        {"inputValue": "job_name"},
                    ],
                },
            },
        }

    def task(self):
        job_name = kfp.v2.dsl.PIPELINE_JOB_NAME_PLACEHOLDER,
        job_resource_name = kfp.v2.dsl.PIPELINE_JOB_RESOURCE_NAME_PLACEHOLDER,
