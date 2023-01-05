from dataclasses import dataclass


@dataclass
class SlackConfig:
    webhook_url: str
    level: str
    mentions: list[str]


class Slack:
    def __init__(self, name: str, config_list: list[SlackConfig]):
        self.name = name
        self.op = None

    def task(self, enable: bool = True):
        pass
