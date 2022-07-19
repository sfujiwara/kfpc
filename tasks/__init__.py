import os
import invoke
import toml


def get_version() -> str:
    """
    Get version from `pyproject.toml`.
    """
    with open("pyproject.toml") as f:
        pyproject = toml.load(f)

    return pyproject["tool"]["poetry"]["version"]


@invoke.task
def docker_build(c):
    """
    Build Docker image.
    """
    os.environ["VERSION"] = get_version()
    c.run("docker-compose build")


@invoke.task
def docker_push(c):
    """
    Build and push Docker image.
    """
    os.environ["VERSION"] = get_version()
    c.run("docker-compose build")
    c.run("docker-compose push")


d = invoke.Collection("docker")
d.add_task(task=docker_build, name="build")
d.add_task(task=docker_push, name="push")

ns = invoke.Collection()
ns.add_collection(d)
