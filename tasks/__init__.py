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


@invoke.task
def black_diff(c):
    """
    Show the diffs of Black formatter.
    """
    c.run("black --diff --check .")


@invoke.task
def black_fmt(c):
    """
    Apply Black formatter.
    """
    c.run("black .")


@invoke.task
def docs_build(c):
    """
    Generate documentations using Sphinx.
    """
    with c.cd("sphinx"):
        c.run("make html")


docker = invoke.Collection("docker")
docker.add_task(task=docker_build, name="build")
docker.add_task(task=docker_push, name="push")

black = invoke.Collection("black")
black.add_task(task=black_diff, name="diff")
black.add_task(task=black_fmt, name="fmt")

docs = invoke.Collection("docs")
docs.add_task(task=docs_build, name="build")

ns = invoke.Collection()
ns.add_collection(docker)
ns.add_collection(black)
ns.add_collection(docs)
