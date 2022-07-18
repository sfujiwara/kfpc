import invoke
import toml


def get_version() -> str:
    """
    Get version from `pyproject.toml`.
    """
    with open("pyproject.toml") as f:
        pyproject = toml.load(f)

    return pyproject["tool"]["poetry"]["version"]
