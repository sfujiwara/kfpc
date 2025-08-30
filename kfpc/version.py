"""Get version of `kfpc` package."""

import pkg_resources


def get_version() -> str:
    """Get version of `kfpc` package."""
    return pkg_resources.get_distribution("kfpc").version
