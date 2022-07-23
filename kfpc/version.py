import pkg_resources


def get_version():
    return pkg_resources.get_distribution("kfpc").version
