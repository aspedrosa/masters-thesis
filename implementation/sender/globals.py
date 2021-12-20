import os

from schema_registry.client import SchemaRegistryClient


def _get_env_variable(name):
    try:
        return os.environ[name]
    except KeyError:
        raise ValueError(f"Missing {name} environment variable")


BOOTSTRAP_SERVERS = _get_env_variable("BOOTSTRAP_SERVERS").split(",")
_ADMIN_PORTAL_HOST = _get_env_variable("ADMIN_PORTAL_HOST")
_ADMIN_PORTAL_PORT = _get_env_variable("ADMIN_PORTAL_PORT")
ADMIN_PORTAL_URL = f"http://{_ADMIN_PORTAL_HOST}:{_ADMIN_PORTAL_PORT}/api"
ADMIN_PORTAL_USER = _get_env_variable("ADMIN_PORTAL_USER")
ADMIN_PORTAL_PASSWORD = _get_env_variable("ADMIN_PORTAL_PASSWORD")
_SCHEMA_REGISTRY_HOST = _get_env_variable("SCHEMA_REGISTRY_HOST")
_SCHEMA_REGISTRY_PORT = _get_env_variable("SCHEMA_REGISTRY_PORT")
SCHEMA_REGISTRY_CLIENT = SchemaRegistryClient(f"http://{_SCHEMA_REGISTRY_HOST}:{_SCHEMA_REGISTRY_PORT}")
