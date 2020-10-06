from os import getenv

from aito.client import AitoClient


def get_env_var(var_name):
    env_var = getenv(var_name)
    if not env_var:
        raise KeyError(f'environment variable `{var_name}` not found')
    return env_var


def default_client():
    return AitoClient(get_env_var('AITO_INSTANCE_URL'), get_env_var('AITO_API_KEY'))


def grocery_demo_client():
    return AitoClient(get_env_var('AITO_GROCERY_DEMO_INSTANCE_URL'), get_env_var('AITO_GROCERY_DEMO_API_KEY'))
