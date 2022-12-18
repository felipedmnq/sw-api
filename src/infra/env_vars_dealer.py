import os


def get_env_variable(variable: str) -> str:
    auth_exists = variable in os.environ
    auth_msg = f"""
        Variable {variable} doesn't exist in a local environment.
        Please provide the right argument or add a proper variable to the system.
    """

    assert auth_exists, auth_msg

    return os.environ[variable]