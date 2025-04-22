"""Configuration helpers for managing environment variables."""

import os
import logging


class ConfigurationError(Exception):
    """Custom exception for configuration-related errors."""
    pass


def get_required_env_var(var_name: str) -> str:
    """
    Gets an environment variable, raising an error if it's not set.

    Args:
        var_name: The name of the environment variable.

    Returns:
        The value of the environment variable.

    Raises:
        ConfigurationError: If the environment variable is not set.
    """
    value = os.environ.get(var_name)
    if value is None or value == "":
        logging.error(f"Required environment variable '{var_name}' is not set.")
        raise ConfigurationError(f"Required environment variable '{var_name}' is not set.")
    logging.debug(f"Retrieved required environment variable '{var_name}'.")
    return value


def get_optional_env_var(var_name: str, default: str | None = None) -> str | None:
    """
    Gets an optional environment variable, returning a default value if not set.

    Args:
        var_name: The name of the environment variable.
        default: The default value to return if the variable is not set.

    Returns:
        The value of the environment variable or the default value.
    """
    value = os.environ.get(var_name, default)
    logging.debug(f"Retrieved optional environment variable '{var_name}' (defaulted: {value is default}).")
    return value
