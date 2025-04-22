"""Set up the Alma Client package."""

from .client import AlmaClient
from .exceptions import AlmaClientError, AlmaApiError, AlmaClientConfigurationError

# Optionally define what gets imported with 'from alma_client import *'
__all__ = ['AlmaClient', 'AlmaClientError', 'AlmaApiError', 'AlmaClientConfigurationError']
