"""Exceptions for the AlmaClient module."""


class AlmaClientError(Exception):
    """Base exception class for AlmaClient errors."""
    pass


class AlmaApiError(AlmaClientError):
    """Raised when the Alma API returns an error response."""
    def __init__(self, status_code: int, error_code: str | None = None, error_message: str | None = None, response_text: str | None = None):
        self.status_code = status_code
        self.error_code = error_code
        self.error_message = error_message
        self.response_text = response_text
        super().__init__(f"Alma API Error (Status: {status_code}): Code='{error_code}', Message='{error_message}'")


class AlmaClientConfigurationError(AlmaClientError):
    """Raised when the client is missing necessary configuration."""
    pass
