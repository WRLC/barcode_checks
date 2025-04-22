"""Data serialization and formatting utilities."""

import re
import json
import datetime
import logging
from typing import Dict, List, Union


class DataSerializationError(Exception):
    """Custom exception for data serialization/deserialization errors."""
    pass


def create_safe_filename(input_string: str, replacement: str = '_', max_len: int = 200) -> str:
    """
    Removes/replaces characters unsafe for filenames or blob names.
    Also truncates to a maximum length.

    Args:
        input_string: The original string (e.g., report path).
        replacement: The character to use for unsafe characters.
        max_len: The maximum allowed length for the filename.

    Returns:
        A sanitized string safe for use as a filename or blob name component.
    """
    if not input_string:
        return "default_filename"

    # Remove leading/trailing whitespace
    safe_name = str(input_string).strip()
    # Replace common path separators and other problematic characters
    # Including: \ / : * ? " < > | space, and control characters
    safe_name = re.sub(r'[\\/:*?"<>|\s\x00-\x1f\x7f]+', replacement, safe_name)
    # Replace multiple consecutive replacements with a single one
    safe_name = re.sub(f'{replacement}+', replacement, safe_name)
    # Remove leading/trailing replacement characters
    safe_name = safe_name.strip(replacement)
    # Truncate to max length
    safe_name = safe_name[:max_len]
    # Handle cases where the entire string was invalid characters or it's now empty
    if not safe_name:
        return "default_filename"
    logging.debug(f"Created safe filename: '{safe_name}' from '{input_string}'")
    return safe_name


def serialize_data(data: Union[Dict, List]) -> str:
    """
    Serializes a Python dictionary or list to a JSON string.

    Args:
        data: The Python object to serialize.

    Returns:
        A JSON string representation of the data.

    Raises:
        DataSerializationError: If serialization fails.
    """
    try:
        # ensure_ascii=False is often useful for non-latin characters
        return json.dumps(data, ensure_ascii=False)
    except TypeError as e:
        logging.error(f"Failed to serialize data to JSON: {e}. Data type: {type(data)}")
        raise DataSerializationError(f"Failed to serialize data to JSON: {e}") from e


def deserialize_data(data_string: Union[str, bytes]) -> Union[Dict, List]:
    """
    Deserializes a JSON string or bytes into a Python dictionary or list.

    Args:
        data_string: The JSON string or bytes to deserialize.

    Returns:
        A Python dictionary or list.

    Raises:
        DataSerializationError: If deserialization fails or input type is wrong.
    """
    if isinstance(data_string, bytes):
        try:
            data_string = data_string.decode()
        except UnicodeDecodeError as e:
            logging.error(f"Failed to decode bytes using utf-8: {e}")
            raise DataSerializationError(f"Failed to decode bytes using utf-8: {e}") from e

    if not isinstance(data_string, str):
        raise DataSerializationError(f"Input must be string or bytes, got {type(data_string)}")

    try:
        return json.loads(data_string)
    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON data string: {e}. String starts with: '{data_string[:100]}'")
        raise DataSerializationError(f"Failed to decode JSON data string: {e}") from e
    except Exception as e:
        logging.error(f"Unexpected error during deserialization: {e}")
        raise DataSerializationError(f"Unexpected error during deserialization: {e}") from e


def format_datetime_for_display(dt_object: datetime.datetime | None, fmt: str = "%Y-%m-%d %H:%M:%S UTC") -> str:
    """
    Formats a datetime object into a string, defaulting to UTC if naive.

    Args:
        dt_object: The datetime object to format. If None, returns empty string.
        fmt: The strftime format string.

    Returns:
        The formatted datetime string, or an empty string if input is None.
    """
    if dt_object is None:
        return ""
    try:
        # Assume UTC if the datetime object is naive
        if dt_object.tzinfo is None or dt_object.tzinfo.utcoffset(dt_object) is None:
            dt_object = dt_object.replace(tzinfo=datetime.timezone.utc)
        return dt_object.strftime(fmt)
    except Exception as e:
        logging.warning(f"Could not format datetime object '{dt_object}': {e}")
        return str(dt_object)  # Fallback to default string representation
