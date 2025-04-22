"""Storage Helpers for Azure Blob and Queue Services"""

import json
import logging
from typing import Dict, List, Union, Optional
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobClient, ContentSettings
from azure.storage.queue import QueueServiceClient, QueueClient, TextBase64EncodePolicy, QueueMessage
from azure.core.exceptions import ResourceNotFoundError, ResourceExistsError
from .config_helpers import get_required_env_var

# Cache the connection string locally within the module
_STORAGE_CONNECTION_STRING: Optional[str] = None


def _get_storage_connection_string() -> str:
    """Gets the storage connection string, caching it after the first call."""
    global _STORAGE_CONNECTION_STRING
    if _STORAGE_CONNECTION_STRING is None:
        # Azure Functions typically uses 'AzureWebJobsStorage'
        try:
            # Use the helper to ensure it's present
            _STORAGE_CONNECTION_STRING = get_required_env_var("AzureWebJobsStorage")
        except ConfigurationError as e:
            # Log and re-raise as ValueError for compatibility with SDK expectations if needed,
            # or handle appropriately depending on desired function behavior on config error.
            logging.error(f"Storage Configuration Error: {e}")
            raise ValueError(f"Storage Configuration Error: {e}") from e
    return _STORAGE_CONNECTION_STRING


# --- Client Getters ---

def get_blob_service_client() -> BlobServiceClient:
    """Returns an authenticated BlobServiceClient instance."""
    conn_str = _get_storage_connection_string()
    try:
        return BlobServiceClient.from_connection_string(conn_str)
    except ValueError as e:
        logging.error(f"Invalid storage connection string format: {e}")
        raise ValueError(f"Invalid storage connection string format: {e}") from e


def get_queue_service_client() -> QueueServiceClient:
    """Returns an authenticated QueueServiceClient instance."""
    conn_str = _get_storage_connection_string()
    try:
        # Note: Functions often expect Base64 encoding for queue triggers/outputs.
        # Adjust policy if needed based on your specific binding configurations.
        # If using raw strings, set message_encode_policy=None, message_decode_policy=None.
        return QueueServiceClient.from_connection_string(
            conn_str,
            message_encode_policy=TextBase64EncodePolicy()  # Encodes outgoing messages to Base64
        )
    except ValueError as e:
        logging.error(f"Invalid storage connection string format: {e}")
        raise ValueError(f"Invalid storage connection string format: {e}") from e


def get_queue_client(queue_name: str) -> QueueClient:
    """
    Returns an authenticated QueueClient for a specific queue.
    Does NOT automatically create the queue.
    """
    if not queue_name:
        raise ValueError("Queue name cannot be empty.")
    conn_str = _get_storage_connection_string()
    try:
        # Inherits policy from service client if created that way, or specify explicitly
        return QueueClient.from_connection_string(
            conn_str,
            queue_name,
            message_encode_policy=TextBase64EncodePolicy()
        )
    except ValueError as e:
        logging.error(f"Invalid storage connection string or queue name '{queue_name}': {e}")
        raise ValueError(f"Invalid storage connection string or queue name '{queue_name}': {e}") from e


# --- Blob Operations ---

def upload_blob_data(container_name: str, blob_name: str, data: Union[str, bytes, Dict, List], overwrite: bool = True):
    """
    Uploads data to Azure Blob Storage. Serializes Python dicts/lists to JSON.

    Args:
        container_name: The name of the blob container.
        blob_name: The name of the blob.
        data: The data to upload (str, bytes, dict, or list).
        overwrite: Whether to overwrite the blob if it already exists.

    Raises:
        ValueError: If container or blob name is invalid.
        TypeError: If data type is not supported.
        azure.core.exceptions.ServiceRequestError: For network issues.
        azure.core.exceptions.ResourceExistsError: If blob exists and overwrite is False.
    """
    if not container_name or not blob_name:
        raise ValueError("Container name and blob name cannot be empty.")

    logging.debug(f"Attempting to upload blob to {container_name}/{blob_name} (overwrite={overwrite})")
    try:
        blob_client = get_blob_service_client().get_blob_client(container=container_name, blob=blob_name)
    except ValueError as e:  # Catch connection string format error from getter
        logging.error(f"Failed to get blob client for {container_name}/{blob_name}: {e}")
        raise

    try:
        upload_data: Union[str, bytes]
        # **** CHANGE: Instantiate ContentSettings object ****
        settings_to_pass: Optional[ContentSettings] = None

        if isinstance(data, (dict, list)):
            # Use data_utils serializer for consistency
            from .data_utils import serialize_data
            upload_data = serialize_data(data).encode()
            settings_to_pass = ContentSettings(content_type='application/json')  # Create object

        elif isinstance(data, str):
            upload_data = data.encode()
            settings_to_pass = ContentSettings(content_type='text/plain; charset=utf-8')  # Create object

        elif isinstance(data, bytes):
            upload_data = data
            # Let SDK handle content-type for raw bytes, or set if known
            # settings_to_pass = ContentSettings(content_type='application/octet-stream') # Example
        else:
            raise TypeError(f"Unsupported data type for upload_blob_data: {type(data)}")

        # **** CHANGE: Pass the ContentSettings object (or None) ****
        blob_client.upload_blob(
            upload_data,
            blob_type="BlockBlob",
            overwrite=overwrite,
            content_settings=settings_to_pass  # Pass the object
        )
        # **** END CHANGES ****

        logging.info(f"Successfully uploaded blob: {container_name}/{blob_name}")

    except ResourceExistsError as e:
        if not overwrite:
            logging.warning(f"Blob {container_name}/{blob_name} already exists and overwrite is False.")
            # Depending on desired behavior, you might re-raise or just return
            raise  # Re-raise by default if overwrite is False and it exists
        else:
            # This typically shouldn't happen if overwrite=True works as expected, log as error.
            logging.error(
                f"Unexpected ResourceExistsError despite overwrite=True for {container_name}/{blob_name}: {e}")
            raise  # Re-raise unexpected error
    except Exception as e:
        logging.error(f"Failed to upload blob {container_name}/{blob_name}: {e}")
        raise  # Re-raise other SDK or unexpected errors


def download_blob_as_text(container_name: str, blob_name: str, encoding: str = 'utf-8') -> str:
    """
    Downloads blob content as a decoded text string.

    Args:
        container_name: The name of the blob container.
        blob_name: The name of the blob.
        encoding: The encoding to use for decoding the blob bytes.

    Returns:
        The decoded string content of the blob.

    Raises:
        ValueError: If container or blob name is invalid.
        azure.core.exceptions.ResourceNotFoundError: If the blob does not exist.
        azure.core.exceptions.ServiceRequestError: For network issues.
    """
    if not container_name or not blob_name:
        raise ValueError("Container name and blob name cannot be empty.")

    logging.debug(f"Attempting to download blob as text from {container_name}/{blob_name}")
    blob_client = get_blob_service_client().get_blob_client(container=container_name, blob=blob_name)
    try:
        blob_content_bytes = blob_client.download_blob().readall()
        logging.info(f"Successfully downloaded blob: {container_name}/{blob_name}")
        return blob_content_bytes.decode(encoding)
    except ResourceNotFoundError:
        logging.error(f"Blob not found: {container_name}/{blob_name}")
        raise
    except UnicodeDecodeError as e:
        logging.error(f"Failed to decode blob {container_name}/{blob_name} using encoding '{encoding}': {e}")
        raise UnicodeDecodeError(
            f"Failed to decode blob {container_name}/{blob_name} using encoding '{encoding}': {e}") from e
    except Exception as e:
        logging.error(f"Failed to download blob {container_name}/{blob_name} as text: {e}")
        raise


def download_blob_as_json(container_name: str, blob_name: str, encoding: str = 'utf-8') -> Union[Dict, List]:
    """
    Downloads blob content and parses it as JSON.

    Args:
        container_name: The name of the blob container.
        blob_name: The name of the blob.
        encoding: The encoding of the stored text data before JSON parsing.

    Returns:
        A Python dictionary or list parsed from the blob's JSON content.

    Raises:
        ValueError: If container or blob name is invalid.
        DataSerializationError: If the content cannot be parsed as JSON.
        azure.core.exceptions.ResourceNotFoundError: If the blob does not exist.
        azure.core.exceptions.ServiceRequestError: For network issues.
    """
    text_content = download_blob_as_text(container_name, blob_name, encoding)
    try:
        # Use the data_utils deserializer for consistency
        from .data_utils import deserialize_data
        return deserialize_data(text_content)
    except Exception as e:  # Catch potential DataSerializationError from deserialize_data
        logging.error(f"Failed to parse blob content from {container_name}/{blob_name} as JSON: {e}")
        # Re-raise or wrap in a more specific error if needed
        raise


def list_blobs(container_name: str, name_starts_with: Optional[str] = None) -> List[str]:
    """
    Lists blob names in a container, optionally filtering by prefix.

    Args:
        container_name: The name of the blob container.
        name_starts_with: Optional prefix to filter blob names.

    Returns:
        A list of blob names matching the criteria. Returns empty list if
        container not found.

    Raises:
        ValueError: If container name is invalid.
        azure.core.exceptions.ServiceRequestError: For network issues.
    """
    if not container_name:
        raise ValueError("Container name cannot be empty.")

    logging.debug(f"Listing blobs in container '{container_name}' starting with '{name_starts_with or ''}'")
    try:
        container_client = get_blob_service_client().get_container_client(container_name)
        blob_items = container_client.list_blobs(name_starts_with=name_starts_with)
        blob_names = [blob.name for blob in blob_items]
        logging.info(f"Found {len(blob_names)} blobs in '{container_name}' matching prefix '{name_starts_with or ''}'.")
        return blob_names
    except ResourceNotFoundError:
        logging.warning(f"Container '{container_name}' not found while listing blobs.")
        return []  # Return empty list if container doesn't exist
    except Exception as e:
        logging.error(f"Failed to list blobs in container '{container_name}': {e}")
        raise


def delete_blob(container_name: str, blob_name: str):
    """
    Deletes a specific blob. Does not raise error if blob doesn't exist.

    Args:
        container_name: The name of the blob container.
        blob_name: The name of the blob to delete.

    Raises:
        ValueError: If container or blob name is invalid.
        azure.core.exceptions.ServiceRequestError: For network issues.
    """
    if not container_name or not blob_name:
        raise ValueError("Container name and blob name cannot be empty.")

    logging.debug(f"Attempting to delete blob: {container_name}/{blob_name}")
    blob_client = get_blob_service_client().get_blob_client(container=container_name, blob=blob_name)
    try:
        blob_client.delete_blob(delete_snapshots="include")
        logging.info(f"Successfully deleted blob: {container_name}/{blob_name}")
    except ResourceNotFoundError:
        logging.warning(f"Blob not found during deletion, presumed already deleted: {container_name}/{blob_name}")
        # Do not raise an error if it's already gone
    except Exception as e:
        logging.error(f"Failed to delete blob {container_name}/{blob_name}: {e}")
        raise


# --- Queue Operations ---


def send_queue_message(queue_name: str, message_content: Union[Dict, List, str]):
    """
    Sends a message to the specified Azure Queue. Serializes dicts/lists to JSON.

    Args:
        queue_name: The name of the target queue.
        message_content: The message content (dict, list, or string).

    Raises:
        ValueError: If queue name is invalid.
        TypeError: If message_content type is not supported.
        azure.core.exceptions.ServiceRequestError: For network issues.
    """
    if not queue_name:
        raise ValueError("Queue name cannot be empty.")

    message_str: str
    try:
        if isinstance(message_content, (dict, list)):
            # Use the data_utils serializer for consistency
            from .data_utils import serialize_data
            message_str = serialize_data(message_content)
        elif isinstance(message_content, str):
            message_str = message_content
        else:
            raise TypeError(f"Unsupported message content type: {type(message_content)}")

        logging.debug(f"Attempting to send message to queue '{queue_name}': {message_str[:100]}...")
        queue_client = get_queue_client(queue_name)
        queue_client.send_message(message_str)
        logging.info(f"Successfully sent message to queue '{queue_name}'")

    except Exception as e:
        logging.error(f"Failed to send message to queue '{queue_name}': {e}")
        raise  # Re-raise SDK or other exceptions
