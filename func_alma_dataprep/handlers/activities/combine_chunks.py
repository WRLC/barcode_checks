"""Activity to combine JSON chunks from blob storage into a single blob."""

import logging
from typing import Dict, Any
import azure.functions as func
from shared_code.utils import storage_helpers
from shared_code.utils import data_utils

bp = func.Blueprint()


@bp.activity_trigger(input_name="payload")
def combine_report_chunks_activity(payload: Dict[str, Any]) -> Dict[str, str]:
    """
    Activity function to combine JSON chunks from blob storage into a single blob.

    This function is called by the orchestrator after all chunks have been fetched.
    It retrieves the chunks from the source container, combines them into a single JSON object,
    and uploads the combined data to the target container.

    Args:
        payload (Dict[str, Any]): The input payload containing job and report details.

    Returns:
        Dict[str, str]: A dictionary containing the target container and blob name of the combined data.

    Raises:
        ValueError: If required fields are missing or invalid in the payload.
        RuntimeError: If any error occurs during blob listing, downloading, or uploading.

    """
    job_id = payload.get("job_id")
    report_metadata = payload.get("report_metadata", {})
    target_container = payload.get("target_container")
    target_blob_name = payload.get("target_blob_name")

    source_container = report_metadata.get("container_name")
    report_name_safe = report_metadata.get("report_name_safe")
    total_chunks_expected = report_metadata.get("total_chunks")

    logging.info(f"Activity 'combine_report_chunks_activity' started for job {job_id}, report {report_name_safe}.")

    # --- Validate Input ---
    if not all([job_id, source_container, report_name_safe, isinstance(total_chunks_expected, int),
                target_container, target_blob_name]):
        error_msg = f"Job {job_id}: Missing required fields for combining chunks: {payload}"
        logging.error(error_msg)
        raise ValueError(error_msg)

    if total_chunks_expected <= 0:
        error_msg = f"Job {job_id}: Invalid total_chunks_expected: {total_chunks_expected}"
        logging.error(error_msg)
        raise ValueError(error_msg)

    # --- List Chunks ---
    blob_prefix = f"{job_id}/{report_name_safe}/chunk_"
    try:
        logging.info(f"Job {job_id}: Listing blobs in '{source_container}' with prefix '{blob_prefix}'.")
        # Assuming list_blobs returns a list of blob names (strings)
        chunk_blob_names = storage_helpers.list_blobs(container_name=source_container, name_starts_with=blob_prefix)

        if not chunk_blob_names:
            raise FileNotFoundError(
                f"No chunk blobs found with prefix '{blob_prefix}' in container '{source_container}'.")

        # Optional: Sort numerically if needed, assuming format chunk_0.json, chunk_1.json ... chunk_10.json
        chunk_blob_names.sort(key=lambda name: int(name.split('_')[-1].split('.')[0]))

        logging.info(f"Job {job_id}: Found {len(chunk_blob_names)} chunk blobs.")

        # --- Verify Chunk Count ---
        if len(chunk_blob_names) != total_chunks_expected:
            logging.warning(
                f"Job {job_id}: Expected {total_chunks_expected} chunks but found {len(chunk_blob_names)} "
                f"with prefix '{blob_prefix}'. Proceeding with found chunks."
            )
            # Decide if this is a hard error or just a warning
            # raise ValueError(f"Mismatch in chunk count for job {job_id}.")

    except Exception as list_err:
        error_msg = f"Job {job_id}: Failed to list chunk blobs: {list_err}"
        logging.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from list_err

    # --- Download and Combine ---
    combined_data = []
    try:
        for i, blob_name in enumerate(chunk_blob_names):
            logging.debug(f"Job {job_id}: Downloading chunk {i + 1}/{len(chunk_blob_names)}: '{blob_name}'")
            # Assuming download_blob_data returns the blob content as bytes or string
            chunk_content_str = storage_helpers.download_blob_as_text(
                container_name=source_container,
                blob_name=blob_name
            )
            # Assuming each chunk is a JSON list/array
            chunk_data = data_utils.deserialize_data(chunk_content_str)
            if isinstance(chunk_data, list):
                combined_data.extend(chunk_data)
            else:
                logging.warning(
                    f"Job {job_id}: Chunk '{blob_name}' did not contain a JSON list. Type: {type(chunk_data)}")
                # Handle non-list data if necessary (e.g., append if dict, skip, or raise error)

        logging.info(
            f"Job {job_id}: Successfully downloaded and combined data from {len(chunk_blob_names)} chunks. Total rows: "
            f"{len(combined_data)}")

    except Exception as download_err:
        error_msg = f"Job {job_id}: Failed during chunk download or deserialization: {download_err}"
        logging.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from download_err

    # --- Upload Combined Data ---
    try:
        logging.info(f"Job {job_id}: Uploading combined data to '{target_container}/{target_blob_name}'.")
        combined_json_string = data_utils.serialize_data(combined_data)
        storage_helpers.upload_blob_data(container_name=target_container, blob_name=target_blob_name,
                                         data=combined_json_string)
        logging.info(f"Job {job_id}: Successfully uploaded combined blob.")

    except Exception as upload_err:
        error_msg = f"Job {job_id}: Failed to upload combined blob '{target_blob_name}': {upload_err}"
        logging.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from upload_err

    # --- Return Success ---
    return {
        "container_name": target_container,
        "blob_name": target_blob_name
    }
