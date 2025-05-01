"""Activity to merge multiple combined report blobs into a single final blob."""

import logging
from typing import Dict, Any
import azure.functions as func
from shared_code.utils import storage_helpers, data_utils

bp = func.Blueprint()


@bp.activity_trigger(input_name="payload")
def merge_reports_activity(payload: Dict[str, Any]) -> Dict[str, str]:
    """
    Activity function to merge multiple combined report blobs into a single final blob.

    Downloads each combined report (assumed to be a JSON list), concatenates
    the lists, and uploads the final merged list as a new JSON blob.

    Args:
        payload (Dict[str, Any]): Input payload containing job details, list of
                                  combined blobs, target location, and config.
            Expected keys:
            - job_id (str): The identifier for the job.
            - trigger_config_id (int): The configuration ID.
            - combined_blob_info_list (List[Dict[str, str]]): List of dicts,
              each with "container_name" and "blob_name" of a combined report.
            - target_container (str): Container for the final merged blob.
            - target_blob_name (str): Blob name for the final merged blob.
            - config_details (Dict[str, Any]): Configuration details (e.g., header_map),
              currently unused in merge logic but passed for potential future use.

    Returns:
        Dict[str, str]: Dictionary containing the "container_name" and "blob_name"
                        of the final merged blob.

    Raises:
        ValueError: If required fields are missing or invalid in the payload.
        RuntimeError: If any error occurs during blob downloading, merging, or uploading.
    """
    job_id = payload.get("job_id")
    trigger_config_id = payload.get("trigger_config_id")
    combined_blob_info_list = payload.get("combined_blob_info_list", [])
    target_container = payload.get("target_container")
    target_blob_name = payload.get("target_blob_name")
    # config_details = payload.get("config_details", {}) # Available if needed

    logging.info(f"Activity 'merge_reports_activity' started for job {job_id}, trigger {trigger_config_id}.")

    # --- Validate Input ---
    if not all([job_id, trigger_config_id, isinstance(combined_blob_info_list, list),
                target_container, target_blob_name]):
        error_msg = f"Job {job_id}: Missing required fields for merging reports: {payload}"
        logging.error(error_msg)
        raise ValueError(error_msg)

    if not combined_blob_info_list:
        error_msg = f"Job {job_id}: 'combined_blob_info_list' cannot be empty for merging."
        logging.error(error_msg)
        raise ValueError(error_msg)

    # --- Download, Parse, and Merge Data ---
    final_merged_data = []
    try:
        logging.info(f"Job {job_id}: Starting download and merge for {len(combined_blob_info_list)} combined reports.")
        for i, blob_info in enumerate(combined_blob_info_list):
            container = blob_info.get("container_name")
            blob = blob_info.get("blob_name")
            if not container or not blob:
                raise ValueError(f"Invalid blob info at index {i}: {blob_info}")

            logging.debug(
                f"Job {job_id}: Downloading report {i + 1}/{len(combined_blob_info_list)} from '{container}/{blob}'")
            # Assumes download_blob_as_json returns a list (based on combine_report_chunks_activity output)
            report_data = storage_helpers.download_blob_as_json(container_name=container, blob_name=blob)

            if isinstance(report_data, list):
                final_merged_data.extend(report_data)
                logging.debug(f"Job {job_id}: Merged {len(report_data)} records from '{blob}'. "
                              f"Total records now: {len(final_merged_data)}")
            else:
                # Handle cases where a combined report isn't a list if necessary
                logging.warning(
                    f"Job {job_id}: Expected a list from '{container}/{blob}', but got {type(report_data)}. Skipping "
                    f"merge for this file."
                )
                # Optionally raise an error here if non-list data is unacceptable
                # raise TypeError(f"Expected list, got {type(report_data)} from {container}/{blob}")

        logging.info(f"Job {job_id}: Successfully merged data from {len(combined_blob_info_list)} reports. "
                     f"Total final records: {len(final_merged_data)}")

    except Exception as download_merge_err:
        error_msg = f"Job {job_id}: Failed during report download or merging: {download_merge_err}"
        logging.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from download_merge_err

    # --- Upload Final Merged Data ---
    try:
        logging.info(f"Job {job_id}: Uploading final merged data to '{target_container}/{target_blob_name}'.")
        merged_json_string = data_utils.serialize_data(final_merged_data)
        storage_helpers.upload_blob_data(
            container_name=target_container, blob_name=target_blob_name, data=merged_json_string
        )
        logging.info(f"Job {job_id}: Successfully uploaded final merged blob.")

    except Exception as upload_err:
        error_msg = f"Job {job_id}: Failed to upload final merged blob '{target_blob_name}': {upload_err}"
        logging.error(error_msg, exc_info=True)
        raise RuntimeError(error_msg) from upload_err

    # --- Return Success ---
    return {
        "container_name": target_container,
        "blob_name": target_blob_name
    }
