"""Activity to process and send notifications for report generation."""
import logging
from typing import Dict, Any
import azure.functions as func
from shared_code.config import NOTIFIER_QUEUE_NAME
from shared_code import storage_helpers

bp = func.Blueprint()


@bp.activity_trigger(input_name="payload")
def process_and_send_single_report_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Activity function to prepare and send a notification message for a single or merged report.

    This function is called by the suborchestrator after combining/merging report chunks.
    It prepares the notification payload, including either a single connector ID or a list
    of connector IDs, and sends it to the Notifier Queue.

    Args:
        payload (Dict[str, Any]): The input payload containing job and report details.
            May contain 'iz_analysis_connector_id' (int) or 'iz_analysis_connector_ids' (list[int]).

    Returns:
        Dict[str, Any]: A dictionary indicating the status of the notification.

    Raises:
        ValueError: If required fields are missing or invalid in the payload.
        RuntimeError: If any error occurs during message sending.

    """
    job_id = payload.get("job_id")
    trigger_config_id = payload.get("trigger_config_id")
    combined_data_blob = payload.get("combined_data_blob")
    combined_data_container = payload.get("combined_data_container")

    logging.info(f"Activity 'process_and_send_single_report' started for job {job_id}, "
                 f"trigger {trigger_config_id}, blob {combined_data_container}/{combined_data_blob}.")

    # --- Validate Input ---
    if not all([job_id, trigger_config_id, combined_data_container, combined_data_blob]):
        error_msg = f"Job {job_id}: Missing required fields in activity payload: {list(payload.keys())}"
        logging.error(error_msg)
        raise ValueError(error_msg)

    # --- Prepare Notification Payload ---
    # Base payload structure
    notification_payload = {
        "job_id": job_id,
        "trigger_config_id": trigger_config_id,
        "combined_data_container": combined_data_container,
        "combined_data_blob": combined_data_blob,
        "status": "ReadyForNotification",
        "iz_code": payload.get("iz_code"),
        "analysis_id": payload.get("analysis_id"),
        "analysis_name": payload.get("analysis_name"),
        "header_map": payload.get("header_map")
    }

    # Add the appropriate connector ID field based on what's in the payload
    if "iz_analysis_connector_id" in payload:
        # Came from single_report_email_workflow
        notification_payload["iz_analysis_connector_id"] = payload.get("iz_analysis_connector_id")
        logging.info(
            f"Job {job_id}: Using single iz_analysis_connector_id: {notification_payload['iz_analysis_connector_id']}"
        )
    elif "iz_analysis_connector_ids" in payload:
        # Came from multi_report_email_workflow
        notification_payload["iz_analysis_connector_ids"] = payload.get("iz_analysis_connector_ids")
        logging.info(
            f"Job {job_id}: Using list of iz_analysis_connector_ids: "
            f"{notification_payload['iz_analysis_connector_ids']}"
        )
    else:
        # Log a warning if neither is present, as one is expected
        logging.warning(
            f"Job {job_id}: Neither 'iz_analysis_connector_id' nor 'iz_analysis_connector_ids' found in payload.")
        # Depending on requirements, you might raise an error here if one is strictly required.
        # raise ValueError(f"Job {job_id}: Missing connector ID information in payload.")

    # --- Send Message to Notifier Queue ---
    try:
        logging.info(
            f"Job {job_id}: Sending notification message to queue '{NOTIFIER_QUEUE_NAME}'. Payload keys: "
            f"{list(notification_payload.keys())}"
        )
        storage_helpers.send_queue_message(
            queue_name=NOTIFIER_QUEUE_NAME,
            message_content=notification_payload,
        )
        logging.info(f"Job {job_id}: Successfully sent message to '{NOTIFIER_QUEUE_NAME}'.")
        return {"status": "NotificationSent", "queue": NOTIFIER_QUEUE_NAME}
    except Exception as queue_err:
        logging.error(f"Job {job_id}: Failed to send message to queue '{NOTIFIER_QUEUE_NAME}': {queue_err}",
                      exc_info=True)
        # Raise the error to signal failure to the suborchestrator
        raise queue_err
