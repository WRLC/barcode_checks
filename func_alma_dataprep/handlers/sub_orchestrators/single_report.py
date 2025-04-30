"""Suborchestrator for workflows that send an email notification for a single report."""

import logging
from typing import Dict, Any
import azure.functions as func
import azure.durable_functions as df

bp = func.Blueprint()


# noinspection PyTypeChecker
@bp.orchestration_trigger(context_name="context")
def single_report_email_workflow(context: df.DurableOrchestrationContext):
    """Suborchestrator for workflows that process a single report and send an email notification.

    This suborchestrator expects a list of report metadata dictionaries.
    It calls an activity to combine chunks for the single report and then
    calls another activity to process and send the notification.

    Args:
        context (df.DurableOrchestrationContext): The context for the orchestration.

    Returns:
        Any: The result of the orchestration.

    Raises:
        ValueError: If the input data is invalid or missing required fields.
        RuntimeError: If any error occurs during activity calls or processing.

    """
    input_data: Dict[str, Any] = context.get_input()
    job_id = input_data.get("job_id")
    trigger_config_id = input_data.get("trigger_config_id")
    config_details = input_data.get("config_details", {})
    # Expecting a list containing one metadata dictionary
    report_metadata_list: list[Dict[str, Any]] = input_data.get("report_metadata_list", [])

    logging.info(f"Suborchestrator 'single_report_email_workflow' started for job {job_id}.")

    # --- Validate Input ---
    if not report_metadata_list or len(report_metadata_list) != 1:
        error_msg = (f"Job {job_id}: Expected exactly one report metadata entry for 'single_report_email_workflow', "
                     f"found {len(report_metadata_list)}.")
        logging.error(error_msg)
        context.set_custom_status({"error": error_msg})
        raise ValueError(error_msg)

    # Extract the single report metadata
    single_report_metadata = report_metadata_list[0]
    logging.info(f"Job {job_id}: Processing report metadata: {single_report_metadata}")

    # --- Call Activity to Combine Chunks ---
    combine_activity_input = {
        "job_id": job_id,
        "report_metadata": single_report_metadata,
        # Add any other necessary details for combining, e.g., target container/blob name pattern
        "target_container": "alma-combined-reports",  # Example target container
        "target_blob_name": f"{job_id}/{single_report_metadata.get('report_name_safe', 'combined_report')}.json"
        # Example target blob
    }

    try:
        logging.info(f"Job {job_id}: Calling activity 'combine_report_chunks_activity'.")
        # This activity needs to be created. It takes metadata, finds chunks, combines them,
        # and returns the path of the combined blob.
        combined_blob_info = yield context.call_activity("combine_report_chunks_activity", combine_activity_input)
        # Example expected return: {"container_name": "alma-combined-reports", "blob_name": "job_id/report_name.json"}
        combined_data_container = combined_blob_info.get("container_name")
        combined_data_blob = combined_blob_info.get("blob_name")

        if not combined_data_container or not combined_data_blob:
            raise ValueError("Combine activity did not return valid container/blob name.")

        logging.info(f"Job {job_id}: Chunks combined into: {combined_data_container}/{combined_data_blob}")

    except Exception as combine_ex:
        error_msg = f"Job {job_id}: Activity 'combine_report_chunks_activity' failed: {combine_ex}"
        logging.error(error_msg, exc_info=True)
        context.set_custom_status({"error": error_msg})
        raise  # Re-raise to fail the suborchestration

    # --- Prepare Input for Notification Activity ---
    # Extract the specific connector ID for this single report
    single_iz_analysis_connector_id = single_report_metadata.get("iz_analysis_connector_id")
    if not single_iz_analysis_connector_id:
        error_msg = f"Job {job_id}: Missing 'iz_analysis_connector_id' in report metadata for single report workflow."
        logging.error(error_msg)
        context.set_custom_status({"error": error_msg})
        raise ValueError(error_msg)

    notification_activity_input = {
        "job_id": job_id,
        "trigger_config_id": trigger_config_id,
        # Use the ID from the specific report's metadata
        "iz_analysis_connector_id": single_iz_analysis_connector_id,
        "combined_data_container": combined_data_container,
        "combined_data_blob": combined_data_blob,
        "iz_code": config_details.get("iz_code"),
        "analysis_id": config_details.get("analysis_id"),
        "analysis_name": config_details.get("analysis_name"),
        "header_map": config_details.get("header_map")
    }
    # --- Call Notification Activity Function ---
    try:
        logging.info(f"Job {job_id}: Calling activity 'process_and_send_single_report_activity'.")
        # This activity now takes the combined blob path and sends the notification
        result = yield context.call_activity("process_and_send_single_report_activity", notification_activity_input)
        logging.info(f"Job {job_id}: Activity 'process_and_send_single_report_activity' completed.")
        context.set_custom_status({"status": "Completed", "result": result})
        return result
    except Exception as activity_ex:
        error_msg = f"Job {job_id}: Activity 'process_and_send_single_report_activity' failed: {activity_ex}"
        logging.error(error_msg, exc_info=True)
        context.set_custom_status({"error": error_msg})
        # Re-raise to fail the suborchestration
        raise
