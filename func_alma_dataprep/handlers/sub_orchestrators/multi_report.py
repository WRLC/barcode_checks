"""Suborchestrator for workflows that combine multiple reports, merge them, and send an email notification."""

import logging
from typing import Dict, Any
import azure.functions as func
import azure.durable_functions as df

bp = func.Blueprint()


# noinspection PyUnusedLocal
@bp.orchestration_trigger(context_name="context")
def multi_report_email_workflow(context: df.DurableOrchestrationContext):
    """Suborchestrator for workflows that combine multiple reports, merge them, and send an email notification.

    This suborchestrator expects a list of report metadata dictionaries.
    It calls an activity to combine chunks for each report in parallel.
    Then, it calls a new activity to merge these combined reports into a single file.
    Finally, it calls another activity to process and send the notification for the merged report.

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
    report_metadata_list: list[Dict[str, Any]] = input_data.get("report_metadata_list", [])

    logging.info(f"Suborchestrator 'multi_report_email_workflow' started for job {job_id}.")

    # --- Validate Input ---
    if not report_metadata_list:
        error_msg = (f"Job {job_id}: Expected at least one report metadata entry for 'multi_report_email_workflow', "
                     f"found {len(report_metadata_list)}.")
        logging.error(error_msg)
        context.set_custom_status({"error": error_msg})
        raise ValueError(error_msg)

    logging.info(f"Job {job_id}: Processing {len(report_metadata_list)} reports.")

    # --- Call Activity to Combine Chunks for Each Report (in parallel) ---
    combine_tasks = []
    target_container = "alma-combined-reports"  # Example target container for individual combined reports
    for report_metadata in report_metadata_list:
        report_name_safe = report_metadata.get('report_name_safe', f'report_{report_metadata.get("sequence_number")}')
        # Create a unique blob name for each combined report before merging
        target_blob_name = f"{job_id}/{report_name_safe}_combined.json"

        combine_activity_input = {
            "job_id": job_id,
            "report_metadata": report_metadata,
            "target_container": target_container,
            "target_blob_name": target_blob_name
        }
        logging.debug(f"Job {job_id}: Adding task to combine chunks for report: {report_name_safe}")
        combine_tasks.append(context.call_activity("combine_report_chunks_activity", combine_activity_input))

    combined_blob_info_list = []
    try:
        logging.info(f"Job {job_id}: Calling 'combine_report_chunks_activity' for {len(combine_tasks)} reports.")
        # Results will be a list of dictionaries like {"container_name": "...", "blob_name": "..."}
        combined_blob_info_list = yield context.task_all(combine_tasks)
        logging.info(f"Job {job_id}: Successfully combined chunks for all {len(combined_blob_info_list)} reports.")
        # Basic validation of results
        if not all(isinstance(info, dict) and info.get("container_name") and info.get("blob_name")
                   for info in combined_blob_info_list):
            raise ValueError("One or more combine activities did not return valid container/blob names.")

    except Exception as combine_ex:
        error_msg = f"Job {job_id}: One or more 'combine_report_chunks_activity' calls failed: {combine_ex}"
        logging.error(error_msg, exc_info=True)
        context.set_custom_status({"error": error_msg})
        raise  # Re-raise to fail the suborchestration

    # --- Call Activity to Merge Combined Reports ---
    # Define where the final merged report should go
    final_merged_container = "alma-merged-reports"  # Example container for the final merged file
    final_merged_blob_name = f"{job_id}/merged_report_{trigger_config_id}.json"  # Example final blob name

    merge_activity_input = {
        "job_id": job_id,
        "trigger_config_id": trigger_config_id,
        "combined_blob_info_list": combined_blob_info_list,  # Pass the list of dicts
        "target_container": final_merged_container,
        "target_blob_name": final_merged_blob_name,
        "config_details": config_details  # Pass config if merge logic needs it (e.g., header map)
    }

    merged_blob_info = {}
    try:
        logging.info(f"Job {job_id}: Calling activity 'merge_reports_activity'.")
        # This activity needs to be created. It takes the list of combined blob paths,
        # downloads, merges them, uploads the final result, and returns its path.
        # Expected return: {"container_name": "alma-merged-reports", "blob_name": "job_id/merged_report_config_id.json"}
        merged_blob_info = yield context.call_activity("merge_reports_activity", merge_activity_input)

        merged_data_container = merged_blob_info.get("container_name")
        merged_data_blob = merged_blob_info.get("blob_name")

        if not merged_data_container or not merged_data_blob:
            raise ValueError("Merge activity did not return valid container/blob name.")

        logging.info(f"Job {job_id}: Reports merged into: {merged_data_container}/{merged_data_blob}")

    except Exception as merge_ex:
        error_msg = f"Job {job_id}: Activity 'merge_reports_activity' failed: {merge_ex}"
        logging.error(error_msg, exc_info=True)
        context.set_custom_status({"error": error_msg})
        raise  # Re-raise to fail the suborchestration

# --- Prepare Input for Notification Activity (using the final merged blob) ---
    # Extract the list of all connector IDs involved in this job
    iz_analysis_connector_ids = input_data.get("iz_analysis_connector_ids", [])
    if not iz_analysis_connector_ids:
        # Log a warning or raise an error if the list is unexpectedly empty
        logging.warning(f"Job {job_id}: 'iz_analysis_connector_ids' list is empty in input for multi-report workflow.")
        # Depending on requirements, you might want to raise an error here:
        # raise ValueError(f"Job {job_id}: Missing 'iz_analysis_connector_ids' list.")

    notification_activity_input = {
        "job_id": job_id,
        "trigger_config_id": trigger_config_id,
        # Pass the list of all connector IDs
        "iz_analysis_connector_ids": iz_analysis_connector_ids,
        "combined_data_container": merged_data_container,  # Use the final merged container
        "combined_data_blob": merged_data_blob,  # Use the final merged blob
        "iz_code": config_details.get("iz_code"),
        "analysis_id": config_details.get("analysis_id"),
        "analysis_name": config_details.get("analysis_name"),
        "header_map": config_details.get("header_map")
        # Add any other details needed by the notification activity
    }

    # --- Call Notification Activity Function ---
    try:
        logging.info(f"Job {job_id}: Calling activity 'process_and_send_single_report_activity' for merged report.")
        # Using the existing activity, assuming it can handle the payload structure
        # with the path to the final merged report.
        result = yield context.call_activity("process_and_send_single_report_activity", notification_activity_input)
        logging.info(f"Job {job_id}: Activity 'process_and_send_single_report_activity' completed for merged report.")
        context.set_custom_status({"status": "Completed", "result": result})
        return result
    except Exception as activity_ex:
        error_msg = (f"Job {job_id}: Activity 'process_and_send_single_report_activity' failed for merged report: "
                     f"{activity_ex}")
        logging.error(error_msg, exc_info=True)
        context.set_custom_status({"error": error_msg})
        # Re-raise to fail the suborchestration
        raise
