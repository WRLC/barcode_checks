"""Durable Function to handle messages from a queue and orchestrate data preparation tasks."""

import logging
import json
from typing import Generator, Any, Dict
import azure.functions as func
import azure.durable_functions as df
from shared_code.config import NOTIFIER_QUEUE_NAME
from shared_code.utils import storage_helpers, data_utils
from shared_code.database import session_scope
from shared_code.database.models import TimerTriggerConfig

# Define the Function App. In a real app, this might be in a shared location.
bp = func.Blueprint()  # Or appropriate auth level


@bp.queue_trigger(
    arg_name="msg",
    queue_name="datacombinequeue", connection="AzureWebJobsStorage"
)  # Trigger binding - Updated queue name
@bp.durable_client_input(client_name="starter")  # Durable client input binding
async def dataprep_queue_trigger(msg: func.QueueMessage, starter: df.DurableOrchestrationClient):
    """Azure Function triggered by messages in 'datacombinequeue'.

    Acts as the client for the Durable Orchestration using v4 model decorators.
    Starts the orchestrator for the first report or signals an entity/orchestrator
    for subsequent reports.

    Args:
        msg (func.QueueMessage): The message from the queue.
        starter (df.DurableOrchestrationClient): The Durable Functions client.

    Returns:
        None: The function does not return a value.

    Raises:
        ValueError: If the message payload is invalid or missing required fields.
        RuntimeError: If any error occurs during orchestration or signaling.
        json.JSONDecodeError: If the message body cannot be decoded as JSON.
        Exception: For any other unexpected errors.

    """
    job_id: int | None = None  # Initialize job_id
    try:
        message_body = data_utils.deserialize_data(msg.get_body())
        if not isinstance(message_body, dict):
            raise ValueError("Msg body not dict.")

        # --- Get parameters ---
        job_id: str = message_body.get("job_id")
        trigger_config_id: int = message_body.get("trigger_config_id")
        iz_analysis_connector_id: int = message_body.get("iz_analysis_connector_id")
        report_path: str = message_body.get("report_path")
        report_name_safe: str = message_body.get("report_name_safe")
        total_chunks: int = message_body.get("total_chunks")
        chunk_container: str = message_body.get("container_name")
        header_map: dict | None = message_body.get("header_map")
        iz_code: int = message_body.get("iz_code")
        analysis_id: int = message_body.get("analysis_id")
        analysis_name: str = message_body.get("analysis_name")
        sequence_number: int = message_body.get("report_sequence_number")
        total_reports: int = message_body.get("total_reports_expected")
        container_name: int = message_body.get("container_name")

        if not all(
                [
                    job_id, trigger_config_id, iz_analysis_connector_id, report_name_safe, total_chunks,
                    chunk_container, header_map, iz_code, analysis_id, analysis_name, sequence_number, total_reports,
                    container_name
                ]
        ):
            logging.error(f"Invalid message payload received: {message_body}. Missing required chunk metadata fields.")
            return

        instance_id = f"dp-{job_id}"
        entity_id = df.EntityId("ReportTrackerEntity", job_id)

        existing_instance = await starter.get_status(instance_id)
        is_orchestrator_running = existing_instance and existing_instance.runtime_status not in [
            df.OrchestrationRuntimeStatus.Completed,
            df.OrchestrationRuntimeStatus.Failed,
            df.OrchestrationRuntimeStatus.Terminated,
        ]

        report_metadata = {
            "sequence_number": sequence_number,
            "report_name_safe": report_name_safe,
            "container_name": container_name,
            "total_chunks": total_chunks
        }

        if sequence_number == 1:
            if not is_orchestrator_running:
                logging.info(f"Starting orchestrator instance '{instance_id}' for job '{job_id}'.")
                # Pass initial data needed by the orchestrator
                orchestrator_input = {
                    "job_id": job_id,
                    "trigger_config_id": trigger_config_id,
                    "iz_analysis_connector_id": iz_analysis_connector_id,
                    "report_path": report_path,
                    "iz_code": iz_code,
                    "header_map": header_map,
                    "analysis_id": analysis_id,
                    "analysis_name": analysis_name,
                    "total_reports_expected": total_reports,
                    "initial_report_metadata": report_metadata,
                }
                await starter.start_new(
                    orchestration_function_name="orchestrator_function",
                    instance_id=instance_id,
                    client_input=orchestrator_input
                )
            else:
                logging.warning(
                    f"Received message with sequence_number 1 for job '{job_id}', but orchestrator instance "
                    f"'{instance_id}' is already running ({existing_instance.runtime_status}). Ignoring start request."
                )

        elif sequence_number > 1:
            if is_orchestrator_running:
                logging.info(
                    f"Signaling running orchestrator '{instance_id}' with report {sequence_number}/{total_reports} "
                    f"for job '{job_id}'."
                )
                await starter.raise_event(instance_id, "ReportMetadataReady", report_metadata)
            else:
                logging.info(
                    f"Orchestrator '{instance_id}' not running. Signaling entity '{entity_id.key}' to store report "
                    f"{sequence_number}/{total_reports} for job '{job_id}'."
                )
                # The "addReport" operation needs to be implemented in your ReportTrackerEntity
                await starter.signal_entity(entity_id, "addReportMetata", report_metadata)
        else:
            logging.error(f"Invalid sequence_number '{sequence_number}' received for job '{job_id}'.")

    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from queue message: {e}. Message body: {msg.get_body().decode()}")
        # Handle poison message (e.g., let it requeue up to max count then dead-letter)
        raise
    except Exception as e:
        logging.error(f"Error processing queue message for job '{job_id if 'job_id' in locals() else 'unknown'}': {e}",
                      exc_info=True)
        # Handle unexpected errors
        raise


# noinspection PyUnusedLocal
@bp.orchestration_trigger(context_name="context")
def orchestrator_function(context: df.DurableOrchestrationContext) -> Generator[Any, Any, Any]:
    """Orchestrator function to handle data preparation tasks.

    This function is triggered by the queue message and orchestrates the workflow.
    It collects reports, validates workflow details, and routes to the appropriate sub-orchestrator.

    Args:
        context (df.DurableOrchestrationContext): The context for the orchestration.

    Returns:
        Generator[Any, Any, Any]: The result of the orchestration.

    Raises:
        ValueError: If required fields are missing or invalid in the input data.
        RuntimeError: If any error occurs during activity calls or processing.
        Exception: For any other unexpected errors.

    """

    input_data = context.get_input()
    job_id = input_data.get("job_id")
    trigger_config_id = input_data.get("trigger_config_id")
    iz_analysis_connector_id = input_data.get("iz_analysis_connector_id")
    iz_code = input_data.get("iz_code")
    header_map = input_data.get("header_map")
    analysis_id = input_data.get("analysis_id")
    analysis_name = input_data.get("analysis_name")
    total_reports_expected = input_data.get("total_reports_expected")
    initial_report_metadata = input_data.get("initial_report_metadata")

    if not trigger_config_id:
        error_msg = f"Orchestrator for job {job_id} received no trigger_config_id."
        logging.error(error_msg)
        raise ValueError(error_msg)

    workflow_details = {
        "iz_analysis_connector_id": iz_analysis_connector_id,
        "iz_code": iz_code,
        "header_map": header_map,
        "analysis_id": analysis_id,
        "analysis_name": analysis_name
    }

    logging.info(f"Orchestrator {context.instance_id} started for job {job_id}, trigger_config_id {trigger_config_id}.")

    # --- Collect all reports ---
    # This dictionary will store {sequence_number: report_metadata_dict}
    received_reports_metadata = {}
    if initial_report_metadata:
        seq_num = initial_report_metadata.get("sequence_number")
        if seq_num is not None:
            received_reports_metadata[seq_num] = initial_report_metadata
            logging.info(
                f"Orchestrator {context.instance_id} received initial report metadata "
                f"{seq_num}/{total_reports_expected}"
            )
        else:
            logging.warning(
                f"Orchestrator {context.instance_id} received initial report metadata without sequence number: "
                f"{initial_report_metadata}"
            )

    # Wait for subsequent reports using "ReportMetadataReady" event
    while len(received_reports_metadata) < total_reports_expected:
        # The event payload should be the report_metadata dictionary
        report_metadata_event = yield context.wait_for_external_event("ReportMetadataReady")
        seq_num = report_metadata_event.get("sequence_number")
        # Basic validation of the received metadata
        if seq_num is not None and all(
                k in report_metadata_event for k in ["report_name_safe", "container_name", "total_chunks"]):
            received_reports_metadata[seq_num] = report_metadata_event
            logging.info(
                f"Orchestrator {context.instance_id} received report metadata {seq_num}/{total_reports_expected}")
        else:
            logging.warning(
                f"Orchestrator {context.instance_id} received invalid ReportMetadataReady event: "
                f"{report_metadata_event}"
            )

    logging.info(
        f"Orchestrator {context.instance_id} received all {total_reports_expected} report metadata entries for job "
        f"{job_id}."
    )

    # --- Prepare input for Suborchestrator ---
    # Sort the collected metadata by sequence number before passing
    sorted_report_metadata = [received_reports_metadata[k] for k in sorted(received_reports_metadata.keys())]

    sub_orchestrator_input = {
        "job_id": job_id,
        "trigger_config_id": trigger_config_id,
        "config_details": workflow_details,  # Pass all details retrieved
        "report_metadata_list": sorted_report_metadata  # Pass the list of metadata dicts
    }

    # --- Get Dataprep Handler Type from DB ---
    dataprep_handler_type = None
    email_subject_from_config = None
    try:
        with session_scope() as db_session:
            config = db_session.get(TimerTriggerConfig, trigger_config_id)
            if not config:
                raise ValueError(f"TimerTriggerConfig not found for ID {trigger_config_id}")
            dataprep_handler_type = config.dataprep_handler_type
            email_subject_from_config = config.email_subject
            if not dataprep_handler_type:
                raise ValueError(f"dataprep_handler_type not set for TimerTriggerConfig ID {trigger_config_id}")
            logging.info(
                f"Job {job_id}: Found dataprep handler type '{dataprep_handler_type}' and email subject "
                f"'{email_subject_from_config}' from config.")
    except Exception as db_err:
        logging.exception(f"Job {job_id}: Failed to retrieve TimerTriggerConfig ID {trigger_config_id}: {db_err}")
        raise db_err

    # --- Routing to Suborchestrator based on dataprep_handler_type ---
    result = None
    try:
        # Use the dataprep_handler_type value for routing
        if dataprep_handler_type == "single_report_email":  # Example handler type value
            logging.info(f"Calling suborchestrator 'single_report_email_workflow' for job {job_id}")
            # Ensure the suborchestrator 'single_report_email_workflow' is updated
            # to handle 'report_metadata_list' instead of 'report_blob_paths'
            # and potentially call a new 'combine_chunks_activity' if needed.
            result = yield context.call_sub_orchestrator("single_report_email_workflow", sub_orchestrator_input)

        # Add more elif branches for other possible values of dataprep_handler_type
        # elif dataprep_handler_type == "multi_report_email":
        #     logging.info(f"Calling suborchestrator 'multi_report_email_workflow' for job {job_id}")
        #     result = yield context.call_sub_orchestrator("multi_report_email_workflow", sub_orchestrator_input)
        # elif dataprep_handler_type == "email_summary":
        #     logging.info(f"Calling suborchestrator 'email_summary_workflow' for job {job_id}")
        #     result = yield context.call_sub_orchestrator("email_summary_workflow", sub_orchestrator_input)

        else:
            # Handle unknown handler type derived from DB
            error_msg = (
                f"Unknown or unsupported dataprep_handler_type '{dataprep_handler_type}' derived from "
                f"trigger_config_id {trigger_config_id} for job {job_id}"
            )
            logging.error(error_msg)
            raise ValueError(error_msg)

        logging.info(f"Suborchestrator for job {job_id} (handler: {dataprep_handler_type}) completed. Result: {result}")

    except Exception as e:
        logging.error(f"Suborchestrator for handler '{dataprep_handler_type}' failed for job {job_id}: {e}",
                      exc_info=True)
        raise  # Re-raise to fail the main orchestration

    # ... process result from suborchestrator if needed ...
    return result


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
    notification_activity_input = {
        "job_id": job_id,
        "trigger_config_id": trigger_config_id,
        "iz_analysis_connector_id": config_details.get("iz_analysis_connector_id"),
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


# --- Activity: Combine Report Chunks ---
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


# --- Activity: Process and Send Single Report ---
@bp.activity_trigger(input_name="payload")
def process_and_send_single_report_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Activity function to prepare and send a notification message for a single report.

    This function is called by the suborchestrator after combining report chunks.
    It prepares the notification payload and sends it to the Notifier Queue.

    Args:
        payload (Dict[str, Any]): The input payload containing job and report details.

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
    # Mirrors the 'next_message_payload' from the old combiner's SimpleEmailNotifier path
    notification_payload = {
        "job_id": job_id,
        "trigger_config_id": trigger_config_id,
        "iz_analysis_connector_id": payload.get("iz_analysis_connector_id"),
        "combined_data_container": combined_data_container,
        "combined_data_blob": combined_data_blob,
        "status": "ReadyForNotification",  # Or "Combined"
        "iz_code": payload.get("iz_code"),
        "analysis_id": payload.get("analysis_id"),
        "analysis_name": payload.get("analysis_name"),
        "header_map": payload.get("header_map")
    }

    # --- Send Message to Notifier Queue ---
    try:
        logging.info(f"Job {job_id}: Sending notification message to queue '{NOTIFIER_QUEUE_NAME}'.")
        # Corrected parameter name from message_payload to message_content
        # Assuming connection_string_name is still a valid (optional) parameter
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
