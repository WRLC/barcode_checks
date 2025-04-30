"""Durable Function to handle messages from a queue and orchestrate data preparation tasks."""

import logging
import json
import time
import asyncio
from typing import Generator, Any, Dict, List, Set
import azure.functions as func
import azure.durable_functions as df
from shared_code.utils import data_utils
from shared_code.database import get_db_session
from shared_code.database.models import TimerTriggerConfig

bp = func.Blueprint()


# noinspection PyUnusedLocal
@bp.queue_trigger(
    arg_name="msg",
    queue_name="datacombinequeue", connection="AzureWebJobsStorage"
)
@bp.durable_client_input(client_name="starter")
async def dataprep_queue_trigger(msg: func.QueueMessage, starter: df.DurableOrchestrationClient):
    """Azure Function triggered by messages in 'datacombinequeue'."""
    job_id: str | None = None  # Initialize job_id
    instance_id: str | None = None  # Initialize instance_id
    sequence_number: int | None = None  # Initialize sequence_number
    total_reports: int | None = None  # Initialize total_reports

    try:
        message_body = data_utils.deserialize_data(msg.get_body())
        if not isinstance(message_body, dict):
            raise ValueError("Msg body not dict.")

        # --- Get parameters ---
        job_id = message_body.get("job_id")
        trigger_config_id = message_body.get("trigger_config_id")
        iz_analysis_connector_id = message_body.get("iz_analysis_connector_id")
        report_path = message_body.get("report_path")  # Needed for initial start
        report_name_safe = message_body.get("report_name_safe")
        total_chunks = message_body.get("total_chunks")
        chunk_container = message_body.get("container_name")
        header_map = message_body.get("header_map")  # Needed for initial start
        iz_code = message_body.get("iz_code")  # Needed for initial start
        analysis_id = message_body.get("analysis_id")  # Needed for initial start
        analysis_name = message_body.get("analysis_name")  # Needed for initial start
        sequence_number = message_body.get("report_sequence_number")
        total_reports = message_body.get("total_reports_expected")

        # Validate required fields more robustly
        required_fields = {
            "job_id": job_id, "trigger_config_id": trigger_config_id,
            "iz_analysis_connector_id": iz_analysis_connector_id, "report_name_safe": report_name_safe,
            "total_chunks": total_chunks, "container_name": chunk_container, "header_map": header_map,
            "iz_code": iz_code, "analysis_id": analysis_id, "analysis_name": analysis_name,
            "report_sequence_number": sequence_number, "total_reports_expected": total_reports,
            "report_path": report_path  # Ensure report_path is also checked
        }
        missing_fields = [k for k, v in required_fields.items() if v is None]
        if missing_fields:
            logging.error(
                f"Invalid message payload received: {message_body}. Missing required fields: {missing_fields}")
            return  # Stop processing if essential data is missing

        instance_id = f"dp-{job_id}"

        # Check the status of an existing orchestration instance.
        existing_instance = await starter.get_status(instance_id)
        status_enum = existing_instance.runtime_status if existing_instance else None

        logging.info(
            f"Received message for job '{job_id}', report {sequence_number}/{total_reports}. "
            f"Checking orchestrator status for instance '{instance_id}'. Initial status: {status_enum}"
        )

        # Define non-running terminal states
        terminal_statuses = {
            df.OrchestrationRuntimeStatus.Completed,
            df.OrchestrationRuntimeStatus.Failed,
            df.OrchestrationRuntimeStatus.Terminated,
            df.OrchestrationRuntimeStatus.Canceled
        }
        # Define states where the orchestrator is considered active/running
        running_statuses = {
            df.OrchestrationRuntimeStatus.Running,
            df.OrchestrationRuntimeStatus.Pending,
            df.OrchestrationRuntimeStatus.ContinuedAsNew
        }

        # Prepare report metadata (used for both start and event)
        report_metadata = {
            "sequence_number": sequence_number,
            "report_name_safe": report_name_safe,
            "container_name": chunk_container,
            "total_chunks": total_chunks,
            "iz_analysis_connector_id": iz_analysis_connector_id
        }

        # --- Logic based on instance status and sequence number ---

        if status_enum is None or status_enum in terminal_statuses:
            # Instance doesn't exist or is finished.
            if sequence_number == 1:
                # Only #1 should attempt to start it.
                logging.info(
                    f"Instance '{instance_id}' not found or in terminal state ({status_enum}). "
                    f"Attempting to start new instance for report {sequence_number}/{total_reports}."
                )
                orchestrator_input = {
                    "job_id": job_id,
                    "trigger_config_id": trigger_config_id,
                    "iz_code": iz_code,
                    "header_map": header_map,
                    "analysis_id": analysis_id,
                    "analysis_name": analysis_name,
                    "total_reports_expected": total_reports,
                    "initial_report_metadata": report_metadata,
                    "report_path": report_path,
                    "iz_analysis_connector_id": iz_analysis_connector_id
                }
                try:
                    await starter.start_new("orchestrator_function", instance_id=instance_id,
                                            client_input=orchestrator_input)
                    logging.info(
                        f"Successfully requested start for orchestrator instance '{instance_id}' for job '{job_id}'.")
                except Exception as start_ex:
                    logging.warning(
                        f"Attempt to start instance '{instance_id}' failed or instance already existed. "
                        f"This might be due to a race condition where another trigger started it concurrently. Error: "
                        f"{start_ex}"
                    )
                    pass  # Assume the other trigger succeeded or will handle it.

            else:  # sequence_number > 1
                # Instance isn't running. Wait and retry for a period before giving up.
                max_wait_seconds = 60
                retry_interval_seconds = 5
                start_time = time.monotonic()
                found_running = False
                logging.info(
                    f"Report {sequence_number}/{total_reports} for job '{job_id}' found instance '{instance_id}' "
                    f"in state {status_enum}. Will wait up to {max_wait_seconds}s for it to become running."
                )

                while time.monotonic() - start_time < max_wait_seconds:
                    await asyncio.sleep(retry_interval_seconds)  # Use asyncio.sleep in async func
                    current_instance = await starter.get_status(instance_id)
                    current_status = current_instance.runtime_status if current_instance else None
                    logging.info(
                        f"Retrying status check for instance '{instance_id}' (Report {sequence_number}/"
                        f"{total_reports}). Current status: {current_status}"
                    )

                    if current_status in running_statuses:
                        logging.info(
                            f"Instance '{instance_id}' is now running ({current_status}). Proceeding to raise event "
                            f"for report {sequence_number}/{total_reports}."
                        )
                        try:
                            await starter.raise_event(
                                instance_id=instance_id,
                                event_name="ReportMetadataReady",
                                event_data=report_metadata
                            )
                            logging.info(
                                f"Successfully raised 'ReportMetadataReady' event to instance '{instance_id}' for "
                                f"job '{job_id}' after retry."
                            )
                            found_running = True
                            break  # Exit retry loop
                        except Exception as raise_ex:
                            # Handle cases where the instance might have terminated between status check and raise_event
                            logging.error(
                                f"Failed to raise 'ReportMetadataReady' event for instance '{instance_id}' "
                                f"(status was {current_status} during check, but might have changed): {raise_ex}",
                                exc_info=True
                            )
                            # Break loop even on failure to raise, as the instance was found running at one point.
                            found_running = True  # Mark as found, even if raise failed
                            break
                    elif current_status in terminal_statuses:
                        logging.warning(
                            f"Instance '{instance_id}' entered terminal state {current_status} while waiting for "
                            f"report {sequence_number}/{total_reports}. Stopping wait."
                        )
                        break  # Stop waiting if it terminated

                if not found_running:
                    logging.error(
                        f"Timed out waiting for instance '{instance_id}' to become running for report "
                        f"{sequence_number}/{total_reports}. Instance status remained {status_enum} or None after "
                        f"{max_wait_seconds}s. Discarding message."
                    )

        elif status_enum in running_statuses:
            # Instance is running.
            if sequence_number > 1:
                # This is the expected path for subsequent reports. Raise the event.
                logging.info(
                    f"Instance '{instance_id}' is running ({status_enum}). Raising 'ReportMetadataReady' event for "
                    f"report {sequence_number}/{total_reports}."
                )
                try:
                    await starter.raise_event(
                        instance_id=instance_id,
                        event_name="ReportMetadataReady",
                        event_data=report_metadata
                    )
                    logging.info(
                        f"Successfully raised 'ReportMetadataReady' event to instance '{instance_id}' for "
                        f"job '{job_id}'."
                    )
                except Exception as raise_ex:
                    logging.error(
                        f"Failed to raise 'ReportMetadataReady' event for instance '{instance_id}' (status "
                        f"was {status_enum} during check, but might have changed): {raise_ex}",
                        exc_info=True
                    )

            else:  # sequence_number == 1
                logging.warning(
                    f"Received message with sequence_number 1 for job '{job_id}', but orchestrator instance "
                    f"'{instance_id}' is already running ({status_enum}). Ignoring duplicate start request."
                )

        else:
            # Handle any other unexpected statuses
            logging.warning(
                f"Instance '{instance_id}' has an unexpected or unhandled status '{status_enum}'. "
                f"Taking no action for report {sequence_number}/{total_reports}."
            )

    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from queue message: {e}. Message body: {msg.get_body().decode()}")
    except ValueError as ve:  # Catch specific validation errors
        logging.error(f"Data validation error processing queue message: {ve}", exc_info=True)
    except Exception as e:
        # Log context information along with the exception
        log_context = (f"job_id='{job_id if job_id else 'unknown'}', "
                       f"instance_id='{instance_id if instance_id else 'unknown'}', "
                       f"seq_num='{sequence_number if sequence_number else 'unknown'}'")
        logging.error(f"Unhandled error processing queue message ({log_context}): {e}", exc_info=True)
        # Avoid re-raising to prevent poison message loops by default.
        # Consider dead-lettering logic if needed.


# noinspection PyUnusedLocal,PyUnboundLocalVariable
@bp.orchestration_trigger(context_name="context")
def orchestrator_function(context: df.DurableOrchestrationContext) -> Generator[Any, Any, Any]:
    """Orchestrator function to handle data preparation tasks."""
    input_data = context.get_input()
    job_id = input_data.get("job_id")
    trigger_config_id = input_data.get("trigger_config_id")
    # iz_analysis_connector_id = input_data.get("iz_analysis_connector_id") # This is part of report_metadata now
    iz_code = input_data.get("iz_code")
    header_map = input_data.get("header_map")
    analysis_id = input_data.get("analysis_id")
    analysis_name = input_data.get("analysis_name")
    total_reports_expected = input_data.get("total_reports_expected")
    initial_report_metadata = input_data.get("initial_report_metadata")

    if not trigger_config_id:
        dataprep_handler_error_msg = f"Orchestrator for job {job_id} received no trigger_config_id."
        logging.error(dataprep_handler_error_msg)
        raise ValueError(dataprep_handler_error_msg)

    workflow_details = {
        "iz_code": iz_code,
        "header_map": header_map,
        "analysis_id": analysis_id,
        "analysis_name": analysis_name
    }

    logging.info(f"Orchestrator {context.instance_id} started for job {job_id}, trigger_config_id {trigger_config_id}.")

    # --- Collect all reports ---
    received_reports_metadata: Dict[int, Dict[str, Any]] = {}
    iz_analysis_connector_ids: Set[Any] = set()

    # Process initial report metadata if present
    if initial_report_metadata and isinstance(initial_report_metadata, dict):
        seq_num = initial_report_metadata.get("sequence_number")
        conn_id = initial_report_metadata.get("iz_analysis_connector_id")
        if seq_num == 1 and conn_id is not None:  # Ensure it's the first report
            received_reports_metadata[seq_num] = initial_report_metadata
            iz_analysis_connector_ids.add(conn_id)
            logging.info(
                f"Orchestrator {context.instance_id} processed initial report metadata "
                f"{seq_num}/{total_reports_expected}"
            )
        else:
            logging.warning(
                f"Orchestrator {context.instance_id} received initial report metadata but it was invalid "
                f"or not sequence number 1: {initial_report_metadata}"
            )

    # Wait for subsequent reports
    while len(received_reports_metadata) < total_reports_expected:
        event_payload = yield context.wait_for_external_event("ReportMetadataReady")
        report_metadata_event = None

        # Check if the payload is a string (potentially JSON) and deserialize
        if isinstance(event_payload, str):
            try:
                report_metadata_event = json.loads(event_payload)
                if not isinstance(report_metadata_event, dict):
                    logging.warning(
                        f"Orchestrator {context.instance_id} deserialized event payload but result was not a dict: "
                        f"{type(report_metadata_event)}"
                    )
                    report_metadata_event = None  # Reset if not a dict
                else:
                    logging.info(f"Orchestrator {context.instance_id} successfully deserialized string event payload.")
            except json.JSONDecodeError:
                logging.warning(
                    f"Orchestrator {context.instance_id} received string event payload that is not valid JSON: "
                    f"'{event_payload}'"
                )
                continue  # Skip this event
        elif isinstance(event_payload, dict):
            # Assume it's already a dictionary
            report_metadata_event = event_payload
        else:
            logging.warning(
                f"Orchestrator {context.instance_id} received event payload of unexpected type "
                f"{type(event_payload)}: {event_payload}"
            )
            continue  # Skip this event

        # Now process the dictionary 'report_metadata_event' if valid
        if report_metadata_event:
            seq_num = report_metadata_event.get("sequence_number")
            conn_id = report_metadata_event.get("iz_analysis_connector_id")
            # Check for required keys *after* confirming it's a dictionary
            required_keys = ["report_name_safe", "container_name", "total_chunks", "iz_analysis_connector_id"]
            if seq_num is not None and conn_id is not None and all(k in report_metadata_event for k in required_keys):
                if seq_num not in received_reports_metadata:
                    received_reports_metadata[seq_num] = report_metadata_event
                    iz_analysis_connector_ids.add(conn_id)
                    logging.info(
                        f"Orchestrator {context.instance_id} received and processed report metadata "
                        f"{seq_num}/{total_reports_expected}"
                    )
                else:
                    logging.warning(
                        f"Orchestrator {context.instance_id} received duplicate report metadata for sequence number "
                        f"{seq_num}. Ignoring."
                    )
            else:
                logging.warning(
                    f"Orchestrator {context.instance_id} received invalid or incomplete ReportMetadataReady event "
                    f"dictionary: {report_metadata_event}"
                )
        # Loop continues if more reports are expected

    logging.info(
        f"Orchestrator {context.instance_id} received all {total_reports_expected} report metadata entries for job "
        f"{job_id}."
    )

    # Sort reports by sequence number
    sorted_report_metadata: List[Dict[str, Any]] = [
        received_reports_metadata[k] for k in sorted(received_reports_metadata.keys())
    ]

    sub_orchestrator_input = {
        "job_id": job_id,
        "trigger_config_id": trigger_config_id,
        "config_details": workflow_details,
        "report_metadata_list": sorted_report_metadata,
        "iz_analysis_connector_ids": list(iz_analysis_connector_ids)  # Pass unique connector IDs
    }

    dataprep_handler_type = None
    try:
        # Use context manager for session safety
        with get_db_session() as db_session:
            config = db_session.get(TimerTriggerConfig, trigger_config_id)
            if not config:
                raise ValueError(f"TimerTriggerConfig not found for ID {trigger_config_id}")
            dataprep_handler_type = config.dataprep_handler_type
            logging.info(
                f"Job {job_id}: Found dataprep handler type '{dataprep_handler_type}' from config.")
    except Exception as db_err:
        logging.exception(f"Job {job_id}: Failed to retrieve TimerTriggerConfig ID {trigger_config_id}: {db_err}")
        # Re-raise to fail the orchestration
        raise db_err

    # Check handler type before calling sub-orchestrator
    if not dataprep_handler_type or not isinstance(dataprep_handler_type, str):
        dataprep_handler_error_msg = (
            f"Invalid dataprep_handler_type '{dataprep_handler_type}' (Type: {type(dataprep_handler_type)}) "
            f"retrieved for trigger_config_id {trigger_config_id}. Cannot determine sub-orchestrator."
        )
        logging.error(dataprep_handler_error_msg)
        raise ValueError(dataprep_handler_error_msg)  # Raise an error to fail the orchestration

    # --- Call appropriate sub-orchestrator based on handler type ---
    sub_orchestrator_name = None
    if dataprep_handler_type == "single_report_email":
        sub_orchestrator_name = "single_report_email_workflow"
    elif dataprep_handler_type == "multi_report_email":
        sub_orchestrator_name = "multi_report_email_workflow"
    # Add more handlers here if needed
    # elif dataprep_handler_type == "another_workflow":
    #     sub_orchestrator_name = "another_workflow_orchestrator"
    else:
        error_msg = (
            f"Unknown or unsupported dataprep_handler_type '{dataprep_handler_type}' for trigger_config_id "
            f"{trigger_config_id}. No matching sub-orchestrator found."
        )
        logging.error(error_msg)
        raise ValueError(error_msg)  # Fail if no handler matches

    logging.info(
        f"Orchestrator {context.instance_id} calling sub-orchestrator '{sub_orchestrator_name}' for job {job_id}."
    )

    try:
        # Call the determined sub-orchestrator
        result = yield context.call_sub_orchestrator(sub_orchestrator_name, sub_orchestrator_input)
        logging.info(
            f"Orchestrator {context.instance_id} completed sub-orchestrator '{sub_orchestrator_name}' for "
            f"job {job_id}. Result: {result}"
        )
        return result  # Return the result from the sub-orchestrator
    except Exception as sub_orch_ex:
        logging.exception(
            f"Orchestrator {context.instance_id} encountered an error calling sub-orchestrator "
            f"'{sub_orchestrator_name}' for job {job_id}: {sub_orch_ex}"
        )
        # Re-raise the exception to fail the main orchestrator
        raise sub_orch_ex

    # Note: The actual sub-orchestrator functions (e.g., single_report_email_workflow, multi_report_email_workflow)
    # and any activity functions they call would need to be defined elsewhere, potentially in separate blueprints
    # or within this file if appropriate.
