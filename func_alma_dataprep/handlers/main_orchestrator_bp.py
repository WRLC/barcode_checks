"""Durable Function to handle messages from a queue and orchestrate data preparation tasks."""

import logging
import json
from typing import Generator, Any
import azure.functions as func
import azure.durable_functions as df
from shared_code.utils import data_utils
from shared_code.database import get_db_session
from shared_code.database.models import TimerTriggerConfig

bp = func.Blueprint()


@bp.queue_trigger(
    arg_name="msg",
    queue_name="datacombinequeue", connection="AzureWebJobsStorage"
)
@bp.durable_client_input(client_name="starter")
async def dataprep_queue_trigger(msg: func.QueueMessage, starter: df.DurableOrchestrationClient):
    """Azure Function triggered by messages in 'datacombinequeue'."""
    job_id: int | None = None
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
            "total_chunks": total_chunks,
            "iz_analysis_connector_id": iz_analysis_connector_id
        }

        if sequence_number == 1:
            if not is_orchestrator_running:
                logging.info(f"Starting orchestrator instance '{instance_id}' for job '{job_id}'.")
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
                await starter.signal_entity(entity_id, "addReportMetata", report_metadata)
        else:
            logging.error(f"Invalid sequence_number '{sequence_number}' received for job '{job_id}'.")

    except json.JSONDecodeError as e:
        logging.error(f"Failed to decode JSON from queue message: {e}. Message body: {msg.get_body().decode()}")
        raise
    except Exception as e:
        logging.error(f"Error processing queue message for job '{job_id if 'job_id' in locals() else 'unknown'}': {e}",
                      exc_info=True)
        raise


# noinspection PyUnusedLocal
@bp.orchestration_trigger(context_name="context")
def orchestrator_function(context: df.DurableOrchestrationContext) -> Generator[Any, Any, Any]:
    """Orchestrator function to handle data preparation tasks."""
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
        "iz_code": iz_code,
        "header_map": header_map,
        "analysis_id": analysis_id,
        "analysis_name": analysis_name
    }

    logging.info(f"Orchestrator {context.instance_id} started for job {job_id}, trigger_config_id {trigger_config_id}.")

    # --- Collect all reports ---
    received_reports_metadata = {}
    iz_analysis_connector_ids = set()
    if initial_report_metadata:
        seq_num = initial_report_metadata.get("sequence_number")
        if seq_num is not None:
            received_reports_metadata[seq_num] = initial_report_metadata
            iz_analysis_connector_ids.add(initial_report_metadata.get("iz_analysis_connector_id"))
            logging.info(
                f"Orchestrator {context.instance_id} received initial report metadata "
                f"{seq_num}/{total_reports_expected}"
            )

    while len(received_reports_metadata) < total_reports_expected:
        report_metadata_event = yield context.wait_for_external_event("ReportMetadataReady")
        seq_num = report_metadata_event.get("sequence_number")
        if seq_num is not None and all(
                k in report_metadata_event for k in ["report_name_safe", "container_name", "total_chunks"]):
            received_reports_metadata[seq_num] = report_metadata_event
            iz_analysis_connector_ids.add(report_metadata_event.get("iz_analysis_connector_id"))
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

    sorted_report_metadata = [received_reports_metadata[k] for k in sorted(received_reports_metadata.keys())]

    sub_orchestrator_input = {
        "job_id": job_id,
        "trigger_config_id": trigger_config_id,
        "config_details": workflow_details,
        "report_metadata_list": sorted_report_metadata,
        "iz_analysis_connector_ids": list(iz_analysis_connector_ids)
    }

    dataprep_handler_type = None
    try:
        with get_db_session() as db_session:
            config = db_session.get(TimerTriggerConfig, trigger_config_id)
            if not config:
                raise ValueError(f"TimerTriggerConfig not found for ID {trigger_config_id}")
            dataprep_handler_type = config.dataprep_handler_type
            logging.info(
                f"Job {job_id}: Found dataprep handler type '{dataprep_handler_type}' from config.")
    except Exception as db_err:
        logging.exception(f"Job {job_id}: Failed to retrieve TimerTriggerConfig ID {trigger_config_id}: {db_err}")
        raise db_err

    logging.warning(
        f"Job {job_id}: Checking handler type. Value is: '{dataprep_handler_type}' (Type: "
        f"{type(dataprep_handler_type)})"
    )

    result = None
    try:
        if dataprep_handler_type == "single_report_email":
            logging.info(f"Calling suborchestrator 'single_report_email_workflow' for job {job_id}")
            result = yield context.call_sub_orchestrator("single_report_email_workflow", sub_orchestrator_input)
        elif dataprep_handler_type == "multi_report_email":
            logging.info(f"Calling suborchestrator 'multi_report_email_workflow' for job {job_id}")
            result = yield context.call_sub_orchestrator("multi_report_email_workflow", sub_orchestrator_input)
        else:
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
        raise

    return result
