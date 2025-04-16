"""SQLAlchemy ORM models for the Alma API data."""

import logging
import json
import azure.functions as func
from typing import Union, Dict, List, Optional
import datetime
from sqlalchemy import desc
from shared_code.alma_client import AlmaClient, AlmaApiError, AlmaClientConfigurationError
from shared_code.utils import storage_helpers, data_utils, config_helpers
from shared_code.database import session_scope, APIKey, InstitutionZone, AlmaApiPermission, ApiKeyPermission

bp = func.Blueprint()

# --- Constants ---
STORAGE_CONNECTION_STRING_NAME = "AzureWebJobsStorage"
CHUNK_CONTAINER_NAME = config_helpers.get_optional_env_var("CHUNK_CONTAINER_NAME", "alma-report-chunks")
DEFAULT_CHUNK_LIMIT = 1000


# --- Function Definition ---
@bp.queue_trigger(queue_name="ApiFetchQueue", connection=STORAGE_CONNECTION_STRING_NAME, arg_name="msg")
@bp.queue_output(
    queue_name="ApiFetchQueue", connection=STORAGE_CONNECTION_STRING_NAME, arg_name="outputPaginationQueue"
)
@bp.queue_output(
    queue_name="DataCombineQueue", connection=STORAGE_CONNECTION_STRING_NAME, arg_name="outputCombineQueue"
)
def alma_analytics_fetcher(
        msg: func.QueueMessage,
        outputPaginationQueue: func.Out[str],
        outputCombineQueue: func.Out[str]
) -> None:
    """
    Fetches Alma Analytics report chunks based on parameters passed in message,
    including trigger_config_id and iz_analysis_connector_id for context.
    Looks up API key based on IZ code and required permission from message.

    Expected input message format (JSON):
    {
        "job_id": "...",               // Unique ID for the whole job run
        "trigger_config_id": number,   // ID of the TimerTriggerConfig
        "iz_analysis_connector_id": number, // ID linking IZ, Analysis, Path
        "report_path": "/shared/...",  // Provided by Scheduler for efficiency
        "iz_code": "INST_CODE",        // Provided by Scheduler for API key lookup
        "alma_api_permission": "PermName", // Provided by Scheduler for API key lookup
        "resumption_token": null | "token...",
        "chunk_index": 0,
        "limit": 1000, // Optional
        "header_map": null | {...} // Optional
        // Optional: analysis_id, analysis_name for logging/context
    }
    """
    job_id = None
    actual_api_key = None
    # noinspection PyUnusedLocal
    trigger_config_id = None
    # noinspection PyUnusedLocal
    iz_analysis_connector_id = None

    try:
        logging.info(f"Fetcher Received message ID: {msg.id}, DequeueCount: {msg.dequeue_count}")
        message_body = data_utils.deserialize_data(msg.get_body())
        if not isinstance(message_body, dict):
            raise ValueError("Msg body not dict.")

        # --- Get parameters ---
        job_id = message_body.get("job_id")
        trigger_config_id = message_body.get("trigger_config_id")
        iz_analysis_connector_id = message_body.get("iz_analysis_connector_id")

        report_path = message_body.get("report_path")
        iz_code = message_body.get("iz_code")
        permission_name_from_message = message_body.get("alma_api_permission")

        # State variables
        resumption_token = message_body.get("resumption_token")
        chunk_index = message_body.get("chunk_index", 0)
        limit = message_body.get("limit", DEFAULT_CHUNK_LIMIT)
        header_map_from_message: Optional[Dict[str, str]] = message_body.get("header_map")

        # --- Validate essential input ---
        if not job_id:
            raise ValueError("Missing 'job_id'")
        if not trigger_config_id:
            raise ValueError("Missing 'trigger_config_id'")
        if not iz_analysis_connector_id:
            raise ValueError("Missing 'iz_analysis_connector_id'")
        if not iz_code:
            raise ValueError("Missing 'iz_code'")
        if not permission_name_from_message:
            raise ValueError("Missing 'alma_api_permission'")
        if not report_path and not resumption_token:
            raise ValueError("Missing 'report_path' or 'resumption_token'")
        if resumption_token and report_path:
            report_path = None

        logging.info(
            f"Processing Job ID: {job_id}, TriggerCfg: {trigger_config_id}, IZAnalysisConn: {iz_analysis_connector_id}"
            f", Chunk: {chunk_index}, IZ: {iz_code}, Permission: {permission_name_from_message}, "
            f"Report: '{report_path or 'N/A'}'...")

        # --- Get API Key From DB ---
        # noinspection PyUnusedLocal
        api_key_record = None
        try:
            with session_scope() as db_session:
                required_permission = permission_name_from_message
                logging.debug(
                    f"Job {job_id}: Querying API key for IZ '{iz_code}' with permission '{required_permission}', "
                    f"preferring read-only.")
                query = (
                    db_session.query(APIKey)
                    .join(InstitutionZone).filter(InstitutionZone.iz_code == iz_code)
                    .join(APIKey.permission_links).join(ApiKeyPermission.permission)
                    .filter(AlmaApiPermission.permission_name == required_permission)
                    .order_by(desc(ApiKeyPermission.is_read_only))
                )
                api_key_record = query.first()
                if api_key_record:
                    actual_api_key = api_key_record.api_key_value
                    perm_link = next((link for link in api_key_record.permission_links if
                                      link.permission.permission_name == required_permission), None)
                    read_only_status = perm_link.is_read_only if perm_link else "Unknown"
                    logging.info(f"Job {job_id}: Found API key ID {api_key_record.id}. Read-Only={read_only_status}")
                    if not actual_api_key:
                        raise ValueError(
                            f"API key value empty for IZ '{iz_code}' / Perm '{required_permission}'")
                else:
                    raise ValueError(f"No API key configured for IZ '{iz_code}' / Perm '{required_permission}'")
        except Exception as db_err:
            logging.exception(f"Job {job_id}: Failed to retrieve API key: {db_err}")
            raise db_err

        # --- Instantiate Alma Client ---
        alma_client = AlmaClient(api_key=actual_api_key)

        # --- Fetch Data Chunk ---
        logging.debug(f"Job {job_id}: Calling AlmaClient.get_analytics_report_chunk")
        rows, next_token, is_finished, map_found_this_call = alma_client.get_analytics_report_chunk(
            report_path=report_path, resumption_token=resumption_token, limit=limit, header_map=header_map_from_message
        )
        logging.info(
            f"Job {job_id}: Fetched {len(rows)} rows. Finished: {is_finished}. NextToken: {next_token is not None}. "
            f"MapFound: {map_found_this_call is not None}")
        map_for_next_step = map_found_this_call if map_found_this_call is not None else header_map_from_message
        logging.debug(f"DEBUG Fetcher Job {job_id}: map_for_next_step = {map_for_next_step}")

        # --- Prepare Blob Path & Save Data ---
        report_name_for_path = message_body.get("report_path", "unknown_report_token_only")
        report_name_safe = data_utils.create_safe_filename(report_name_for_path)
        blob_path = f"{job_id}/{report_name_safe}/chunk_{chunk_index}.json"
        logging.info(f"Job {job_id}: Saving chunk data to blob: {CHUNK_CONTAINER_NAME}/{blob_path}")
        try:
            rows_json_string = data_utils.serialize_data(rows)
            storage_helpers.upload_blob_data(container_name=CHUNK_CONTAINER_NAME, blob_name=blob_path,
                                             data=rows_json_string)
            logging.debug(f"Job {job_id}: Successfully uploaded blob data.")
        except Exception as upload_err:
            logging.error(f"Job {job_id}: Failed to upload blob {blob_path}: {upload_err}", exc_info=True)
            raise upload_err

        # --- Queue Next Step ---
        base_context = {
            "job_id": job_id,
            "trigger_config_id": trigger_config_id,
            "iz_analysis_connector_id": iz_analysis_connector_id,
            "report_path": message_body.get("report_path"),
            "iz_code": iz_code,
            "alma_api_permission": permission_name_from_message,
            "header_map": map_for_next_step,
            "analysis_id": message_body.get("analysis_id"),  # Pass if present
            "analysis_name": message_body.get("analysis_name")  # Pass if present
        }

        if not is_finished and next_token:
            next_chunk_message = base_context.copy()
            next_chunk_message.update({
                "resumption_token": next_token,
                "chunk_index": chunk_index + 1,
                "limit": limit
            })
            logging.info(f"Job {job_id}: Queuing next chunk fetch (Index: {chunk_index + 1}).")
            outputPaginationQueue.set(data_utils.serialize_data(next_chunk_message))
        else:
            combine_message = base_context.copy()
            combine_message.update({
                "report_name_safe": report_name_safe,
                "total_chunks": chunk_index + 1,
                "container_name": CHUNK_CONTAINER_NAME
            })
            logging.info(f"Job {job_id}: Report fetch finished. Triggering combiner. Chunks: {chunk_index + 1}")
            outputCombineQueue.set(data_utils.serialize_data(combine_message))

        logging.info(f"Successfully processed fetcher message for Job ID: {job_id}, Chunk Index: {chunk_index}")

    # --- Error Handling ---
    except Exception as e:
        logging.exception(f"Error processing fetcher message for Job ID {job_id}: {e}")
        raise e
