"""Combine report chunks and notify the notifier queue."""

import logging
import json
import azure.functions as func
import pandas as pd
import io
from typing import Union, Dict, List, Optional
from shared_code.utils import storage_helpers, data_utils, config_helpers
from shared_code.database import session_scope, TimerTriggerConfig

bp = func.Blueprint()

# --- Constants ---
STORAGE_CONNECTION_STRING_NAME = "AzureWebJobsStorage"
INPUT_QUEUE_NAME = "datacombinequeue"
COMBINED_CONTAINER_NAME = config_helpers.get_optional_env_var(
    "COMBINED_CONTAINER_NAME", "alma-combined-reports"
)


# --- Function Definition ---
@bp.queue_trigger(queue_name=INPUT_QUEUE_NAME, connection=STORAGE_CONNECTION_STRING_NAME, arg_name="msg")
def report_chunk_combiner(msg: func.QueueMessage) -> None:
    """
    Combines report chunks based on input message containing IDs.
    Looks up dataprep handler type from DB using trigger_config_id.
    Saves combined CSV and sends message to the appropriate next queue.

    Expected input message format (JSON):
    {
        "job_id": "...",
        "trigger_config_id": number, // ID of TimerTriggerConfig
        "iz_analysis_connector_id": number, // ID of specific IZ/Analysis/Path link
        "report_name_safe": "...",
        "total_chunks": number,
        "container_name": "alma-report-chunks",
        "header_map": {...} | null,
        // Optional context passed through:
        "iz_code": "...", "alma_api_permission": "...", "report_path": "...",
        "analysis_id": number|null, "analysis_name": "..."|null
    }
    """
    job_id = None
    trigger_config_id = None
    try:
        logging.info(f"Combiner received message ID: {msg.id}, DequeueCount: {msg.dequeue_count}")
        message_body = data_utils.deserialize_data(msg.get_body())
        if not isinstance(message_body, dict):
            raise ValueError("Msg body not dict.")

        # --- Get parameters ---
        job_id = message_body.get("job_id")
        trigger_config_id = message_body.get("trigger_config_id")
        iz_analysis_connector_id = message_body.get("iz_analysis_connector_id")
        report_name_safe = message_body.get("report_name_safe")
        total_chunks = message_body.get("total_chunks")
        chunk_container = message_body.get("container_name")
        header_map = message_body.get("header_map")
        iz_code = message_body.get("iz_code")
        analysis_id = message_body.get("analysis_id")
        analysis_name = message_body.get("analysis_name")
        # noinspection PyUnusedLocal
        original_report_path = message_body.get("report_path")
        # noinspection PyUnusedLocal
        permission_context = message_body.get("alma_api_permission")

        # --- Validate essential input ---
        if not all(
                [job_id, trigger_config_id, iz_analysis_connector_id, report_name_safe, total_chunks, chunk_container]):
            raise ValueError("Missing required fields in combiner message.")
        if not isinstance(total_chunks, int) or total_chunks <= 0:
            raise ValueError(f"Invalid total_chunks value: {total_chunks}")

        logging.info(
            f"Combiner started for Job ID: {job_id}, TriggerCfg: {trigger_config_id}, IZAnalysisConn: "
            f"{iz_analysis_connector_id}, Report: '{report_name_safe}', Chunks: {total_chunks}")

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

        # --- Download and Combine Chunks ---
        all_rows = []
        logging.debug(f"Job {job_id}: Reading chunks from container '{chunk_container}'")
        for i in range(total_chunks):
            chunk_blob_name = f"{job_id}/{report_name_safe}/chunk_{i}.json"
            try:
                chunk_data = storage_helpers.download_blob_as_json(chunk_container, chunk_blob_name)
                if isinstance(chunk_data, list):
                    all_rows.extend(chunk_data)
                else:
                    logging.warning(f"Job {job_id}: Chunk {i} did not contain a list.")
            except Exception as download_err:
                logging.error(f"Job {job_id}: Failed chunk {i} download: {download_err}", exc_info=True)
                raise ValueError(f"Failed chunk {i}") from download_err
        logging.info(f"Job {job_id}: Downloaded {total_chunks} chunks, total rows: {len(all_rows)}")

        # --- Convert to Pandas DataFrame and then CSV ---
        if not all_rows:
            logging.warning(f"Job {job_id}: No rows found. Skipping CSV/Notification.")
            return  # Exit successfully
        logging.debug(f"Job {job_id}: Creating DataFrame and converting to CSV.")
        try:
            df = pd.DataFrame(all_rows)
            if header_map:
                ordered_headings = [header_map.get(f"Column{i}") for i in range(len(header_map)) if
                                    header_map.get(f"Column{i}")]
                final_ordered_headings = [h for h in ordered_headings if h in df.columns]
                if final_ordered_headings:
                    df = df[final_ordered_headings]
                else:
                    logging.warning(f"Job {job_id}: Header map provided but no matching columns found.")
            else:
                logging.warning(f"Job {job_id}: No header map provided, CSV columns may be unordered.")
            csv_data = df.to_csv(index=False)
            logging.debug(f"Job {job_id}: CSV conversion successful.")
        except Exception as pd_err:
            logging.error(f"Job {job_id}: Pandas/CSV processing failed: {pd_err}", exc_info=True)
            raise ValueError(f"Pandas/CSV processing failed for job {job_id}") from pd_err

        # --- Upload Combined CSV to Blob Storage ---
        combined_blob_name = f"{job_id}/{report_name_safe}/combined_report.csv"
        logging.info(f"Job {job_id}: Uploading combined report to {COMBINED_CONTAINER_NAME}/{combined_blob_name}")
        try:
            storage_helpers.upload_blob_data(COMBINED_CONTAINER_NAME, combined_blob_name, csv_data)
            logging.debug(f"Job {job_id}: Successfully uploaded combined CSV.")
        except Exception as upload_err:
            logging.error(f"Job {job_id}: Failed to upload combined CSV: {upload_err}", exc_info=True)
            raise upload_err

        # --- Prepare and Send Message to Appropriate Next Queue ---
        next_message_payload = {
            "job_id": job_id,
            "trigger_config_id": trigger_config_id,
            "iz_analysis_connector_id": iz_analysis_connector_id,
            "combined_data_container": COMBINED_CONTAINER_NAME,
            "combined_data_blob": combined_blob_name,
            "status": "Combined",
            "email_subject": email_subject_from_config,
            "iz_code": iz_code,
            "analysis_id": analysis_id,
            "analysis_name": analysis_name,
            "header_map": header_map
        }

        # Determine target queue based on handler type
        # noinspection PyUnusedLocal
        target_queue_name: Optional[str] = None

        # -- SimpleEmailNotifier handler --
        if dataprep_handler_type == "SimpleEmailNotifier":
            target_queue_name = "notifierqueue"

        # Add elif blocks here for other handler types -> different queue names
        # elif dataprep_handler_type == "CrossReferenceAndUpdate":
        #     target_queue_name = "CrossReferenceQueue"

        else:
            logging.error(
                f"Job {job_id}: Unknown/unsupported dataprep_handler_type '{dataprep_handler_type}' "
                f"found for TriggerConfig ID {trigger_config_id}.")
            raise ValueError(f"Unsupported dataprep_handler_type: {dataprep_handler_type}")

        logging.info(
            f"Job {job_id}: Sending message for next step ({dataprep_handler_type}) to queue '{target_queue_name}'.")
        storage_helpers.send_queue_message(target_queue_name, next_message_payload)

        # --- Clean up source chunk files ---
        logging.info(f"Job {job_id}: Cleaning up source chunk files from container '{chunk_container}'.")
        deleted_count = 0
        failed_count = 0
        if isinstance(total_chunks, int) and total_chunks > 0:
            for i in range(total_chunks):
                chunk_blob_name_to_delete = f"{job_id}/{report_name_safe}/chunk_{i}.json"
                try:
                    storage_helpers.delete_blob(chunk_container, chunk_blob_name_to_delete)
                    logging.debug(f"Job {job_id}: Deleted chunk blob: {chunk_blob_name_to_delete}")
                    deleted_count += 1
                except Exception as delete_err:
                    logging.warning(
                        f"Job {job_id}: Failed to delete chunk blob {chunk_blob_name_to_delete}. "
                        f"Error: {delete_err}",
                        exc_info=False)
                    failed_count += 1
            logging.info(
                f"Job {job_id}: Chunk cleanup finished for report '{report_name_safe}'. Deleted: {deleted_count}, "
                f"Failed/Not Found: {failed_count}.")
        else:
            logging.warning(f"Job {job_id}: Invalid total_chunks value ({total_chunks}), skipping chunk cleanup.")

        logging.info(f"Combiner finished successfully for Job ID: {job_id}")

    # --- Error Handling ---
    except Exception as e:
        logging.exception(f"Error in combiner for Job ID {job_id} / TriggerCfg {trigger_config_id}: {e}")
        raise e
