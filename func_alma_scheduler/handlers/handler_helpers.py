"""Helper functions for timer triggers."""

import azure.functions as func
import logging
import os
import json
import datetime
import uuid
from shared_code.database import get_db_session
from shared_code.database.models import (
    TimerTriggerConfig, TriggerConfigIZAnalysisLink, IZAnalysisConnector,
    InstitutionZone, Analysis
)
from sqlalchemy import select, join
from shared_code.utils import storage_helpers, config_helpers

# --- Constants ---
# Queue name remains constant for this handler's purpose
TARGET_QUEUE_NAME = "apifetchqueue"

# --- Hardcoded values for the apifetchqueue message ---
# These apply to all timers using this helper logic
DEFAULT_ALMA_API_PERMISSION = "Analytics"
DEFAULT_LIMIT = 1000
INITIAL_RESUMPTION_TOKEN = None
INITIAL_CHUNK_INDEX = 0
INITIAL_HEADER_MAP = None


# --- Reusable Helper Function ---
def _trigger_fetcher_job(
        timer_config_name: str,
        job_id: str,
        report_sequence_number: int,
        total_reports_expected: int
) -> None:
    """
    Reads config for the given timer_config_name, gets linked analysis details,
    constructs a fetcher message using hardcoded defaults and DB data,
    and sends it to the target queue.
    """
    logging.info(f'Helper function: Triggering fetch job for "{timer_config_name}".')
    db_session = None
    try:
        # 1. Get DB session
        db_session = get_db_session()
        if db_session is None:
            raise ConnectionError("Helper function failed to get database session.")

        # 2. Fetch the specific config row and linked analysis details
        config_data = _get_linked_analysis_config(db_session, timer_config_name)  # Uses the helper below

        if not config_data:
            logging.error(
                f"Helper function: No configuration or linked analysis found in DB for '{timer_config_name}'. "
                f"Aborting job trigger."
            )
            # Returning gracefully here means the timer invocation succeeds but does nothing.
            # Consider raising an exception if missing config is a critical error.
            return

        # 3. Extract parameters (using .get for safety on the returned dict)
        trigger_config_id = config_data.get('trigger_config_id')
        iz_analysis_connector_id = config_data.get('iz_analysis_connector_id')
        report_path = config_data.get('report_path')
        iz_code = config_data.get('iz_code')
        analysis_id = config_data.get('analysis_id')
        analysis_name = config_data.get('analysis_name')

        # Validate essential fields pulled from DB
        required_fields = {
            "trigger_config_id": trigger_config_id,
            "iz_analysis_connector_id": iz_analysis_connector_id,
            "report_path": report_path,
            "iz_code": iz_code,
            "analysis_id": analysis_id,
            "analysis_name": analysis_name
        }
        missing_fields = [k for k, v in required_fields.items() if v is None]
        if missing_fields:
            logging.error(
                f"Helper function: Missing required fields {missing_fields} from DB query for '{timer_config_name}'. "
                f"Aborting job trigger.")
            return

        # 5. Construct the FETCHER queue message payload
        message_payload = {
            "job_id": job_id,
            "trigger_config_id": trigger_config_id,
            "iz_analysis_connector_id": iz_analysis_connector_id,
            "report_path": report_path,
            "iz_code": iz_code,
            "alma_api_permission": DEFAULT_ALMA_API_PERMISSION,  # Hardcoded
            "resumption_token": INITIAL_RESUMPTION_TOKEN,  # Hardcoded
            "chunk_index": INITIAL_CHUNK_INDEX,  # Hardcoded
            "limit": DEFAULT_LIMIT,  # Hardcoded
            "header_map": INITIAL_HEADER_MAP,  # Hardcoded
            "analysis_id": analysis_id,
            "analysis_name": analysis_name,
            "report_sequence_number": report_sequence_number,
            "total_reports_expected": total_reports_expected
        }
        message_str = json.dumps(message_payload, ensure_ascii=False)

        # 6. Send message to the apifetchqueue
        logging.info(
            f"Helper function: Sending message to queue '{TARGET_QUEUE_NAME}' for '{timer_config_name}': {message_str}")
        storage_helpers.send_queue_message(TARGET_QUEUE_NAME, message_str)

        logging.info(f'Helper function: Successfully queued fetch job for "{timer_config_name}". Job ID: {job_id}')

    # Let exceptions propagate up to the calling timer function
    finally:
        if db_session:
            db_session.close()
            logging.debug(f"Helper function: Database session closed for '{timer_config_name}'.")


# --- Database Query Helper Function (Remains the same as before) ---
def _get_linked_analysis_config(session, timer_name):
    """
    Fetches TimerTriggerConfig row AND its linked IzAnalysisConnector details.
    Returns a dictionary containing combined data, or None if not found/linked.
    """
    logging.info(f"DB Helper: Fetching linked analysis config for: name='{timer_name}'")

    stmt = (
        select(
            TimerTriggerConfig.id.label('trigger_config_id'),
            IZAnalysisConnector.id.label('iz_analysis_connector_id'),
            IZAnalysisConnector.analysis_path.label('report_path'),
            InstitutionZone.iz_code.label('iz_code'),
            Analysis.id.label('analysis_id'),
            Analysis.analysis_name.label('analysis_name')
        )
        .select_from(TimerTriggerConfig)
        .join(TriggerConfigIZAnalysisLink, TimerTriggerConfig.id == TriggerConfigIZAnalysisLink.trigger_config_id)
        .join(IZAnalysisConnector, TriggerConfigIZAnalysisLink.iz_analysis_connector_id == IZAnalysisConnector.id)
        .join(InstitutionZone, IZAnalysisConnector.institution_zone_id == InstitutionZone.id)
        .join(Analysis, IZAnalysisConnector.analysis_id == Analysis.id)
        .where(TimerTriggerConfig.config_name == timer_name)
    )
    result_mapping = session.execute(stmt).mappings().first()

    if not result_mapping:
        logging.warning(f"DB Helper: No config row or linked analysis found for timer_name='{timer_name}'")
        return None

    logging.info(f"DB Helper: Found config data for '{timer_name}'")
    return dict(result_mapping)
