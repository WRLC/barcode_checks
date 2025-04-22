"""Timer trigger to fetch duplicate barcodes from the Alma API for SCF."""

import azure.functions as func
import logging
import os
import json
import datetime
import uuid

# --- Import necessary components from shared_code ---
from shared_code.database import get_db_session
# *** Import the specific models needed from your models.py ***
from shared_code.database.models import (
    TimerTriggerConfig,
    TriggerConfigIZAnalysisLink,
    IZAnalysisConnector,
    InstitutionZone,
    Analysis
)
from sqlalchemy import select, join
from shared_code.utils import storage_helpers, config_helpers  # Assuming queue helper

# Create a Blueprint instance
bp = func.Blueprint()

# --- Constants ---
# Hardcoded name to look up in the timer_trigger_configs table
THIS_TIMER_CONFIG_DB_NAME = "SCF Duplicate Barcodes"
# Name of the queue that func-alma-apifetcher listens to
TARGET_QUEUE_NAME = "apifetchqueue"  # Confirm this is correct

# --- Hardcoded values for the apifetchqueue message ---
DEFAULT_ALMA_API_PERMISSION = "Analytics"
DEFAULT_LIMIT = 1000  # Default limit for this timer-initiated fetch
INITIAL_RESUMPTION_TOKEN = None
INITIAL_CHUNK_INDEX = 0
INITIAL_HEADER_MAP = None


# --- Use App Setting for the schedule ---
# In Azure Portal App Settings, define SCF_DUPLICATES_SCHEDULE="YOUR_CRON_STRING"
@bp.timer_trigger(schedule="%SCF_DUPLICATES_SCHEDULE%",
                  arg_name="scfDupTimer", run_on_startup=False,
                  use_monitor=False)
def scf_duplicates_timer(scfDupTimer: func.TimerRequest) -> None:
    """
    Reads 'scf_duplicates' config from DB, gets linked analysis details,
    and sends a message to apifetchqueue to trigger data fetching.
    """
    if scfDupTimer.past_due:
        logging.info(f'Timer function "{THIS_TIMER_CONFIG_DB_NAME}" is past due!')

    logging.info(f'Python timer trigger function running for "{THIS_TIMER_CONFIG_DB_NAME}".')

    db_session = None
    try:
        # 1. Get configuration and associated analysis details from database
        db_session = get_db_session()
        if db_session is None:
            raise ConnectionError("Failed to get database session.")

        # Fetch the specific config row and linked analysis details
        config_data = _get_linked_analysis_config(db_session, THIS_TIMER_CONFIG_DB_NAME)

        if not config_data:
            logging.error(
                f"No configuration or linked analysis found in DB for '{THIS_TIMER_CONFIG_DB_NAME}'. Aborting.")
            return  # Exit gracefully if config is missing

        # 2. Extract parameters for the FETCHER queue message from config_data dict
        trigger_config_id = config_data.get('trigger_config_id')
        iz_analysis_connector_id = config_data.get('iz_analysis_connector_id')
        report_path = config_data.get('report_path')
        iz_code = config_data.get('iz_code')
        analysis_id = config_data.get('analysis_id')
        analysis_name = config_data.get('analysis_name')

        # Validate essential fields pulled from DB (add others if needed)
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
                f"Missing required fields {missing_fields} from DB query for '{THIS_TIMER_CONFIG_DB_NAME}'. Aborting.")
            return

        # 3. Generate Job ID
        timestamp_aware = datetime.datetime.now(datetime.timezone.utc)  # Get tz-aware UTC timestamp
        timestamp_str = timestamp_aware.strftime("%Y%m%d%H%M%S")  # Format as string
        unique_id = str(uuid.uuid4())[:8]
        job_id = f"timer_{THIS_TIMER_CONFIG_DB_NAME}_{timestamp_str}_{unique_id}"

        # 4. Construct the FETCHER queue message payload (using hardcoded values)
        message_payload = {
            "job_id": job_id,
            "trigger_config_id": trigger_config_id,  # Pass this for downstream use
            "iz_analysis_connector_id": iz_analysis_connector_id,  # Pass this for downstream use
            "report_path": report_path,
            "iz_code": iz_code,
            "alma_api_permission": DEFAULT_ALMA_API_PERMISSION,  # Hardcoded
            "resumption_token": INITIAL_RESUMPTION_TOKEN,  # Hardcoded
            "chunk_index": INITIAL_CHUNK_INDEX,  # Hardcoded
            "limit": DEFAULT_LIMIT,  # Hardcoded
            "header_map": INITIAL_HEADER_MAP,  # Hardcoded
            "analysis_id": analysis_id,
            "analysis_name": analysis_name
            # Recipient info is handled downstream by the notifier
        }
        message_str = json.dumps(message_payload, ensure_ascii=False)

        # 5. Send message to the apifetchqueue
        logging.info(f"Sending message to queue '{TARGET_QUEUE_NAME}': {message_str}")
        storage_helpers.send_queue_message(TARGET_QUEUE_NAME, message_str)

        logging.info(f'Successfully queued fetch job for "{THIS_TIMER_CONFIG_DB_NAME}". Job ID: {job_id}')

    except Exception as e:
        logging.error(f"Error in timer function '{THIS_TIMER_CONFIG_DB_NAME}': {e}", exc_info=True)
        raise e
    finally:
        if db_session:
            db_session.close()
            logging.debug("Database session closed.")


def _get_linked_analysis_config(session, timer_name):
    """
    Fetches TimerTriggerConfig row AND its linked IzAnalysisConnector details,
    including related InstitutionZone and Analysis info.
    Uses the passed-in SQLAlchemy session.
    Returns a dictionary containing combined data, or None if not found/linked.
    """
    logging.info(f"Fetching linked analysis config from DB for: name='{timer_name}'")

    stmt = (
        select(
            TimerTriggerConfig.id.label('trigger_config_id'),
            IZAnalysisConnector.id.label('iz_analysis_connector_id'),
            IZAnalysisConnector.analysis_path.label('report_path'),
            InstitutionZone.iz_code.label('iz_code'),
            Analysis.id.label('analysis_id'),
            Analysis.analysis_name.label('analysis_name')
            # Add other columns here if needed
        )
        .select_from(TimerTriggerConfig)
        .join(TriggerConfigIZAnalysisLink, TimerTriggerConfig.id == TriggerConfigIZAnalysisLink.trigger_config_id)
        .join(IZAnalysisConnector, TriggerConfigIZAnalysisLink.iz_analysis_connector_id == IZAnalysisConnector.id)
        .join(InstitutionZone, IZAnalysisConnector.institution_zone_id == InstitutionZone.id)
        .join(Analysis, IZAnalysisConnector.analysis_id == Analysis.id)
        .where(TimerTriggerConfig.config_name == timer_name)
    )
    # <<< Use .mappings().first() to get a dictionary-like RowMapping or None >>>
    result_mapping = session.execute(stmt).mappings().first()

    if not result_mapping:
        logging.warning(f"No config row or linked analysis found for timer_name='{timer_name}'")
        return None

    logging.info(f"Found config data for '{timer_name}'")
    # <<< Convert the RowMapping to a standard dict before returning >>>
    return dict(result_mapping)
