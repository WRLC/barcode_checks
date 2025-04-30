"""Helper functions for timer triggers."""

import logging
import json
from shared_code.database import get_db_session
from shared_code.database.models import (
    TimerTriggerConfig, TriggerConfigIZAnalysisLink, IZAnalysisConnector,
    InstitutionZone, Analysis
)
from sqlalchemy import select
from shared_code.utils import storage_helpers

# --- Constants ---
# Queue name remains constant for this handler's purpose
TARGET_QUEUE_NAME = "apifetchqueue"
DEFAULT_ALMA_API_PERMISSION = "Analytics"
INITIAL_RESUMPTION_TOKEN = None
INITIAL_CHUNK_INDEX = 0
DEFAULT_LIMIT = 1000
INITIAL_HEADER_MAP = None


# --- Database Query Helper Function (Updated) ---
def _get_linked_analysis_configs(session, timer_name):
    """
    Fetches TimerTriggerConfig row AND ALL its linked IzAnalysisConnector details.
    Returns a list of dictionaries containing combined data for each linked report,
    or an empty list if not found/linked.

    """
    logging.info(f"DB Helper: Fetching all linked analysis configs for: name='{timer_name}'")

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
        # Order by something predictable if sequence matters, e.g., IZAnalysisConnector.id
        .order_by(IZAnalysisConnector.id)
    )
    # Use .all() to fetch all matching linked reports
    result_mappings = session.execute(stmt).mappings().all()

    if not result_mappings:
        logging.warning(f"DB Helper: No config row or linked analyses found for timer_name='{timer_name}'")
        return []  # Return empty list

    logging.info(f"DB Helper: Found {len(result_mappings)} linked config(s) for '{timer_name}'")
    # Convert RowMappings to plain dictionaries
    return [dict(row) for row in result_mappings]


# --- Reusable Helper Function (Updated) ---
def _trigger_fetcher_job(
        timer_config_name: str,
        job_id: str
) -> None:
    """
    Reads config for the given timer_config_name, gets ALL linked analysis details,
    constructs a fetcher message for EACH linked report using hardcoded defaults
    and DB data, and sends each message to the target queue.

    """
    logging.info(f'Helper function: Triggering fetch job(s) for "{timer_config_name}". Job ID: {job_id}')
    db_session = None
    try:
        # 1. Get DB session
        db_session = get_db_session()
        if db_session is None:
            raise ConnectionError("Helper function failed to get database session.")

        # 2. Fetch all linked analysis configurations
        config_list = _get_linked_analysis_configs(db_session, timer_config_name)  # Use the updated helper

        if not config_list:
            logging.warning(  # Changed from error to warning as it might be valid
                f"Helper function: No configuration or linked analyses found in DB for '{timer_config_name}'. "
                f"No fetcher jobs will be triggered for Job ID: {job_id}."
            )
            return  # Nothing to do

        total_reports_expected = len(config_list)
        logging.info(f"Helper function: Found {total_reports_expected} report(s) to trigger for '{timer_config_name}'.")

        # 3. Iterate through each linked report config and send a message
        for seq_num, config_data in enumerate(config_list, start=1):

            # 4. Extract parameters for the current report
            trigger_config_id = config_data.get('trigger_config_id')
            iz_analysis_connector_id = config_data.get('iz_analysis_connector_id')
            report_path = config_data.get('report_path')
            iz_code = config_data.get('iz_code')
            analysis_id = config_data.get('analysis_id')
            analysis_name = config_data.get('analysis_name')

            # Validate essential fields pulled from DB for this specific report
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
                    f"Helper function: Missing required fields {missing_fields} from DB query for report "
                    f"{seq_num}/{total_reports_expected} (IZ Connector ID: {iz_analysis_connector_id}) "
                    f"linked to '{timer_config_name}'. Skipping this report trigger for Job ID: {job_id}."
                )
                continue  # Skip this report, proceed to the next

            # 5. Construct the FETCHER queue message payload for this report
            message_payload = {
                "job_id": job_id,
                "trigger_config_id": trigger_config_id,
                "iz_analysis_connector_id": iz_analysis_connector_id,
                "report_path": report_path,
                "iz_code": iz_code,
                "alma_api_permission": DEFAULT_ALMA_API_PERMISSION,
                "resumption_token": INITIAL_RESUMPTION_TOKEN,
                "chunk_index": INITIAL_CHUNK_INDEX,
                "limit": DEFAULT_LIMIT,
                "header_map": INITIAL_HEADER_MAP,
                "analysis_id": analysis_id,
                "analysis_name": analysis_name,
                "report_sequence_number": seq_num,  # Use the current sequence number
                "total_reports_expected": total_reports_expected  # Use the total count
            }
            message_str = json.dumps(message_payload, ensure_ascii=False)

            # 6. Send message to the apifetchqueue
            logging.info(
                f"Helper function: Sending message {seq_num}/{total_reports_expected} to queue '{TARGET_QUEUE_NAME}' "
                f"for '{timer_config_name}' (IZ Connector ID: {iz_analysis_connector_id}): {message_str}"
            )
            storage_helpers.send_queue_message(TARGET_QUEUE_NAME, message_str)

            logging.info(
                f"Helper function: Successfully queued fetch job {seq_num}/{total_reports_expected} for "
                f"'{timer_config_name}' (IZ Connector ID: {iz_analysis_connector_id}). Job ID: {job_id}"
            )

        logging.info(
            f"Helper function: Finished queuing all {total_reports_expected} fetch jobs for '{timer_config_name}'. "
            f"Job ID: {job_id}"
        )

    # Let exceptions propagate up to the calling timer function
    finally:
        if db_session:
            db_session.close()
            logging.debug(f"Helper function: Database session closed for '{timer_config_name}'. Job ID: {job_id}")
