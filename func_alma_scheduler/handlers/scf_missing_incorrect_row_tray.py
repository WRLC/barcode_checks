"""Timer trigger to fetch barcodes with missing or incorrect row/tray data for SCF."""

import azure.functions as func
import logging
import datetime
import uuid
from .handler_helpers import _trigger_fetcher_job


# Create a Blueprint instance
bp = func.Blueprint()


# Timer for SCF Duplicates (Now just a wrapper)
@bp.timer_trigger(schedule="%SCF_MISSING_ROW_TRAY_SCHEDULE%",
                  arg_name="scfRowTrayTimer", run_on_startup=False,
                  use_monitor=False)
def scf_missing_row_tray_timer(scfRowTrayTimer: func.TimerRequest) -> None:
    """ Timer function wrapper for the 'scf_duplicates' configuration. """

    timer_config_name = "SCF Missing or Incorrect Row-Tray Data"

    if scfRowTrayTimer.past_due:
        logging.info(f'Timer function "{timer_config_name}" is past due!')

    logging.info(f'Python timer trigger function initiating job for "{timer_config_name}".')

    try:
        # Generate Job ID
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        job_id = f"timer_{timer_config_name}_{timestamp}_{unique_id}"  # Use passed name

        _trigger_fetcher_job(timer_config_name, job_id)
    except Exception as e:
        logging.error(f"Error during '{timer_config_name}' timer execution: {e}", exc_info=True)
        raise
