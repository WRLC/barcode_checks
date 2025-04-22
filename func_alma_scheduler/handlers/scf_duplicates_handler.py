"""Timer trigger to fetch duplicate barcodes from the Alma API for SCF."""

import azure.functions as func
import logging
import os
import json
import datetime
import uuid
from .handler_helpers import _trigger_fetcher_job


# Create a Blueprint instance
bp = func.Blueprint()


# Timer for SCF Duplicates (Now just a wrapper)
@bp.timer_trigger(schedule="%SCF_DUPLICATES_SCHEDULE%",
                  arg_name="scfDupTimer", run_on_startup=False,
                  use_monitor=False)
def scf_duplicates_timer(scfDupTimer: func.TimerRequest) -> None:
    """ Timer function wrapper for the 'scf_duplicates' configuration. """

    timer_config_name = "SCF Duplicate Barcodes"

    if scfDupTimer.past_due:
        logging.info(f'Timer function "{timer_config_name}" is past due!')

    logging.info(f'Python timer trigger function initiating job for "{timer_config_name}".')

    try:
        _trigger_fetcher_job(timer_config_name)
    except Exception as e:
        logging.error(f"Error during '{timer_config_name}' timer execution: {e}", exc_info=True)
        raise
