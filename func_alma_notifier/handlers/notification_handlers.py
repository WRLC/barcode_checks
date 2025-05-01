"""Notifier function to send email notifications via Azure Communication Services (ACS) Email."""

import logging
import json
import azure.functions as func
import pandas as pd
import io
import pathlib
from typing import List, Optional
from jinja2 import Environment, FileSystemLoader, select_autoescape, TemplateNotFound
from azure.communication.email import EmailClient
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import HttpResponseError, ServiceRequestError
from shared_code.utils import storage_helpers, data_utils, config_helpers
from shared_code.database import session_scope, TimerTriggerConfig, TriggerConfigUserLink, User

bp = func.Blueprint()

# --- Constants ---
STORAGE_CONNECTION_STRING_NAME = "AzureWebJobsStorage"
INPUT_QUEUE_NAME = "notifierqueue"

# ACS Config
ACS_CONNECTION_STRING = config_helpers.get_optional_env_var("COMMUNICATION_SERVICES_CONNECTION_STRING")
ACS_ENDPOINT = config_helpers.get_optional_env_var("COMMUNICATION_SERVICES_ENDPOINT")
SENDER_ADDRESS = config_helpers.get_required_env_var("SENDER_ADDRESS")
TEMPLATE_FILE_NAME = "email_template.html.j2"

# --- Jinja2 Environment Setup ---
# Determine template directory path relative to this file
# Assumes /templates directory is at the same level as the /handlers directory
template_dir = pathlib.Path(__file__).parent.parent / "templates"
jinja_env: Optional[Environment] = None
# noinspection PyBroadException
try:
    if not template_dir.is_dir():
        logging.error(f"Jinja template directory not found at: {template_dir}")
        raise FileNotFoundError(f"Jinja template directory not found: {template_dir}")
    else:
        jinja_env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
        logging.info(f"Jinja2 environment loaded successfully from: {template_dir}")
except Exception as e:
    logging.exception(f"Failed to initialize Jinja2 environment from {template_dir}")


# --- Function Definition ---
@bp.queue_trigger(queue_name=INPUT_QUEUE_NAME, connection=STORAGE_CONNECTION_STRING_NAME, arg_name="msg")
def report_notifier(msg: func.QueueMessage) -> None:
    """
    Sends report notification via ACS Email using Jinja2 template.
    Retrieves recipients and subject from DB using trigger_config_id from message.
    Formats data from blob CSV into HTML table.

    Expected input message format (JSON):
    {
        "job_id": "...",
        "trigger_config_id": number, // REQUIRED ID for config lookup
        "iz_analysis_connector_id": number, // Context
        "combined_data_container": "...",
        "combined_data_blob": "...", // Path to CSV
        "status": "Combined",
        // email_subject and target_emails are fetched from DB using trigger_config_id
        // Optional context passed through: iz_code, analysis_id, analysis_name, header_map, original_report_path
    }
    """
    job_id: Optional[str] = None
    trigger_config_id: Optional[int] = None

    try:
        if not jinja_env:
            raise RuntimeError("Jinja2 environment failed to initialize. Cannot format email.")
        if not ACS_CONNECTION_STRING and not ACS_ENDPOINT:
            raise ValueError("COMMUNICATION_SERVICES_CONNECTION_STRING or ENDPOINT required.")
        if not SENDER_ADDRESS:
            raise ValueError("SENDER_ADDRESS required.")

        logging.info(f"Notifier received message ID: {msg.id}, DequeueCount: {msg.dequeue_count}")
        message_body = data_utils.deserialize_data(msg.get_body())
        if not isinstance(message_body, dict):
            raise ValueError("Msg body not dict.")

        # --- Get parameters ---
        job_id = message_body.get("job_id")
        trigger_config_id = message_body.get("trigger_config_id")
        combined_container = message_body.get("combined_data_container")
        combined_blob = message_body.get("combined_data_blob")
        analysis_name = message_body.get("analysis_name", "Analysis Report")
        iz_code = message_body.get("iz_code")
        original_report_path = message_body.get("original_report_path")

        # --- Validate necessary IDs and blob info ---
        if not all([job_id, trigger_config_id, combined_container, combined_blob]):
            raise ValueError("Missing required fields: job_id, trigger_config_id, container, blob")

        logging.info(f"Notifier started for Job ID: {job_id}, TriggerCfg: {trigger_config_id}")

        # --- Get Recipients and Subject from DB ---
        target_emails: List[str] = []
        email_subject: str = f"Report Ready: {analysis_name}"

        try:
            with session_scope() as db_session:
                logging.debug(f"Job {job_id}: Querying recipients and subject for TriggerConfig ID {trigger_config_id}")

                config: Optional[TimerTriggerConfig] = db_session.get(TimerTriggerConfig, trigger_config_id)

                if not config:
                    logging.error(f"Job {job_id}: Could not find TimerTriggerConfig ID {trigger_config_id} in DB.")
                    raise ValueError(f"TimerTriggerConfig not found for ID {trigger_config_id}")

                if config.email_subject:
                    email_subject = config.email_subject
                    logging.info(f"Job {job_id}: Using subject from DB: '{email_subject}'")
                else:
                    logging.warning(
                        f"Job {job_id}: email_subject is empty for TriggerConfig ID {trigger_config_id}. "
                        f"Using default."
                    )

                users_query = (
                    db_session.query(User.user_email)
                    .join(TriggerConfigUserLink, TriggerConfigUserLink.user_id == User.id)
                    .filter(TriggerConfigUserLink.trigger_config_id == trigger_config_id)
                )
                recipient_emails = [email for email, in users_query.all() if email]

                if not recipient_emails:
                    logging.warning(
                        f"Job {job_id}: No recipients found linked to TriggerConfig ID {trigger_config_id}. "
                        f"No email will be sent.")
                    return

                logging.info(
                    f"Job {job_id}: Found {len(recipient_emails)} recipient(s) for TriggerConfig ID "
                    f"{trigger_config_id}."
                )
                target_emails = recipient_emails

        except Exception as db_err:
            logging.exception(f"Job {job_id}: Failed DB lookup for TriggerConfig ID {trigger_config_id}: {db_err}")
            raise db_err

        # --- Download Combined Data JSON ---
        # noinspection PyUnusedLocal
        json_data_string: Optional[str] = None
        try:
            logging.debug(f"Job {job_id}: Downloading combined data from {combined_container}/{combined_blob}")
            json_data_string = storage_helpers.download_blob_as_text(combined_container, combined_blob)
            logging.info(f"Job {job_id}: Successfully downloaded combined data JSON.")
        except Exception as download_err:
            logging.error(
                f"Job {job_id}: Failed JSON download from {combined_container}/{combined_blob}: {download_err}",
                exc_info=True)
            raise download_err

        # --- Convert JSON to HTML Table ---
        html_table = "Error generating table from data."
        record_count = 0
        try:
            if json_data_string:
                json_io = io.StringIO(json_data_string)
                df = pd.read_json(json_io, orient='records')  # Adjust 'orient' if needed

                # Check if column '0' exists and all its values are '0' (as string or int)
                if '0' in df.columns and df['0'].astype(str).eq('0').all():
                    logging.debug(f"Job {job_id}: Column '0' contains only '0' values. Dropping column.")
                    df.drop('0', axis=1, inplace=True)

                record_count = len(df)
                logging.debug(f"Job {job_id}: Read {record_count} rows into DataFrame.")
                if not df.empty:
                    html_table = df.to_html(
                        index=False, border=1, escape=True, na_rep=''
                    ).replace(
                        'border="1"',
                        'border="1" style="border-collapse: collapse; border: 1px solid black;"'
                    )
                    logging.debug(f"Job {job_id}: Converted DataFrame to HTML string.")
                else:
                    html_table = (
                        f"<i>Report '{analysis_name}' generated, but contained no displayable data.</i><br>"
                        f"(Report Path: {original_report_path or 'N/A'})"
                    )
            else:
                logging.warning(f"Job {job_id}: No JSON data string available for conversion.")
                return
        except Exception as convert_err:
            logging.error(f"Job {job_id}: Failed JSON->HTML conversion: {convert_err}", exc_info=True)

        # --- Render HTML Email Body using Jinja2 ---
        # noinspection PyUnusedLocal
        html_content_body = "Error rendering email template."
        try:
            template = jinja_env.get_template(TEMPLATE_FILE_NAME)
            template_context = {
                "email_caption": email_subject,
                "analysis_name": analysis_name,
                "data_table_html": html_table,
                "record_count": record_count,
                "job_id": job_id,
                "iz_code": iz_code,
                "original_report_path": original_report_path
            }
            html_content_body = template.render(template_context)
            logging.debug(f"Job {job_id}: Successfully rendered Jinja2 email template.")
        except TemplateNotFound:
            logging.error(f"Job {job_id}: Email template '{TEMPLATE_FILE_NAME}' not found in {template_dir}.")
            html_content_body = (
                f"<p>Report {analysis_name} is ready. Data table could not be formatted (template not "
                f"found).</p><p>Job ID: {job_id}</p>"
            )
        except Exception as template_err:
            logging.exception(f"Job {job_id}: Failed to render Jinja2 template: {template_err}")
            html_content_body = (
                f"<p>Report {analysis_name} is ready. Data table formatting failed.</p><p>Job ID: {job_id}</p>"
            )

        # --- Send Email using ACS ---
        logging.debug(f"Job {job_id}: Preparing to send email via ACS.")
        try:
            email_client: EmailClient
            if ACS_CONNECTION_STRING:
                logging.debug("Using ACS Connection String.")
                email_client = EmailClient.from_connection_string(ACS_CONNECTION_STRING)
            else:
                logging.debug("Using ACS Endpoint and DefaultAzureCredential.")
                credential = DefaultAzureCredential()
                # noinspection PyTypeChecker
                email_client = EmailClient(endpoint=ACS_ENDPOINT, credential=credential)

            recipient_list_for_acs = [{"address": email} for email in target_emails]

            message = {
                "senderAddress": SENDER_ADDRESS,
                "recipients": {
                    "to": recipient_list_for_acs
                },
                "content": {
                    "subject": email_subject,
                    "html": html_content_body
                }
            }

            logging.info(f"Job {job_id}: Sending email to {len(recipient_list_for_acs)} recipients via ACS.")
            poller = email_client.begin_send(message)
            send_result = poller.result()
            logging.info(f"Job {job_id}: ACS send poller finished.")

            status = send_result.get('status') if isinstance(send_result, dict) else None
            message_id = send_result.get('id') if isinstance(send_result, dict) else None

            if status and status.lower() == "succeeded":
                logging.info(f"Job {job_id}: Successfully sent email via ACS. Message ID: {message_id}")
            else:
                error_details = send_result.get('error', {}) if isinstance(send_result, dict) else send_result
                logging.error(
                    f"Job {job_id}: ACS Email send finished with status: {status}. Message ID: {message_id}. Details: "
                    f"{error_details}"
                )
                raise Exception(f"ACS Email send failed with status {status}")

        except (HttpResponseError, ServiceRequestError) as acs_sdk_err:
            logging.exception(f"Job {job_id}: Azure SDK Error sending email via ACS: {acs_sdk_err}")
            raise acs_sdk_err
        except Exception as email_err:
            logging.exception(f"Job {job_id}: Failed to send email via ACS: {email_err}")
            raise email_err

        logging.info(f"Notifier finished successfully for Job ID: {job_id}")

    # --- Error Handling ---
    except (ValueError, TypeError, json.JSONDecodeError, data_utils.DataSerializationError, KeyError,
            RuntimeError) as config_or_data_err:
        # Includes DB lookup failures (ValueError), bad messages, Jinja init failure etc.
        logging.error(
            f"Configuration/Data/Runtime Error processing notify message for Job ID {job_id}, TriggerCfg "
            f"{trigger_config_id}: {config_or_data_err}",
            exc_info=True
        )
        # Let bad messages / config issues dead-letter without endless retries
        pass
    except Exception as err:
        # Includes DB connection errors from session_scope or query execution, ACS send errors if re-raised
        logging.exception(
            f"An unexpected error occurred in notifier for Job ID {job_id}, TriggerCfg {trigger_config_id}: {err}")
        # Re-raise unknown errors / transient errors to allow host retries
        raise err
