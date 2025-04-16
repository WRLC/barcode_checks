"""Notifier function to send email notifications via Azure Communication Services (ACS) Email."""

import logging
import json
import azure.functions as func
import pandas as pd
import io
import pathlib
from typing import Union, Dict, List, Optional
from jinja2 import Environment, FileSystemLoader, select_autoescape
# noinspection PyProtectedMember
from azure.communication.email import EmailClient, EmailAddress
from azure.identity import DefaultAzureCredential
from azure.core.exceptions import HttpResponseError, ServiceRequestError
from shared_code.utils import storage_helpers, data_utils, config_helpers

bp = func.Blueprint()

# --- Constants ---
STORAGE_CONNECTION_STRING_NAME = "AzureWebJobsStorage"
INPUT_QUEUE_NAME = "NotifierQueue"
ACS_CONNECTION_STRING = config_helpers.get_optional_env_var("COMMUNICATION_SERVICES_CONNECTION_STRING")
ACS_ENDPOINT = config_helpers.get_optional_env_var("COMMUNICATION_SERVICES_ENDPOINT")
SENDER_ADDRESS = config_helpers.get_required_env_var("SENDER_ADDRESS")
TEMPLATE_FILE_NAME = "email_template.html.j2"

# --- Jinja2 Environment Setup ---
template_dir = pathlib.Path(__file__).parent.parent / "templates"
# noinspection PyBroadException
try:
    jinja_env = Environment(
        loader=FileSystemLoader(template_dir),
        autoescape=select_autoescape(['html', 'xml'])
    )
    logging.info(f"Jinja2 environment loaded successfully from: {template_dir}")
except Exception as e:
    logging.exception(f"Failed to initialize Jinja2 environment from {template_dir}")
    jinja_env = None


# --- Function Definition ---
@bp.queue_trigger(queue_name=INPUT_QUEUE_NAME, connection=STORAGE_CONNECTION_STRING_NAME, arg_name="msg")
def report_notifier(msg: func.QueueMessage) -> None:
    """
    Sends report notification via ACS Email using a Jinja2 template for HTML body.
    Retrieves recipients and subject from the message payload.

    Expected input message format (JSON):
    {
        "job_id": "...",
        "trigger_config_id": number, // Context
        "iz_analysis_connector_id": number, // Context
        "combined_data_container": "...",
        "combined_data_blob": "...", // Path to CSV
        "status": "Combined",
        "email_subject": "Subject Line / Caption", // REQUIRED
        "target_emails": ["user1@example.com", ...], // REQUIRED list of recipients
        // Optional context: iz_code, analysis_id, analysis_name, header_map, original_report_path
    }
    """
    job_id = None
    trigger_config_id = None

    try:
        # --- Validate ACS & Jinja Config ---
        if not jinja_env:
            raise RuntimeError("Jinja2 environment failed to initialize. Cannot format email.")
        if not ACS_CONNECTION_STRING and not ACS_ENDPOINT:
            raise ValueError("ACS Connection String or Endpoint required.")
        if not SENDER_ADDRESS:
            raise ValueError("SENDER_ADDRESS required.")

        logging.info(f"Notifier received message ID: {msg.id}, DequeueCount: {msg.dequeue_count}")
        message_body = data_utils.deserialize_data(msg.get_body())
        if not isinstance(message_body, dict): raise ValueError("Msg body not dict.")

        # --- Get parameters ---
        job_id = message_body.get("job_id")
        trigger_config_id = message_body.get("trigger_config_id")
        combined_container = message_body.get("combined_data_container")
        combined_blob = message_body.get("combined_data_blob")
        email_subject = message_body.get("email_subject")
        target_emails = message_body.get("target_emails")
        # noinspection PyUnusedLocal
        analysis_id = message_body.get("analysis_id")
        analysis_name = message_body.get("analysis_name", "Unknown Analysis")
        iz_code = message_body.get("iz_code")
        original_report_path = message_body.get("original_report_path")

        # --- Validate input ---
        if not all([job_id, trigger_config_id, combined_container, combined_blob, email_subject]):
            raise ValueError("Missing required fields: job_id, trigger_config_id, container, blob, email_subject")
        if not target_emails or not isinstance(target_emails, list) or len(target_emails) == 0:
            logging.warning(f"Job {job_id}: No target_emails provided. No email sent.")
            return

        logging.info(
            f"Notifier started for Job ID: {job_id}, TriggerCfg: {trigger_config_id}, Subject: '{email_subject}'")
        logging.info(f"Attempting to notify {len(target_emails)} recipient(s).")

        # --- Download Combined Data CSV ---
        # noinspection PyUnusedLocal
        csv_data_string: Optional[str] = None
        try:
            logging.debug(f"Job {job_id}: Downloading CSV from {combined_container}/{combined_blob}")
            csv_data_string = storage_helpers.download_blob_as_text(combined_container, combined_blob)
            logging.info(f"Job {job_id}: Successfully downloaded CSV.")
        except Exception as download_err:
            logging.error(f"Job {job_id}: Failed CSV download: {download_err}", exc_info=True)
            raise download_err

        # --- Convert CSV to HTML Table ---
        html_table = "Error generating table from data."
        record_count = 0
        try:
            if csv_data_string:
                df = pd.read_csv(io.StringIO(csv_data_string))
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
                    html_table = f"<i>Report '{analysis_name}' generated, but contained no data rows.</i>"
            else:
                html_table = "<i>Could not retrieve report data to display.</i>"
        except Exception as convert_err:
            logging.error(f"Job {job_id}: Failed CSV->HTML conversion: {convert_err}", exc_info=True)

        # --- Render HTML Email Body using Jinja2 ---
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
        except Exception as template_err:
            logging.exception(f"Job {job_id}: Failed to render Jinja2 template: {template_err}")

        # --- Send Email using ACS ---
        logging.debug(f"Job {job_id}: Preparing to send email via ACS.")
        try:
            email_client: EmailClient
            if ACS_CONNECTION_STRING:
                email_client = EmailClient.from_connection_string(ACS_CONNECTION_STRING)
            else:
                credential = DefaultAzureCredential()
                # noinspection PyTypeChecker
                email_client = EmailClient(endpoint=ACS_ENDPOINT, credential=credential)

            recipient_list_acs = [EmailAddress(email=email) for email in target_emails]
            message = {
                "senderAddress": SENDER_ADDRESS,
                "recipients": {"to": recipient_list_acs},
                "content": {
                    "subject": email_subject,
                    "html": html_content_body
                }
            }

            logging.info(f"Job {job_id}: Sending email to {len(recipient_list_acs)} recipients via ACS.")
            poller = email_client.begin_send(message)
            send_result = poller.result()

            status = send_result.get('status') if isinstance(send_result, dict) else None
            message_id = send_result.get('id') if isinstance(send_result, dict) else None
            if status and status.lower() == "succeeded":
                logging.info(f"Job {job_id}: Successfully sent email. Message ID: {message_id}")
            else:
                logging.error(
                    f"Job {job_id}: ACS Email send finished with status: {status}. Message ID: {message_id}. "
                    f"Result: {send_result}"
                )

        except Exception as email_err:
            logging.exception(f"Job {job_id}: Failed to send email via ACS: {email_err}")
            raise email_err

        logging.info(f"Notifier finished successfully for Job ID: {job_id}")

    # --- Error Handling ---
    except (ValueError, TypeError, json.JSONDecodeError, data_utils.DataSerializationError, KeyError,
            RuntimeError) as config_or_data_err:
        logging.error(
            f"Configuration/Data/Runtime Error processing notify message for Job ID {job_id}, "
            f"TriggerCfg {trigger_config_id}: {config_or_data_err}",
            exc_info=True)
        pass  # Let bad messages / config issues dead-letter
    except Exception as err:
        logging.exception(
            f"An unexpected error occurred in notifier for Job ID {job_id}, TriggerCfg {trigger_config_id}: {err}")
        raise err
