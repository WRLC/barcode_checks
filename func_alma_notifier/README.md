# Alma Notifier

This is a Python-based Azure Function App designed to handle notifications triggered by Alma API data

## Shared Code

This function app uses shared code from the `shared_code` directory. This code is used to handle common tasks such as API requests, logging, and other utility functions.

The shared code is expected to be in a directory named `shared_code` at the same level as this function app's directory. The shared code should include modules like `alma_client`, `logging`, and any other necessary utilities.

To ensure the shared code is included in the deployment, run the following command from the root of this function app:

```bash
rsync -av --delete \
  ../shared_code/ ./shared_code/ \
  --exclude '.venv' \
  --exclude '__pycache__' \
  --exclude '*.pyc' \
  --exclude '.pytest_cache' \
  --exclude 'dist/' \
  --exclude '.git' \
  --exclude '.gitignore' \
  --exclude 'pyproject.toml' \
  --exclude 'poetry.lock' \
  --exclude 'tests/' \
  --exclude 'README.md'
```

## Trigger

Structure of message in `notifierqueue` to trigger an email notification:

```json5
{
    "job_id": "fetch_test_20250416_04",
    "trigger_config_id": 5,
    "iz_analysis_connector_id": 10,
    "combined_data_container": "alma-combined-reports",
    "combined_data_blob": "fetch_test_20250416_04/Your_Report_Safe_Name/combined_report.csv",
    "status": "Combined",
    "email_subject": "Daily Loan Report Results", // Required by Notifier
    "target_emails": ["user1@yourdomain.com", "manager@yourdomain.com"], // Required by Notifier
    "iz_code": "YOUR_IZ_CODE",
    "analysis_id": 42,
    "analysis_name": "Specific Report For Testing V3",
    "header_map": {"Column0": "Heading0", ...} // Optional context
}
```

This assumes the `combined_data_container` and `combined_data_blob` are set by the `func_alma_dataprep` function.

The `status` field is used to determine if the report is ready for notification. The `header_map` field is optional and can be used to provide context for the report's columns.