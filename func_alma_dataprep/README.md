# Alma Data Preparation

This is a Python-based Azure Function App designed to handle data preparation tasks triggered by Alma API data.

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

Structure of message in `datacombinequeue` to trigger the data preparation function:

```json5
{
    "job_id": "fetch_job_20250416_164800",
    "trigger_config_id": 7,
    "iz_analysis_connector_id": 23,
    "report_path": "/shared/WRLC/Reports/Fetch Test Report",
    "report_name_safe": "shared_WRLC_Reports_Fetch_Test_Report",
    "total_chunks": 1,
    "container_name": "alma-report-chunks",
    "header_map": {
        "Column0": "ID",
        "Column1": "Title",
        "Column2": "Barcode",
        "Column3": "Internal Note 1",
        "Column4": "Item Call Number",
        "Column5": "Provenance Code"
    },
    "iz_code": "WRLC_IZ",
    "alma_api_permission": "Analytics",
    "analysis_id": 5,
    "analysis_name": "WRLC Fetch Test Analysis",
    "email_subject": "WRLC Fetch Test Report Results",
    "target_emails": [
        "testuser@wrlc.org",
        "admin@wrlc.org"
    ]
}
```

This assumes the `combined_data_container` and `combined_data_blob` are set by the `func_alma_apifetcher` function.

Processed data will be sent to a queue (e.g., `notifierqueue`) and blob container (e.g., `alma-combined-reports`) for further action by another function (e.g., `func_alma_notifier`).

The `status` field is used to determine if the report is ready for processing. The `header_map` field is optional and can be used to provide context for the report's columns.