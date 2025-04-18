# Alma API Fetcher

This is a Python-based Azure Function App designed to fetch data via the Alma API

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

Structure of message in `apifetchqueue` to trigger the Alma API fetcher:

```json5
{
    "job_id": "trig5-20250417114437",
    "trigger_config_id": 1,
    "iz_analysis_connector_id": 1,
    "report_path": "/shared/Shared storage institution/Reports/Barcode Check/SCF No Row Tray/No RowTray in SCF - part 1",
    "iz_code": "scf",
    "alma_api_permission": "Analytics",
    "resumption_token": null,
    "chunk_index": 0,
    "limit": 25,
    "header_map": null,
    "analysis_id": 1,
    "analysis_name": "No RowTray in SCF - part 1"
}
```

The retrieved data will be sent to the `datacombinequeue` and the `alma-report-chunks` blob container for further processing by the `func_alma_dataprep` function.

The `header_map` field is optional and can be used to provide context for the report's columns.

The `email_subject` and `target_emails` fields are used to send notifications via the `func_alma_notifier` function.