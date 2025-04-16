"""Azure Function App for the Alma API Fetcher."""

import azure.functions as func
import logging

# Set the level for the Python root logger.
# This allows DEBUG messages from any module using the root logger (like client.py)
# to be processed by Python's logging framework itself.
# These messages then reach the Azure Functions handler, which filters
# based on host.json ("Function": "Debug" allows them through).
logging.getLogger().setLevel(logging.DEBUG)
logging.info(f"Python root logger level set to: {logging.getLogger().level}")  # Confirm level

# Define the FunctionApp instance for this specific app
# App-level configuration like http_auth_level could be set here if needed
app = func.FunctionApp()

# Import the handlers module to register the functions defined there
# This assumes your function code is in './handlers/api_fetch_handlers.py'
from handlers.api_fetch_handlers import bp as api_fetch_handlers_blueprint
# Register the blueprint with the FunctionApp instance
# noinspection PyTypeChecker
app.register_blueprint(api_fetch_handlers_blueprint)

logging.info("FunctionApp instance created and handlers imported for func_alma_apifetcher.")


# You could add an optional HTTP trigger here for basic health checks
# noinspection PyUnusedLocal
@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check endpoint for the function app.

    This endpoint can be used to check if the function app is running and accessible.

    Args:
        req (func.HttpRequest): The HTTP request object.
    Returns:
        func.HttpResponse: A simple HTTP response indicating the health status.

    """
    logging.info('Health check endpoint was triggered for api_fetcher.')
    return func.HttpResponse("func_alma_apifetcher is healthy.", status_code=200)
