"""Azure Function App for the Alma Notifier."""

import azure.functions as func
import logging

# Optional: Set the level for the Python root logger if you want DEBUG messages
# from shared_code modules (like utils or client if used here) to appear
# when host.json's "Function" level is also set to "Debug".
# logging.getLogger().setLevel(logging.DEBUG)
# logging.info(f"Python root logger level set to: {logging.getLogger().level}") # Confirm

# Define the FunctionApp instance for this specific app
# You can configure app-level settings like HTTP auth here if needed
# e.g., app = func.FunctionApp(http_auth_level=func.AuthLevel.FUNCTION)
app = func.FunctionApp()

logging.info("FunctionApp instance created for func_alma_notifier.")

try:
    from handlers import notification_handlers
    # Register the functions defined in the blueprint with the main app
    app.register_blueprint(notification_handlers.bp)
    logging.info("Registered function blueprints from notifier_handlers.")
except ImportError as e:
    logging.error(
        f"Could not import or register blueprints from handlers. Ensure handlers/notifier_handlers.py exists and "
        f"defines 'bp'. Error: {e}",
        exc_info=True
    )


# Optional: Define a simple HTTP trigger for health checks directly on the app
# noinspection PyUnusedLocal
@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """
    Basic HTTP endpoint to check if the Function App is running.
    """
    logging.info('Health check endpoint was triggered for func_alma_notifier.')
    return func.HttpResponse(
        "func_alma_notifier is running.",
        status_code=200
    )
