# func_alma_dataprep/function_app.py
import azure.functions as func
import logging

# Define the FunctionApp instance for this specific app
app = func.FunctionApp()
logging.info("FunctionApp instance created for func_alma_dataprep.")

# Import the handlers module to register the functions defined there
from handlers import combiner_handlers  # Assumes blueprint 'bp' is in this module

# Register the functions defined in the blueprint with the main app
app.register_blueprint(combiner_handlers.bp)
logging.info("Registered combiner blueprint.")


# Optional health check
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
    logging.info('Health check endpoint was triggered for dataprep.')
    return func.HttpResponse("func_alma_dataprep is healthy.", status_code=200)
