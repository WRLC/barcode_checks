"""Scheduler triggers for barcode checks."""
import azure.functions as func
import logging

# Import the blueprint from the handler file
from handlers import scf_duplicates_handler

# Define the main Function App instance
app = func.FunctionApp()  # Or ANONYMOUS

# Register the blueprint containing the timer trigger
app.register_functions(scf_duplicates_handler.bp)  # Assumes blueprint named 'bp'


# Optional health check
# noinspection PyUnusedLocal
@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check for the scheduler function."""
    logging.info('Scheduler health check accessed.')
    return func.HttpResponse("Scheduler function is healthy.", status_code=200)
