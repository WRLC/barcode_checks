"""Scheduler triggers for barcode checks."""
import azure.functions as func
import logging

# Import the blueprint from the handlers module
from handlers import scf_duplicates_handler, scf_missing_incorrect_row_tray

# Define the main Function App instance
app = func.FunctionApp()

# Register all trigger blueprints
app.register_functions(scf_duplicates_handler.bp)
app.register_functions(scf_missing_incorrect_row_tray.bp)


# Optional health check
# noinspection PyUnusedLocal
@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check for the scheduler function."""
    logging.info('Scheduler health check accessed.')
    return func.HttpResponse("Scheduler function is healthy.", status_code=200)
