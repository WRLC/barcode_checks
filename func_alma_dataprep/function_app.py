"""Scheduler triggers for barcode checks."""
import azure.functions as func
import logging

# Import the blueprint from the handlers module
from handlers import main_orchestrator_bp
from handlers.sub_orchestrators import single_report
from handlers.sub_orchestrators import multi_report
from handlers.activities import combine_chunks
from handlers.activities import merge_reports
from handlers.activities import send_notifications

# Define the main Function App instance
app = func.FunctionApp()

# Register all trigger blueprints
app.register_functions(main_orchestrator_bp.bp)
app.register_functions(single_report.bp)
app.register_functions(multi_report.bp)
app.register_functions(combine_chunks.bp)
app.register_functions(merge_reports.bp)
app.register_functions(send_notifications.bp)


# Optional health check
# noinspection PyUnusedLocal
@app.route(route="health", auth_level=func.AuthLevel.ANONYMOUS)
def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Health check for the scheduler function."""
    logging.info('Scheduler health check accessed.')
    return func.HttpResponse("Scheduler function is healthy.", status_code=200)
