"""Durable Entity to track report metadata for a job."""

import logging
import azure.functions as func
import azure.durable_functions as df

# Assuming 'bp' is your function Blueprint, defined elsewhere (e.g., in the same file or imported)
# If not, you'll need to define it:
# bp = func.Blueprint()

# Create a Blueprint instance
bp = func.Blueprint()


def report_tracker_entity(context: df.DurableEntityContext):
    """
    Durable Entity function to track report metadata for a job.

    Operations:
        addReportMetadata: Adds metadata for a received report chunk/file.
                           Input: dict (report_metadata)
        getState: Returns the current list of collected report metadata.
                  Input: None
        reset: Clears the state of the entity.
               Input: None
    """
    current_value = context.get_state(lambda: {"reports": []})
    operation = context.operation_name
    logging.info(f"Entity '{context.entity_key}': Operation '{operation}' called.")

    if operation == "addReportMetadata":
        report_metadata = context.get_input()
        if isinstance(report_metadata, dict) and "sequence_number" in report_metadata:
            # Ensure no duplicates based on sequence number if needed, or just append
            # Simple append shown here:
            current_value["reports"].append(report_metadata)
            # Sort by sequence number for consistency (optional)
            current_value["reports"].sort(key=lambda x: x.get("sequence_number", float('inf')))
            context.set_state(current_value)
            logging.debug(f"Entity '{context.entity_key}': Added metadata for sequence {report_metadata.get('sequence_number')}")
        else:
            logging.warning(f"Entity '{context.entity_key}': Invalid input for addReportMetadata: {report_metadata}")

    elif operation == "getState":
        context.set_result(current_value)
        logging.debug(f"Entity '{context.entity_key}': Returning current state.")

    elif operation == "reset":
        context.set_state({"reports": []})
        logging.info(f"Entity '{context.entity_key}': State reset.")

    else:
        logging.error(f"Entity '{context.entity_key}': Unknown operation '{operation}'")


# Register the entity function with the blueprint
# The entityName MUST match the first argument used in df.EntityId()
@bp.entity_trigger(entity_name="ReportTrackerEntity", context_name="context")
def report_tracker_entity_trigger(context: df.DurableEntityContext):
    """Azure Function binding for the ReportTrackerEntity."""
    return report_tracker_entity(context)
