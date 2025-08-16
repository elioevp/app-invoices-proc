import azure.functions as func
import datetime
import json
import logging

# Add logging to see if the file is being executed
logging.info("--- function_app.py: Starting up ---")

app = func.FunctionApp()
logging.info("--- function_app.py: FunctionApp created ---")


try:
    from receipt_processor import blueprint as receipt_blueprint
    app.register_blueprint(receipt_blueprint)
    logging.info("--- function_app.py: Blueprint registered successfully ---")
except Exception as e:
    logging.error(f"--- function_app.py: Error registering blueprint: {e} ---")

