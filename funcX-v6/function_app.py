import azure.functions as func
import datetime
import json
import logging

app = func.FunctionApp()

from receipt_processor import blueprint as receipt_blueprint
app.register_blueprint(receipt_blueprint)