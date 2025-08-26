import azure.functions as func
import logging
import os
from datetime import datetime, date
import uuid
import json
import pymysql
import io
from PIL import Image
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
from azure.cosmos import CosmosClient, PartitionKey
from azure.storage.blob import BlobClient

blueprint = func.Blueprint()

def get_username_from_db(user_id):
    """
    Obtiene el nombre de usuario desde la base de datos MySQL usando el user_id.
    """
    username = None
    cnx = None # Initialize cnx to None
    try:
        # Get credentials from environment variables
        mysql_host = os.environ.get("MYSQL_HOST")
        mysql_user = os.environ.get("MYSQL_USER")
        mysql_password = os.environ.get("MYSQL_PASSWORD")
        mysql_database = os.environ.get("MYSQL_DATABASE")
        # Construct the absolute path to the certificate file relative to the script's location
        script_dir = os.path.dirname(__file__)
        default_ssl_ca_path = os.path.join(script_dir, 'cert_combined_ca.pem')

        # Use the environment variable if provided, otherwise use the default path
        mysql_ssl_ca_path = os.environ.get("DB_SSL_CA_PATH", default_ssl_ca_path)

        # Log the variables to check if they are loaded correctly
        logging.info(f"Attempting MySQL connection with Host: {mysql_host}, User: {mysql_user}, DB: {mysql_database}, SSL Path: {mysql_ssl_ca_path}")

        if not all([mysql_host, mysql_user, mysql_password, mysql_database]):
            logging.error("Missing one or more required MySQL environment variables.")
            return None

        # More specific SSL handling for debugging
        ssl_options = {} # Default to an empty dictionary for SSL
        if mysql_ssl_ca_path and os.path.exists(mysql_ssl_ca_path):
            logging.info(f"SSL CA path found at: {mysql_ssl_ca_path}")
            ssl_options = {'ca': mysql_ssl_ca_path}
        elif mysql_ssl_ca_path:
            logging.error(f"SSL CA path specified but file not found at: {mysql_ssl_ca_path}")
            # If a path is specified but not found, we might not want to connect at all.
            # Depending on security requirements, you might return None here.
            # For now, we'll log the error and attempt connection without the cert.
            pass
        else:
            logging.info("No SSL CA path provided or found, proceeding with default SSL.")

        # Connect to the database
        logging.info("Connecting to MySQL...")
        cnx = pymysql.connect(
            user=mysql_user,
            password=mysql_password,
            host=mysql_host,
            database=mysql_database,
            ssl=ssl_options,
            connect_timeout=10
        )
        logging.info("MySQL connection successful.")
        
        cursor = cnx.cursor()

        # Execute query
        query = "SELECT username FROM users WHERE id = %s"
        cursor.execute(query, (user_id,))

        # Get result
        result = cursor.fetchone()
        if result:
            username = result[0]
            logging.info(f"Username '{username}' found for user ID '{user_id}'.")
        else:
            logging.warning(f"No username found for user ID '{user_id}'.")

    except Exception as err: # Catch any exception
        logging.error(f"AN ERROR OCCURRED IN get_username_from_db: {err}", exc_info=True) # Log the full exception
        return None
    
    finally:
        if cnx and cnx.open:
            if 'cursor' in locals() and cursor:
                cursor.close()
            cnx.close()
            logging.info("MySQL connection closed.")
            
    return username

def get_field_value(field):
    """
    Extrae el valor de un DocumentField, manejando diferentes tipos.
    """
    if field is None:
        return None
    if hasattr(field, 'value_string') and field.value_string is not None:
        return field.value_string
    if hasattr(field, 'value_number') and field.value_number is not None:
        return field.value_number
    if hasattr(field, 'value_date') and field.value_date is not None:
        return field.value_date.strftime('%Y-%m-%d')
    if hasattr(field, 'value_time') and field.value_time is not None:
        return field.value_time.strftime('%H:%M:%S')
    if hasattr(field, 'value_currency') and field.value_currency is not None:
        if hasattr(field.value_currency, 'amount') and field.value_currency.amount is not None:
            return field.value_currency.amount
    if hasattr(field, 'value') and field.value is not None:
        return field.value
    return None

def get_field_confidence(field):
    """
    Extrae la confianza de un DocumentField.
    """
    if field is None:
        return None
    return field.confidence if hasattr(field, 'confidence') else None

@blueprint.event_grid_trigger(arg_name="event")
def func701(event: func.EventGridEvent):
    logging.info("--- FUNCTION STARTED ---")

    event_data = event.get_json()

    if event.event_type == "Microsoft.EventGrid.SubscriptionValidationEvent":
        validation_code = event_data.get('validationCode')
        logging.info(f"Got SubscriptionValidation event. Validation code: {validation_code}")
        return func.HttpResponse(json.dumps({'validationResponse': validation_code}), status_code=200)

    if event.event_type == "Microsoft.Storage.BlobCreated":
        blob_url = event_data.get('url')
        if not blob_url:
            logging.error("Blob URL not found in event data.")
            return

        logging.info(f"Processing blob from URL: {blob_url}")

        try:
            url_parts = blob_url.split('/')
            blob_path = "/".join(url_parts[4:])
            container_name = url_parts[3]
        except IndexError:
            logging.error(f"Could not parse blob URL: {blob_url}")
            return

        if blob_path.endswith('/.placeholder'):
            logging.info(f"Ignoring placeholder file: {blob_path}")
            return

        user_id = None
        random_subdirectory = None
        try:
            blob_path_parts = blob_path.split('/')
            if len(blob_path_parts) >= 3:
                user_id = blob_path_parts[0]
                random_subdirectory = blob_path_parts[1]
            else:
                logging.error(f"Could not extract user ID and directory from blob path: {blob_path}")
                return
        except Exception as e:
            logging.error(f"Error extracting user ID and directory from blob name: {e}")
            return

        logging.info(f"Processing blob for user ID: {user_id}. Directory: {random_subdirectory}. Blob name: {blob_path}")

        username = get_username_from_db(user_id)
        if username is None:
            logging.warning(f"Proceeding without username for user ID: {user_id}. This may indicate a DB connection issue.")

        doc_int_endpoint = os.environ.get("DI_ENDPOINT")
        doc_int_key = os.environ.get("DI_KEY")

        if not all([doc_int_endpoint, doc_int_key]):
            logging.error("Missing Document Intelligence endpoint or key environment variables.")
            return

        fecha_transaccion = None
        monto_total = None
        extracted_items = []
        full_receipt_data = {}
        item_confidences = []

        try:
            connection_string = os.environ["AzureWebJobsStorage"]
            blob_client = BlobClient.from_connection_string(connection_string, container_name=container_name, blob_name=blob_path)
            blob_content = blob_client.download_blob().readall()

            # --- PRE-PROCESAMIENTO DE IMAGEN (SOLO ESCALA DE GRISES) ---
            logging.info(f"Original image size: {len(blob_content) / 1024:.2f} KB")
            try:
                image_stream = io.BytesIO(blob_content)
                image = Image.open(image_stream)

                # Convert to grayscale
                image = image.convert('L')

                # Save optimized image to an in-memory stream
                output_stream = io.BytesIO()
                image.save(output_stream, format='JPEG')
                blob_content = output_stream.getvalue() # Update blob_content with optimized image
                logging.info(f"Optimized image size (grayscale only): {len(blob_content) / 1024:.2f} KB")

            except Exception as img_ex:
                logging.warning(f"Could not pre-process image, using original. Error: {img_ex}")
            # --- FIN PRE-PROCESAMIENTO ---

            doc_intelligence_client = DocumentIntelligenceClient(
                endpoint=doc_int_endpoint,
                credential=AzureKeyCredential(doc_int_key),
                api_version="2024-02-29-preview"
            )

            poller = doc_intelligence_client.begin_analyze_document(
                "TrainingHard1", blob_content, content_type="application/octet-stream"
            )
            receipt_result = poller.result()

            if receipt_result.documents:
                for doc in receipt_result.documents:
                    if "FechaTransaccion" in doc.fields:
                        fecha_transaccion = get_field_value(doc.fields["FechaTransaccion"])
                        full_receipt_data["fechaTransaccion"] = fecha_transaccion
                    else:
                        logging.warning("FechaTransaccion field not found.")

                    if "MontoTotal" in doc.fields:
                        monto_total = get_field_value(doc.fields["MontoTotal"])
                        full_receipt_data["montoTotal"] = monto_total
                    else:
                        logging.warning("MontoTotal field not found.")

                    if "Items" in doc.fields:
                        items_field = doc.fields["Items"]
                        if items_field.value_array and isinstance(items_field.value_array, list):
                            for item_doc_field in items_field.value_array:
                                if hasattr(item_doc_field, 'value_object') and item_doc_field.value_object:
                                    item_data = {}
                                    if "Description" in item_doc_field.value_object:
                                        desc_field = item_doc_field.value_object["Description"]
                                        item_data["description"] = get_field_value(desc_field)
                                        confidence = get_field_confidence(desc_field)
                                        if confidence is not None:
                                            item_confidences.append(confidence)
                                    if "Quantity" in item_doc_field.value_object:
                                        qty_field = item_doc_field.value_object["Quantity"]
                                        item_data["quantity"] = get_field_value(qty_field)
                                        confidence = get_field_confidence(qty_field)
                                        if confidence is not None:
                                            item_confidences.append(confidence)
                                    if "TotalPrice" in item_doc_field.value_object:
                                        price_field = item_doc_field.value_object["TotalPrice"]
                                        item_data["totalPrice"] = get_field_value(price_field)
                                        confidence = get_field_confidence(price_field)
                                        if confidence is not None:
                                            item_confidences.append(confidence)
                                    if "UnitPrice" in item_doc_field.value_object:
                                        unit_price_field = item_doc_field.value_object["UnitPrice"]
                                        item_data["unitPrice"] = get_field_value(unit_price_field)
                                        confidence = get_field_confidence(unit_price_field)
                                        if confidence is not None:
                                            item_confidences.append(confidence)
                                    if item_data:
                                        extracted_items.append(item_data)
                                else:
                                    logging.warning(f"Item DocumentField found but no value_object for item.")
                            full_receipt_data["items"] = extracted_items
                        else:
                            logging.warning("Items field found but its value_array is missing or not a list.")
                    else:
                        logging.warning("Items field not found.")

                    if "NombreComercio" in doc.fields:
                        full_receipt_data["nombreComercio"] = get_field_value(doc.fields["NombreComercio"])
                    if "RIF-comercio" in doc.fields:
                        full_receipt_data["rifComercio"] = get_field_value(doc.fields["RIF-comercio"])
                    if "FacturaNumero" in doc.fields:
                        full_receipt_data["facturaNumero"] = get_field_value(doc.fields["FacturaNumero"])
                    if "NombreRazon" in doc.fields:
                        full_receipt_data["nombreRazon"] = get_field_value(doc.fields["NombreRazon"])
                    if "RIF-CI" in doc.fields:
                        full_receipt_data["rifCI"] = get_field_value(doc.fields["RIF-CI"])
                    if "MontoExento" in doc.fields:
                        full_receipt_data["montoExento"] = get_field_value(doc.fields["MontoExento"])
                    if "MontoIVA" in doc.fields:
                        full_receipt_data["montoIVA"] = get_field_value(doc.fields["MontoIVA"])
                    if "BaseImponible" in doc.fields:
                        full_receipt_data["baseImponible"] = get_field_value(doc.fields["BaseImponible"])

                    break
            else:
                logging.warning("No documents found in receipt_result.")

            if item_confidences:
                average_item_confidence = sum(item_confidences) / len(item_confidences)
                full_receipt_data["itemsConfidenceScore"] = round(average_item_confidence, 4)
            else:
                full_receipt_data["itemsConfidenceScore"] = None
                logging.warning("No item confidence scores were collected.")

        except Exception as e:
            logging.error(f"An error occurred during Document Intelligence processing for {blob_path}: {e}", exc_info=True)
            return

        if fecha_transaccion is None or monto_total is None:
            logging.warning(f"Could not extract all required data for blob: {blob_path}. FechaTransaccion: {fecha_transaccion}, MontoTotal: {monto_total}. Data not saved to DB.")
            return

        cosmos_endpoint = os.environ.get("COSMOS_ENDPOINT")
        cosmos_key = os.environ.get("COSMOS_KEY")
        cosmos_database_name = os.environ.get("COSMOS_DATABASE_NAME")
        cosmos_container_name = os.environ.get("COSMOS_CONTAINER_NAME")

        if not all([cosmos_endpoint, cosmos_key, cosmos_database_name, cosmos_container_name]):
            logging.error("Missing one or more Cosmos DB connection environment variables.")
            return

        try:
            client = CosmosClient(cosmos_endpoint, credential=cosmos_key)
            database = client.get_database_client(cosmos_database_name)
            container = database.get_container_client(cosmos_container_name)

            receipt_document_id = str(uuid.uuid4())

            final_cosmos_document = {
                "id": receipt_document_id,
                "userId": user_id,
                "id_usuario": user_id,
                "username": username,
                "directorio": random_subdirectory,
                "blobURL": blob_url,
                **full_receipt_data
            }

            container.create_item(body=final_cosmos_document)

            logging.info("Receipt data successfully saved to Cosmos DB with new fields.")
            logging.info(f"Document saved for user ID: {user_id}. Document ID: {receipt_document_id}")

        except Exception as e:
            logging.error(f"An unexpected error occurred during Cosmos DB operation: {e}", exc_info=True)
        finally:
            logging.info("--- FUNCTION FINISHED ---")