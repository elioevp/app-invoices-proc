import azure.functions as func
import logging
import os
from datetime import datetime, date
import uuid
# --- CAMBIO CLAVE 1: Usar PyMySQL en lugar de mysql.connector ---
import pymysql

# --- IMPORTACIONES NECESARIAS PARA DOCUMENT INTELLIGENCE ---
from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.core.credentials import AzureKeyCredential
# --- FIN IMPORTACIONES NECESARIAS ---

# --- IMPORTACIONES NECESARIAS PARA COSMOS DB ---
from azure.cosmos import CosmosClient, PartitionKey
# --- FIN IMPORTACIONES NECESARIAS ---

blueprint = func.Blueprint()

# --- FUNCIÓN AUXILIAR PARA OBTENER USERNAME DE MYSQL ---
def get_username_from_db(user_id):
    """
    Obtiene el nombre de usuario desde la base de datos MySQL usando el user_id.
    """
    username = None
    try:
        # Obtener credenciales de las variables de entorno
        mysql_host = os.environ.get("MYSQL_HOST")
        mysql_user = os.environ.get("MYSQL_USER")
        mysql_password = os.environ.get("MYSQL_PASSWORD")
        mysql_database = os.environ.get("MYSQL_DATABASE")
        mysql_ssl_ca_path = os.environ.get("DB_SSL_CA_PATH") # Nueva línea para leer la ruta del certificado

        if not all([mysql_host, mysql_user, mysql_password, mysql_database]):
            logging.error("Missing MySQL environment variables.")
            return None

        # --- CAMBIO CLAVE 2: Usar pymysql.connect() con SSL ---
        # Conectar a la base de datos
        cnx = pymysql.connect(
            user=mysql_user,
            password=mysql_password,
            host=mysql_host,
            database=mysql_database,
            ssl={'ca': mysql_ssl_ca_path} if mysql_ssl_ca_path else True # Usar certificado si está definido
        )
        cursor = cnx.cursor()

        # Ejecutar consulta
        query = "SELECT username FROM users WHERE id = %s"
        cursor.execute(query, (user_id,))

        # Obtener resultado
        result = cursor.fetchone()
        if result:
            username = result[0]
            logging.info(f"Username '{username}' found for user ID '{user_id}'.")
        else:
            logging.warning(f"No username found for user ID '{user_id}'.")

    # --- CAMBIO CLAVE 3: Capturar el error específico de PyMySQL ---
    except pymysql.MySQLError as err:
        logging.error(f"Error connecting to MySQL or fetching data: {err}")
        return None
    
    finally:
        # --- CAMBIO CLAVE: Cambiar la forma de cerrar la conexión para PyMySQL ---
        # El bloque 'with' es la forma más robusta, pero podemos simplemente cerrarla si existe.
        if cnx is not None:
            # Los cursores en PyMySQL no necesitan ser cerrados por separado si se cierra la conexión
            # Pero cerrarlo no hace daño.
            if 'cursor' in locals() and cursor is not None:
                cursor.close()
            cnx.close()
            logging.info("MySQL connection closed.")
            
    return username

# --- FUNCIÓN AUXILIAR REVERTIDA A LA ORIGINAL (SIN CONFIDENCE ANIDADA) ---
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
        return field.value_date.strftime('%Y-%m-%d') # Formatear fecha
    if hasattr(field, 'value_time') and field.value_time is not None:
        return field.value_time.strftime('%H:%M:%S') # Formatear hora
    if hasattr(field, 'value_currency') and field.value_currency is not None:
        if hasattr(field.value_currency, 'amount') and field.value_currency.amount is not None:
            return field.value_currency.amount
    if hasattr(field, 'value') and field.value is not None:
        return field.value
    return None

# --- NUEVA FUNCIÓN AUXILIAR PARA OBTENER SÓLO LA CONFIANZA ---
def get_field_confidence(field):
    """
    Extrae la confianza de un DocumentField.
    """
    if field is None:
        return None
    return field.confidence if hasattr(field, 'confidence') else None

@blueprint.blob_trigger(arg_name="myblob", path="%BLOB_CONTAINER_NAME%/{id}/{random_subdirectory}/{name}",
                        connection="AzureWebJobsStorage")
def event_grid_blob_trigger(myblob: func.InputStream):
    logging.info("--- FUNCTION STARTED ---")

    # Ignorar archivos placeholder para evitar procesamiento innecesario
    if myblob.name.endswith('/.placeholder'):
        logging.info(f"Ignoring placeholder file: {myblob.name}")
        return

    user_id = None
    random_subdirectory = None
    try:
        blob_path_parts = myblob.name.split('/')
        if len(blob_path_parts) > 3:
            user_id = blob_path_parts[1]
            random_subdirectory = blob_path_parts[2]
        else:
            logging.error(f"Could not extract user ID and directory from blob path: {myblob.name}")
            return
    except Exception as e:
        logging.error(f"Error extracting user ID and directory from blob name: {e}")
        return

    logging.info(f"Processing blob for user ID: {user_id}. Directory: {random_subdirectory}. Blob name: {myblob.name}")

    # --- OBTENER USERNAME DE MYSQL ---
    # --- CAMBIO CLAVE 4: Inicializar 'username' para evitar el warning de Pylance ---
    username = None 
    username = get_username_from_db(user_id)
    if username is None:
        # Aquí puedes decidir si quieres detener la ejecución o continuar sin el username
        logging.warning(f"Proceeding without username for user ID: {user_id}")


    blob_url = myblob.uri
    logging.info(f"Processing blob from URL: {blob_url}")

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
        doc_intelligence_client = DocumentIntelligenceClient(
            endpoint=doc_int_endpoint,
            credential=AzureKeyCredential(doc_int_key),
            api_version="2024-02-29-preview"
        )

        blob_content = myblob.read()

        poller = doc_intelligence_client.begin_analyze_document(
            "TrainingHard1", blob_content, content_type="application/octet-stream"
        )
        receipt_result = poller.result()

        logging.info(f"Raw DI Result Documents: {receipt_result.documents}")

        if receipt_result.documents:
            for doc in receipt_result.documents:

                if "FechaTransaccion" in doc.fields:
                    fecha_transaccion = get_field_value(doc.fields["FechaTransaccion"])
                    full_receipt_data["fechaTransaccion"] = fecha_transaccion
                    logging.info(f"Extracted FechaTransaccion: {fecha_transaccion}")
                else:
                    logging.warning("FechaTransaccion field not found.")

                if "MontoTotal" in doc.fields:
                    monto_total = get_field_value(doc.fields["MontoTotal"])
                    full_receipt_data["montoTotal"] = monto_total
                    logging.info(f"Extracted MontoTotal: {monto_total}")
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
                        logging.info(f"Extracted Items: {extracted_items}")
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
            logging.info(f"Average Items Confidence Score: {full_receipt_data['itemsConfidenceScore']}")
        else:
            full_receipt_data["itemsConfidenceScore"] = None
            logging.warning("No item confidence scores were collected.")


    except Exception as e:
        logging.error(f"An error occurred during Document Intelligence processing for {myblob.name}: {e}")
        return

    # --- 2. Validación de datos extraídos ---
    if fecha_transaccion is None or monto_total is None:
        logging.warning(f"Could not extract all required data for blob: {myblob.name}. FechaTransaccion: {fecha_transaccion}, MontoTotal: {monto_total}. Data not saved to DB.")
        return

    # --- 3. Conexión y guardado en la base de datos Azure Cosmos DB ---
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
            "username": username, # NUEVO CAMPO
            "directorio": random_subdirectory, # NUEVO CAMPO
            "blobURL": blob_url,
            **full_receipt_data
        }

        # CAMBIO CLAVE: Se elimina el argumento 'partition_key' para compatibilidad
        container.create_item(body=final_cosmos_document)

        logging.info("Receipt data successfully saved to Cosmos DB with new fields.")
        logging.info(f"Document saved for user ID: {user_id}. Document ID: {receipt_document_id}")

    except Exception as e:
        logging.error(f"An unexpected error occurred during Cosmos DB operation: {e}")
    finally:
        logging.info("--- FUNCTION FINISHED ---")