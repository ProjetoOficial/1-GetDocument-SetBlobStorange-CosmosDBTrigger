import os
import json
import logging
import azure.functions as func
import azure.storage.blob as blob

documentos_json = []
azure_web_jobs_storage = os.environ["AzureWebJobsStorage"]

app = func.FunctionApp()


@app.cosmos_db_trigger(arg_name="azcosmosdb", container_name="estudo",
                        database_name="casa", connection="conncosmosdb",create_lease_container_if_not_exists=True) 
def GetDocumentSetBlobStorange(azcosmosdb: func.DocumentList):
    logging.info('Python CosmosDB triggered.')
    
    logging.info(azcosmosdb[0])
    
    for documento in azcosmosdb:
        
        documento_dict = dict(documento)
        # Adicionar o dicionário à lista de documentos
        documentos_json.append(documento_dict)

    
    document_data = json.dumps(documentos_json, indent=4, ensure_ascii=False)
        
    # Create a new blob and upload the document data to it.
    blob_service_client = blob.BlobServiceClient.from_connection_string(conn_str=azure_web_jobs_storage)
    container_client = blob_service_client.get_container_client("entrada")
    blob_client = container_client.get_blob_client("my-blob.json")
    blob_client.upload_blob(document_data, overwrite=True)

    # Log a message informing that the document was sent to Blob Storage.
    logging.info("Document sent to Blob Storage.")
