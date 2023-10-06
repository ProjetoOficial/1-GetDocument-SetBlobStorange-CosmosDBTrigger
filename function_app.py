'''
Criador: Sidnei Lima
Data: 06/10/2023
Objetivo: Criar um documento no BlobStorange a partir da criação ou alteração de um documento no Cosmos DB.
Descrição: Esse código é uma função Azure Functions que é acionada por alterações ou criação em um contêiner do Azure Cosmos DB e, 
em seguida, envia os documentos do Cosmos DB para um contêiner de armazenamento de blobs no Azure.
'''

#Importando os módulos necessários:
import os
import json
import logging
import azure.functions as func
import azure.storage.blob as blob

#Inicializando uma lista vazia documentos_json para armazenar documentos do Cosmos DB
documentos_json = []

#Obtendo a string de conexão para o armazenamento do Azure a partir das variáveis de ambiente
azure_web_jobs_storage = os.environ["AzureWebJobsStorage"]

#Inicializando um objeto FunctionApp chamado app:
app = func.FunctionApp()

#Definindo uma função de gatilho do Cosmos DB:  
@app.cosmos_db_trigger(arg_name="azcosmosdb", container_name="estudo",
                        database_name="casa", connection="conncosmosdb",create_lease_container_if_not_exists=True) 
def GetDocumentSetBlobStorange(azcosmosdb: func.DocumentList):
    
    #Iniciando o registro de mensagens de log:
    logging.info('Python CosmosDB triggered.')
    
    logging.info(azcosmosdb[0])
    
    #Iterando sobre os documentos do Cosmos DB e adicionando-os à lista documentos_json:
    for documento in azcosmosdb:
        
        documento_dict = dict(documento)
        # Adicionar o dicionário à lista de documentos
        documentos_json.append(documento_dict)

    #Convertendo a lista documentos_json em uma string JSON formatada:
    document_data = json.dumps(documentos_json, indent=4, ensure_ascii=False)
        
    #Criando um cliente de serviço de blob e enviando os dados do documento para um blob:
    blob_service_client = blob.BlobServiceClient.from_connection_string(conn_str=azure_web_jobs_storage)
    container_client = blob_service_client.get_container_client("entrada")
    blob_client = container_client.get_blob_client("my-blob.json")
    blob_client.upload_blob(document_data, overwrite=True)

    # Log a message informing that the document was sent to Blob Storage.
    logging.info("Document sent to Blob Storage.")
