import os
import json
import asyncio
import logging
import aiohttp
from aio_pika import connect_robust, IncomingMessage, Message
from google.cloud import storage
from dotenv import load_dotenv
from db.db import engine, Base

load_dotenv()

rabbitmq_user = os.getenv("RABBITMQ_USER") 
rabbitmq_pass = os.getenv("RABBITMQ_PASSWORD")
rabbitmq_host = os.getenv("RABBITMQ_HOST")
rabbitmq_port = os.getenv("RABBITMQ_PORT")
rabbitmq_queue_delete = os.getenv("RABBITMQ_QUEUE_DELETE")
rabbitmq_queue_auth = os.getenv("RABBITMQ_QUEUE_AUTHENTICATION")
rabbitmq_queue_notifications = os.getenv("RABBITMQ_QUEUE_NOTIFICATIONS")
rabbitmq_queue_transfer_delete_docs = os.getenv("RABBITMQ_QUEUE_TRANSFER_DELETE_DOCS")
rabbitmq_queue_transfer_delete_user = os.getenv("RABBITMQ_QUEUE_TRANSFER_DELETE_USER")
rabbitmq_queue_transfer_receive_docs = os.getenv("RABBITMQ_QUEUE_TRANSFER_RECEIVE_DOCS")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Delete file from bucket in GCP
async def delete_file_gcp(file_name: str):
    try:
        creds_path = os.getenv("GCP_SA_KEY")        
        bucket_name = os.getenv("GCP_BUCKET_NAME")
        gcs = storage.Client.from_service_account_json(creds_path)
    
        bucket = gcs.get_bucket(bucket_name)
        file_to_delete = bucket.get_blob(file_name)
        if file_to_delete:
            file_to_delete.delete()
            logger.info(f"Archivo eliminado: {file_name}")
            return True
        else: 
            logger.warning(f"Archivo no encontrado: {file_name}")
            return False

    except Exception as e:
        logger.error(f"Error al eliminar archivo: {e}")
    
# Send notification with action result
async def send_notification(action: str, file_name: str, client_email: str):
    try:
        message = {
            "action": action,
            "to_email": client_email,
            "file_name": file_name
        }

        connection = await connect_robust(
            host=rabbitmq_host,
            login=rabbitmq_user,
            password=rabbitmq_pass
        )
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(rabbitmq_queue_notifications, durable=True)
            message =  Message(body=json.dumps(message).encode())
            await channel.default_exchange.publish(message, routing_key=queue.name)

        logger.info(f"Mensaje enviado a {rabbitmq_queue_notifications}: {message}")
    except Exception as e:
        logger.error(f"Error al enviar notificacion: {e}")

# Update metadata for file in GCP
async def update_metadata_gcp(file_path: str):
    try:
        creds_path = os.getenv("GCP_SA_KEY")        
        bucket_name = os.getenv("GCP_BUCKET_NAME")
        gcs = storage.Client.from_service_account_json(creds_path)
    
        bucket = gcs.get_bucket(bucket_name)
        file_to_update = bucket.get_blob(file_path)

        if file_to_update:
            metadata = file_to_update.metadata
            metadata["firmado"] = "true"
            file_to_update.metadata = metadata
            file_to_update.patch()
            logger.info(f"Archivo actualizado exitosamente en bucket")
            return True
            
        else: 
            logger.warning(f"Archivo no encontrado: {file_path}")
            return False

    except Exception as e:
        logger.error(f"Error al actualizar autenticacion: {e}")

async def send_delete_user(id: int, req_status: int):
    try:
        message = {
            "id": id,
            "req_status": req_status
        }

        connection = await connect_robust(
            host=rabbitmq_host,
            login=rabbitmq_user,
            password=rabbitmq_pass
        )
        async with connection:
            channel = await connection.channel()
            queue = await channel.declare_queue(rabbitmq_queue_transfer_delete_user, durable=True)
            message =  Message(body=json.dumps(message).encode())
            await channel.default_exchange.publish(message, routing_key=queue.name)

        logger.info(f"Mensaje enviado a {rabbitmq_queue_transfer_delete_user}: {message}")

    except Exception as e:
        logger.error(f"Error al enviar mensaje: {e}")

async def transfer_delete_docs(id: int, req_status: int):
    try:
        if req_status == 1:
            creds_path = os.getenv("GCP_SA_KEY")        
            bucket_name = os.getenv("GCP_BUCKET_NAME")
            gcs = storage.Client.from_service_account_json(creds_path)
        
            prefix_folder = f"{id}/"
            folder = list(gcs.list_blobs(bucket_name, prefix_folder))
            for file in folder:
                file.delete()

            # Eliminar posible "objeto-carpeta"
            bucket = gcs.get_bucket(bucket_name)
            placeholder_blob = bucket.blob(f"{id}/")
            if placeholder_blob.exists():
                placeholder_blob.delete()

            # Eliminar de base de datos
            
            # Mandar a la cola de usuarios
            await send_delete_user(id, req_status)

        else: 
            return send_notification("transfer_error", client_email)
    except Exception as e:
        logger.error(f"Error al eliminar archivos: {e}")

# Receive docs of new tranfered user    
async def transfer_receive_docs(message: IncomingMessage):
    async with message.process():  # Ack automático
        try:
            payload = json.loads(message.body.decode())
            citizen_id = payload.get("id")
            citizen_name = payload.get("citizenName")
            citizen_email = payload.get("citizenEmail")
            url_documents = payload.get("urlDocuments", {})
            confirm_api = payload.get("confirmAPI")
            
            logger.info(f"Recibido solicitud de transferencia para ciudadano: {citizen_name} ({citizen_id})")
            
            # Configurar cliente de Storage
            creds_path = os.getenv("GCP_SA_KEY")        
            bucket_name = os.getenv("GCP_BUCKET_NAME")
            gcs = storage.Client.from_service_account_json(creds_path)
            bucket = gcs.get_bucket(bucket_name)
            
            # Procesar todos los documentos
            for url_key, urls in url_documents.items():
                for url in urls:
                    try:
                        # Descargar el archivo de la URL
                        async with aiohttp.ClientSession() as session:
                            async with session.get(url) as response:
                                if response.status != 200:
                                    
                                    logger.error(f"Error al descargar archivo: {url}, status: {response.status}")
                                    await send_confirmation(int(citizen_id), 0, citizen_name, confirm_api)
                                    continue
                                
                                # Obtener el nombre del archivo de la URL
                                file_name = f"file_{url_key}"
                                content_type = response.headers.get('Content-Type', 'application/octet-stream')
                                file_content = await response.read()
                                
                                # Subir archivo a Cloud Storage
                                storage_path = f"{citizen_id}/{file_name}"
                                blob = bucket.blob(storage_path)
                                blob.upload_from_string(
                                    file_content,
                                    content_type=content_type
                                )
                                
                                # Guardar en base de datos
                                from models.File import File as FileModel
                                from models.User import User
                                from sqlmodel import Session
                                from sqlmodel import select
                                
                                with Session(engine) as session:
                                    logger.info("Tomar usuario desde BD")
                                    statement = select(User).where(User.documentNumber == str(citizen_id))
                                    user = session.exec(statement).first()
                                    if not user:
                                        logger.error(f"Usuario no encontrado: {citizen_id}")
                                        await send_confirmation(int(citizen_id), 0, citizen_name, confirm_api)
                                        return 
                                    # Crear nuevo registro de archivo
                                    db_file = FileModel.create_new(
                                        user_id=user.id,
                                        file_name=file_name,
                                        file_type=content_type
                                    )
                                    db_file.file_path = storage_path
                                    session.add(db_file)
                                    session.commit()
                                
                                logger.info(f"Archivo {file_name} guardado exitosamente para {citizen_name}")
                    
                    except Exception as e:
                        await send_confirmation(int(citizen_id), 1, citizen_name, confirm_api)
                        logger.error(f"Error procesando archivo desde {url}: {e}")
            
            # Enviar confirmación a la API proporcionada
            await send_confirmation(int(citizen_id), 1, citizen_name, confirm_api)
                
        except json.JSONDecodeError:
            logger.error(f"Mensaje no es JSON válido: {message.body.decode()}")
        except Exception as e:
            logger.error(f"Error procesando transferencia de documentos: {e}")

# Send confirmation of trasfered user
async def send_confirmation(citizen_id: int, status: int, citizen_name: str, confirm_api: str): 
        try:
            async with aiohttp.ClientSession() as session:
                confirm_payload = {
                    "id": citizen_id,
                    "req_status": status,
                    "message": f"Documentos transferidos exitosamente para {citizen_name}"
                }
                async with session.post(confirm_api, json=confirm_payload) as response:
                    if response.status not in (200, 201, 204):
                        logger.error(f"Error al confirmar transferencia: {await response.text()}")
                    else:
                        logger.info(f"Transferencia confirmada para ciudadano {citizen_id}")
        except Exception as e:
            logger.error(f"Error al enviar confirmación: {e}")

# Delete file from DB
async def delete_file(message: IncomingMessage):
    async with message.process():  # Ack automático
        try:
            payload = json.loads(message.body.decode())
            user_id = payload.get("user_id")
            file_name = payload.get("file_name")

            logger.info(f"Mensaje recibido: {file_name}")

            # Nos conectamos a BD para traer la información del usuario
            from models.File import File as FileModel
            from models.User import User
            from sqlmodel import Session
            from sqlmodel import select
            
            with Session(engine) as session:
                logger.info("Tomar usuario desde BD")
                statement = select(User).where(User.id == str(user_id))
                user = session.exec(statement).first()
                if not user:
                    logger.error(f"Usuario no encontrado: {user_id}")
                    return 
                
                file_path = f"{user.documentNumber}/{file_name}"

                # Llamar al método de eliminar archivo de GCP
                file_deleted = await delete_file_gcp(file_path)
                if file_deleted:
                    # Eliminar archivo de BD
                    if FileModel.delete_by_path(session, file_path):
                        logger.info("Archivo eliminado exitosamente")
                        await send_notification("deletedFile", file_name, user.email)
                    else:
                        logger.error(f"El archivo no se pudo eliminar de BD")
                else:
                    logger.error(f"El archivo no se pudo eliminar del bucket")

        except json.JSONDecodeError:
            logger.error(f"Mensaje no es JSON válido: {message.body.decode()}")

# Update metadata to authorize file
async def update_metadata(message: IncomingMessage):
    async with message.process():  # Ack automático
        try:
            payload = json.loads(message.body.decode())
            user_id = payload.get("user_id")
            url_document = payload.get("url_document")
            file_name = payload.get("file_name")
            logger.info(f"Mensaje recibido autenticacion")

            adapter_url = "http://mrpotato-adapter-service.mrpotato-adapter.svc.cluster.local/v1/adapter/autheticateDocument"

            # Conexion BD 
            from models.File import File as FileModel
            from models.User import User
            from sqlmodel import Session
            from sqlmodel import select
            
            with Session(engine) as session_db:
                logger.info("Tomar usuario desde BD")
                statement = select(User).where(User.id == str(user_id))
                user = session_db.exec(statement).first()
                if not user:
                    logger.error(f"Usuario no encontrado: {user_id}")
                    return

                user_document_number = user.documentNumber
                user_email = user.email
                file_path = f"{user_document_number}/{file_name}"

                data = {
                    "idCitizen": user_document_number,
                    "UrlDocument": url_document,
                    "documentTitle": file_name
                }

                async with aiohttp.ClientSession() as session:
                    async with session.put(adapter_url, json=data) as resp:
                        status = resp.status
                        resp_data = await resp.json()

                        if status == 200 or status == 201:
                            logger.info(f"Documento autenticado exitosamente: {file_name}")
                            file_updated = await update_metadata_gcp(file_path)
                            if file_updated:
                                if FileModel.update_authenticated(session_db, file_path):
                                    logger.info("Archivo actualizado correctamente en BD")
                                    await send_notification("fileAuthenticated", file_name, user_email)
                                else:
                                    logger.error("El archivo no se pudo actualizar en BD")
                        elif status == 204:
                            logger.warning(f"Documento no encontrado para {file_name}")

                        else:
                            logger.error(f"Fallo autenticación. Status: {status}, Respuesta: {resp_data}")
            
        except json.JSONDecodeError:
            logger.error(f"Mensaje no es JSON válido: {message.body.decode()}")

async def handle_message_transfer_delete(message: IncomingMessage):
    async with message.process():  # Ack automático
        try:
            payload = json.loads(message.body.decode())
            id = payload.get("id")
            req_status = payload.get("req_status")
            logger.info(f"Usuario {id} - estado de transferencia {req_status}")

            await transfer_delete_docs(id, req_status)
        except json.JSONDecodeError:
            logger.error(f"Mensaje no es JSON válido: {message.body.decode()}")

async def main():
    try:
        # Connect to the database
        Base.metadata.create_all(bind=engine)
        logger.info("Conectado a la base de datos")
        
        connection = await connect_robust(
            host=rabbitmq_host,
            login=rabbitmq_user,
            password=rabbitmq_pass
        )
        logger.info("Conectado a Rabbit")

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        # Escuchar cola de eliminacion
        queue_delete = await channel.declare_queue(rabbitmq_queue_delete, durable=True)
        logger.info(f"Esperando mensajes en la cola: {rabbitmq_queue_delete}")
        await queue_delete.consume(delete_file)

        # Escuchar cola de autenticacion 
        queue_auth = await channel.declare_queue(rabbitmq_queue_auth, durable=True)
        logger.info(f"Esperando mensajes en la cola: {rabbitmq_queue_auth}")
        await queue_auth.consume(update_metadata)

        # Escuchar cola de eliminar todos los archivos de transferencia
        queue_transfer_delete = await channel.declare_queue(rabbitmq_queue_transfer_delete_docs, durable=True)
        logger.info(f"Esperando mensajes en la cola: {rabbitmq_queue_transfer_delete_docs}")
        await queue_transfer_delete.consume(handle_message_transfer_delete)
        
        # Escuchar cola de recibir archivos transferidos
        queue_transfer_receive = await channel.declare_queue(rabbitmq_queue_transfer_receive_docs, durable=True)
        logger.info(f"Esperando mensajes en la cola: {rabbitmq_queue_transfer_receive_docs}")
        await queue_transfer_receive.consume(transfer_receive_docs)

        # Mantener vivo el consumidor
        await asyncio.Future()

    except Exception as e:
        logger.error(f"Error en el consumidor: {e}")

if __name__ == "__main__":
    asyncio.run(main())
