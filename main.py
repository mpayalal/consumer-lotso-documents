import os
import json
import asyncio
import logging
import aiohttp
from aio_pika import connect_robust, IncomingMessage, Message
from google.cloud import storage

rabbitmq_user = os.getenv("RABBITMQ_USER") 
rabbitmq_pass = os.getenv("RABBITMQ_PASSWORD")
rabbitmq_host = os.getenv("RABBITMQ_HOST")
rabbitmq_port = os.getenv("RABBITMQ_PORT")
rabbitmq_queue_delete = os.getenv("RABBITMQ_QUEUE_DELETE")
rabbitmq_queue_auth = os.getenv("RABBITMQ_QUEUE_AUTHENTICATION")
rabbitmq_queue_notifications = os.getenv("RABBITMQ_QUEUE_NOTIFICATIONS")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def delete_file(file_name: str, client_email: str):
    try:
        creds_path = os.getenv("GCP_SA_KEY")        
        bucket_name = os.getenv("GCP_BUCKET_NAME")
        gcs = storage.Client.from_service_account_json(creds_path)
    
        bucket = gcs.get_bucket(bucket_name)
        file_to_delete = bucket.get_blob(file_name)
        if file_to_delete:
            file_to_delete.delete()
            logger.info(f"Archivo eliminado: {file_name}")
            await send_notification(file_name, client_email)
        else: 
            logger.warning(f"Archivo no encontrado: {file_name}")

    except Exception as e:
        logger.error(f"Error al eliminar archivo: {e}")
    
async def send_notification(file_name: str, client_email: str):
    try:
        message = {
            "action": "deletedFile",
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

async def update_metadata(client_id: str, file_name: str):
    try:
        creds_path = os.getenv("GCP_SA_KEY")        
        bucket_name = os.getenv("GCP_BUCKET_NAME")
        gcs = storage.Client.from_service_account_json(creds_path)
    
        bucket = gcs.get_bucket(bucket_name)
        file_path = f"{client_id}/{file_name}"
        file_to_update = bucket.get_blob(file_path)

        if file_to_update:
            metadata = file_to_update.meatdata
            metadata["firmado"] = "true"
            file_to_update.metadata = metadata
            file_to_update.patch()

            logger.info(f"Archivo firmado exitosamente")
            
        else: 
            logger.warning(f"Archivo no encontrado: {file_name}")

    except Exception as e:
        logger.error(f"Error al actualizar autenticacion: {e}")

async def handle_message(message: IncomingMessage):
    async with message.process():  # Ack automático
        try:
            payload = json.loads(message.body.decode())
            file_name = payload.get("file_name")
            client_email = payload.get("client_email")
            logger.info(f"Mensaje recibido: {file_name}")

            await delete_file(file_name, client_email)
        except json.JSONDecodeError:
            logger.error(f"Mensaje no es JSON válido: {message.body.decode()}")

async def handle_authenticate_message(message: IncomingMessage):
    async with message.process():  # Ack automático
        try:
            payload = json.loads(message.body.decode())
            client_id = payload.get("client_id")
            url_document = payload.get("url_document")
            file_name = payload.get("file_name")
            logger.info(f"Mensaje recibido autenticacion")

            adapter_url = "http://mrpotato-adapter-service.mrpotato-adapter.svc.cluster.local/v1/adapter/autheticateDocument"

            data = {
                "idCitizen": client_id,
                "UrlDocument": url_document,
                "documentTitle": file_name
            }

            async with aiohttp.ClientSession() as session:
                async with session.put(adapter_url, json=data) as resp:
                    status = resp.status
                    resp_data = await resp.json()

                    if status == 200 or status == 201:
                        logger.info(f"Documento autenticado exitosamente: {file_name}")
                        await update_metadata(client_id, file_name)
                    elif status == 204:
                        logger.warning(f"Documento no encontrado para {file_name}")
                    else:
                        logger.error(f"Fallo autenticación. Status: {status}, Respuesta: {resp_data}")
            
        except json.JSONDecodeError:
            logger.error(f"Mensaje no es JSON válido: {message.body.decode()}")

async def main():
    try:
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
        await queue_delete.consume(handle_message)

        # Escuchar cola de autenticacion 
        queue_auth = await channel.declare_queue("authenticate", durable=True)
        logger.info(f"Esperando mensajes en la cola: {rabbitmq_queue_delete}")
        await queue_auth.consume(handle_authenticate_message)

        # Mantener vivo el consumidor
        await asyncio.Future()

    except Exception as e:
        logger.error(f"Error en el consumidor: {e}")

if __name__ == "__main__":
    asyncio.run(main())
