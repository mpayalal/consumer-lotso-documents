import os
import asyncio
import logging
from aio_pika import connect_robust, IncomingMessage
from google.cloud import storage

rabbitmq_user = os.getenv("RABBITMQ_USER") 
rabbitmq_pass = os.getenv("RABBITMQ_PASSWORD")
rabbitmq_host = os.getenv("RABBITMQ_HOST")
rabbitmq_port = os.getenv("RABBITMQ_PORT")
rabbitmq_queue = os.getenv("RABBITMQ_QUEUE_DELETE")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

async def delete_file(file_name: str):
    try:
        creds_path = os.getenv("GCP_SA_KEY")
        logger.info(f"Creds path encontradas: {creds_path}")
        
        bucket_name = os.getenv("GCP_BUCKET_NAME")
        gcs = storage.Client.from_service_account_json(creds_path)
    
        bucket = gcs.get_bucket(bucket_name)
        file_to_delete = bucket.get_blob(file_name)
        if file_to_delete:
            file_to_delete.delete()
            logger.info(f"Archivo eliminado: {file_name}")
        else: 
            logger.warning(f"Archivo no encontrado: {file_name}")

    except Exception as e:
        logger.error(f"Error al eliminar archivo: {e}")
    
async def handle_message(message: IncomingMessage):
    async with message.process():  # Ack automático
        file_name = message.body.decode()
        logger.info(f"Mensaje recibido: {file_name}")
        await delete_file(file_name)

async def main():
    try:
        connection = await connect_robust(
            host=rabbitmq_host,
            login=rabbitmq_user,
            password=rabbitmq_pass
        )
        logger.info("Conectado a Rabbit")
        print("Conectado a Rabbit")

        channel = await connection.channel()
        await channel.set_qos(prefetch_count=1)

        queue = await channel.declare_queue(rabbitmq_queue, durable=True)
        logger.info(f"Esperando mensajes en la cola: {rabbitmq_queue}")
        print(f"Esperando mensajes en la cola: {rabbitmq_queue}")
        await queue.consume(handle_message)

        # Mantener vivo el consumidor
        await asyncio.Future()

    except Exception as e:
        logger.error(f"Error en el consumidor: {e}")
        print(f"Error en el consumidor: {e}")

if __name__ == "__main__":
    asyncio.run(main())
