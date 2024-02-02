import asyncio
import aiomqtt
from aiomqtt.client import ClientT

from helpers.custom_logging_helper import logger


class MQTTClient:
    def __init__(self, host: str, port: int, client_id: str, username: str = "", password: str = "", topics=None):
        self.client_id = client_id
        self.hostname = host
        self.port = port
        self.username = username
        self.password = password
        self.topics = topics if topics is not None else []
        self.subscribed_topics = set()
        self.processing_chain = None

        self.client = aiomqtt.Client(hostname=host, port=port, client_id=client_id, username=username, password=password)
        logger.info("Initializing MQTT client...")
        logger.success(f"MQTT client initialized with Host: {host}, Port: {port}, Client ID: {client_id}")

    def set_processing_chain(self, processing_chain):
        self.processing_chain = processing_chain

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self

    async def subscribe_to_topics(self, topics):

        tasks = []
        interval = 10  # Seconds for retry
        logger.info(f"Attempting to connect to MQTT broker at {self.hostname}:{self.port}...")
        while True:
            try:
                async with self.client:
                    logger.success("Connected to MQTT broker!")
                    async with self.client.messages() as messages:
                        for topic in topics:
                            task = asyncio.create_task(self.client.subscribe(topic))
                            tasks.append(task)
                            logger.info(f"Subscribing to topic: {topic}")
                            self.subscribed_topics.add(topic)
                        task = asyncio.create_task(self.handle_messages(messages))
                        tasks.append(task)
                        await asyncio.gather(*tasks)
            except aiomqtt.MqttError as e:
                logger.danger(f"Failed to connect or lost connection: {e}.")
                logger.warning(f"Reconnecting in {interval} seconds ...")
                await asyncio.sleep(interval)
            except Exception as e:
                logger.error(f"An error occurred: {str(e)}")
            finally:
                logger.warning("Reconnecting...")
                await asyncio.sleep(interval)

    async def handle_messages(self, messages):
        async for message in messages:
            asyncio.create_task(self.processing_chain.handle_incoming_message(message, self.client_id))


    async def publish_message(self, topic, message):
        logger.info(f"Publishing message to topic {topic}...")
        await self.client.publish(topic, message)
