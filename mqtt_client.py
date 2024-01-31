import asyncio
import aiomqtt
from aiomqtt.client import ClientT

from helpers.custom_logging_helper import logger


class MQTTClient:

    def __init__(self, host: str, port: int, client_id: str, username: str = "", password: str = "", topic: ClientT=""):
        logger.info("Initializing MQTT client...")
        self.topic = topic
        self.hostname = host
        self.port = port
        self.client = aiomqtt.Client(hostname=host, port=port, client_id=client_id, username=username,
                                     password=password)
        self.processing_chain = None

        logger.success(f"MQTT client initialized with Host: {host}, Port: {port}, Client ID: {client_id}")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return self

    async def connect_to_broker(self) -> None:
        interval = 5  # Sekunden für erneuten Versuch
        logger.info(f"Attempting to connect to MQTT broker at {self.hostname}:{self.port}...")
        while True:
            try:
                async with self.client:
                    logger.success("Connected to MQTT broker!")
                    async with self.client.messages() as messages:
                        logger.info(f"Subscribing to topic: {self.topic}")
                        await self.client.subscribe(self.topic)
                        await self.handle_messages(messages)
            except aiomqtt.MqttError as e:
                logger.danger(f"Failed to connect or lost connection: {e}.")
                logger.warning(f"Reconnecting in {interval} seconds ...")
                await asyncio.sleep(interval)

    async def handle_messages(self, messages):
        logger.info("Starting to handle incoming MQTT messages...")
        async for msg in messages:
            # Verarbeiten der Nachricht mit der übergebenen Verarbeitungskette
            processed_message = self.process_message(msg.payload.decode()) if self.processing_chain else msg.payload.decode()
            logger.info(f"Received message on topic {msg.topic}: {processed_message}")

    async def run_client(self, processing_chain=None) -> None:
        self.processing_chain = processing_chain  # Setzen der Verarbeitungskette
        logger.info("Starting MQTT client...")
        await self.connect_to_broker()

    def process_message(self, message):
        for process in self.processing_chain:
            message = process(message)
        return message

    async def publish_message(self, topic, message):
        logger.info(f"Publishing message to topic {topic}...")
        await self.client.publish(topic, message.encode())

