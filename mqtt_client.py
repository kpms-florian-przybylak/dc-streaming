import asyncio
import aiomqtt
from aiomqtt.client import ClientT

from helpers.custom_logging_helper import logger


class MQTTClient:

    def __init__(self, host: str, port: int, client_id: str, username: str = "", password: str = "", topics=None):
        logger.info("Initializing MQTT client...")
        self.hostname = host
        self.port = port
        self.client = aiomqtt.Client(hostname=host, port=port, client_id=client_id, username=username,
                                     password=password)
        self.topics = topics if topics is not None else []  # Kann eine Liste von Topics sein

        self.processing_chain = None
        self.subscribed_topics = set()  # Zum Speichern der abonnierten Topics

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
                        for topic in self.topics:
                            logger.info(f"Subscribing to topic: {topic}")
                            await self.subscribe_to_topic(topic)
            except aiomqtt.MqttError as e:
                logger.danger(f"Failed to connect or lost connection: {e}.")
                logger.warning(f"Reconnecting in {interval} seconds ...")
                await asyncio.sleep(interval)

    async def subscribe_to_topic(self, topic):
        """Subscribt zu einem oder mehreren Topics."""
        if topic not in self.subscribed_topics:
            await self.client.subscribe(topic)
            self.subscribed_topics.add(topic)
            logger.info(f"Subscribed to topic: {topic}")
    async def run_client(self) -> None:
        """Hauptmethode zum Starten des Clients."""
        logger.info("Starting MQTT client...")
        await self.connect_to_broker()

    def process_message(self, message):
        for process in self.processing_chain:
            message = process(message)
        return message

    async def publish_message(self, topic, message):
        """Veröffentlicht eine Nachricht auf dem angegebenen Topic."""
        logger.info(f"Publishing message to topic {topic}...")
        await self.client.publish(topic, message.encode())

