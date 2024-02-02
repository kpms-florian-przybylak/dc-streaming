import asyncio
from mqtt_client import MQTTClient
from helpers.custom_logging_helper import logger
from rule_chain import RuleChain

class ClientManager:
    def __init__(self, specific_configs):
        self.specific_configs = specific_configs
        self.mqtt_clients = {}
        self.rule_chain = RuleChain(self.specific_configs["data_processing_chains"])

    async def initialize_mqtt_clients(self):
        """
        Initialize all MQTT clients.
        """
        for mqtt_client_config in self.specific_configs['mqtt_clients']:
            mqtt_client = MQTTClient(
                host=mqtt_client_config['server'],
                port=mqtt_client_config['port'],
                client_id=f"mqtt_{mqtt_client_config['id']}",
                username=mqtt_client_config['username'],
                password=mqtt_client_config['password']
            )
            self.mqtt_clients[mqtt_client_config['id']] = mqtt_client
            logger.info(f"MQTT client for {mqtt_client_config['id']} initialized.")

    async def connect_mqtt_clients(self):
        """
        Connect all MQTT clients.
        """
        for client_id, mqtt_client in self.mqtt_clients.items():
            await mqtt_client.connect_to_broker()
            logger.info(f"MQTT client {client_id} connected.")

    async def setup_rule_chains(self):
        for chain_config in self.specific_configs["data_processing_chains"]:
            for source in chain_config.get("sources", []):
                client_id = source["client_id"]
                if source["client_type"] == "mqtt" and client_id in self.mqtt_clients:
                    client = self.mqtt_clients[client_id]
                    client.set_processing_chain(self.rule_chain)

    async def subscribe_to_topics(self):
        for chain_config in self.specific_configs["data_processing_chains"]:
            topics_by_client = {}  # Sammeln von Topics nach Client-ID
            for source in chain_config.get("sources", []):
                client_id = source["client_id"]
                if source["client_type"] == "mqtt":
                    if client_id not in topics_by_client:
                        topics_by_client[client_id] = []
                    topics_by_client[client_id].append(source["topic"])

            for client_id, topics in topics_by_client.items():
                if client_id in self.mqtt_clients:
                    client = self.mqtt_clients[client_id]
                    logger.info(f"Subscribing client '{client_id}' to topics: {topics}")
                    await client.subscribe_to_topics(topics)  # Abonnieren aller gesammelten Topics
                    for topic in topics:
                        logger.info(f"Subscribed to topic: {topic}")

    async def initialize_and_run_clients(self):
        """
        Initialize, connect, and set up MQTT clients.
        """
        await self.initialize_mqtt_clients()
        #await self.connect_mqtt_clients()
        await self.setup_rule_chains()
        await self.subscribe_to_topics()
