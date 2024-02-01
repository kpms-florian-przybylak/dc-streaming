import asyncio
from mqtt_client import MQTTClient
from helpers.custom_logging_helper import logger
from rule_chain import RuleChain


class ClientManager:
    def __init__(self, specific_configs):
        self.specific_configs = specific_configs
        self.mqtt_clients = []
        self.rule_chain = RuleChain(self.specific_configs["data_processing_chains"])

    async def initialize_mqtt_client(self, client_config):
        mqtt_client = MQTTClient(
            host=client_config['server'],
            port=client_config['port'],
            client_id=f"mqtt_{client_config['id']}",
            username=client_config['username'],
            password=client_config['password']
        )
        self.mqtt_clients.append(mqtt_client)
        logger.info(f"MQTT client for {client_config['id']} initialized.")


    async def initialize_and_run_clients(self):
        """
        Initialisiert und startet alle MQTT-Clients.
        """
        for mqtt_client_config in self.specific_configs['mqtt_clients']:
            await self.initialize_mqtt_client(mqtt_client_config)
