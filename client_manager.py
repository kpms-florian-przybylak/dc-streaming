import asyncio
from mqtt_client import MQTTClient
from helpers.custom_logging_helper import logger

class ClientManager:
    def __init__(self, specific_configs):
        self.specific_configs = specific_configs
        self.target_clients = []

    async def initialize_target_client(self, name, client_id):
        """
        Initialisiert einen MQTTClient für das angegebene Ziel.
        """
        logger.info(f"Initializing target client for {name} with client ID {client_id}")


        for target_config in self.specific_configs['mqtt_targets']:
            if target_config['name'] == name:
                mqtt_client = MQTTClient(
                    host=target_config['server'],
                    port=target_config['port'],
                    client_id=client_id,
                    username=target_config['username'],
                    password=target_config['password'],
                    topic=target_config['topic']
                )
                logger.info(f"MQTT client for {name} initialized successfully.")
                return mqtt_client
        logger.error(f"Target configuration for {name} not found.")
        return None

    async def initialize_and_run_clients(self):
        """
        Initialisiert und startet alle MQTT-Clients.
        """
        used_targets = set(target['name'] for target in self.specific_configs['mqtt_targets'])
        tasks = []
        for name in used_targets:
            client_id = f"client_{name}"
            client = await self.initialize_target_client(name, client_id)
            if client:
                self.target_clients.append(client)
                task = asyncio.create_task(client.run_client())
                tasks.append(task)

        if tasks:
            await asyncio.wait(tasks)
