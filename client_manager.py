import asyncio
from mqtt_client import MQTTClient
from helpers.custom_logging_helper import logger

class ClientManager:
    def __init__(self, specific_configs):
        self.specific_configs = specific_configs
        self.target_clients = []
        self.source_clients = []

    async def initialize_source_client(self, name):
        """
        Initialisiert einen MQTTClient für die angegebene Quelle und subscribt das Topic.
        """
        for source_config in self.specific_configs['mqtt_sources']:
            if source_config['name'] == name:
                mqtt_client = MQTTClient(
                    host=source_config['server'],
                    port=source_config['port'],
                    client_id=f"source_{name}",
                    username=source_config['username'],
                    password=source_config['password'],
                    topic=source_config['topic']  # Dieses Topic wird für das Subscribing verwendet
                )
                await mqtt_client.subscribe(mqtt_client.topic)  # Stellen Sie sicher, dass diese Methode existiert
                logger.info(f"MQTT source client for {name} initialized and subscribed to {mqtt_client.topic}.")
                return mqtt_client
        logger.error(f"Source configuration for {name} not found.")
        return None

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
        # Initialisiere und starte Quellen-Clients (Sources)
        used_sources = set(source['name'] for source in self.specific_configs['mqtt_sources'])
        source_tasks = []
        for name in used_sources:
            source_client = await self.initialize_source_client(name)
            if source_client:
                self.source_clients.append(source_client)
                task = asyncio.create_task(source_client.run_client())  # Stellen Sie sicher, dass run_client existiert
                source_tasks.append(task)

        if source_tasks:
            await asyncio.wait(source_tasks)


        if tasks:
            await asyncio.wait(tasks)
