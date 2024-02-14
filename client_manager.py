import asyncio

from db_client import DBClient
from mqtt_client import MQTTClient
from helpers.custom_logging_helper import logger
from rule_chain import RuleChain

class ClientManager:
    def __init__(self, specific_configs):
        self.specific_configs = specific_configs
        self.mqtt_clients = {}
        self.db_clients = {}
        # Extrahieren der Ziel-Informationen
        targets = self.extract_targets(specific_configs["data_processing_chains"])
        # Initialisierung der RuleChain mit Schritten, Zielen und MQTT-Clients
        self.rule_chain = None  # Dies wird in initialize_and_run_clients gesetzt

    def extract_targets(self, chains_config):
        targets = []
        for chain in chains_config:
            if "targets" in chain:
                targets.extend(chain["targets"])
        return targets

    async def initialize_mqtt_clients(self):
        """
        Initialize all MQTT clients.
        """
        for mqtt_client_config in self.specific_configs['mqtt_clients']:
            mqtt_client = MQTTClient(
                host=mqtt_client_config['server'],
                port=mqtt_client_config['port'],
                client_id=mqtt_client_config['id'],
                username=mqtt_client_config['username'],
                password=mqtt_client_config['password']
            )
            self.mqtt_clients[mqtt_client_config['id']] = mqtt_client
            logger.info(f"MQTT client for {mqtt_client_config['id']} initialized.")

    async def initialize_db_clients(self):
        """
        Initialize all PostgreSQL clients.
        """
        for db_client_config in self.specific_configs['postgres_clients']:
            db_client = DBClient(
                client_id = db_client_config['id'],
                connection_string = db_client_config['connection_string']
            )
            await db_client.connect_and_verify()
            self.db_clients[db_client_config['id']] = db_client
            logger.info(f"DB client for {db_client_config['id']} initialized.")
            asyncio.create_task(db_client.start_periodic_verification(30))



    async def initialize_db_polling(self):
        """
        Initializes polling for configured PostgreSQL queries.
        """
        for chain_config in self.specific_configs["data_processing_chains"]:
            for source in chain_config.get("sources", []):
                if source["client_type"] == "postgres":
                    db_client = self.db_clients[source["client_id"]]
                    polling_interval = source.get("polling_interval", 60)  # Standardintervall: 60 Sekunden
                    query = source.get("query")
                    # Startet das Polling für die SQL-Abfrage
                    asyncio.create_task(db_client.start_polling_query(query, polling_interval, self.rule_chain))
                    logger.info(f"Initialized polling for query '{query}' every {polling_interval} seconds.")
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
    async def setup_rule_chains(self):
        # Stellen Sie sicher, dass die RuleChain jetzt mit allen notwendigen Informationen initialisiert wird
        targets = self.extract_targets(self.specific_configs["data_processing_chains"])
        self.rule_chain =  RuleChain(self.specific_configs["data_processing_chains"], targets, self.mqtt_clients, self.db_clients)
        for client_id, mqtt_client in self.mqtt_clients.items():
            mqtt_client.set_processing_chain(self.rule_chain)

    async def initialize_and_run_clients(self):
        """
        Initialize, connect, and set up all clients and start polling, all in parallel.
        """
        init_tasks = [
            self.initialize_mqtt_clients(),
            self.initialize_db_clients(),
            self.setup_rule_chains(),
            self.subscribe_to_topics(),
            self.initialize_db_polling()
        ]

        # Warte auf die Fertigstellung der Initialisierungsaufgaben, außer DB Polling
        await asyncio.gather(*init_tasks)

