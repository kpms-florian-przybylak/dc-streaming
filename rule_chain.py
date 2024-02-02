import json

from helpers.custom_logging_helper import logger
from typing import Dict, List


class RuleChain:
    def __init__(self, chains_config, targets=None, mqtt_clients=None):
        self.targets = targets if targets is not None else []
        self.chain = []
        self.mqtt_clients = mqtt_clients
        self.chains_config = chains_config
        self.initialize_chain()

    def initialize_chain(self):
        # Initialisieren der Verarbeitungskette basierend auf den übergebenen Schritten
        # und Vorbereiten der Ziele.
        for chain in self.chains_config:
            print("Initializing step", chain)

    async def process_step(self, message, client_id):
        print("process step for", client_id)
        # Hier würde die spezifische Logik für jeden Schritt implementiert,
        # z.B. SQL-Abfrage ausführen oder Python-Skript aufrufen
        return message
    def find_chains_by_client_id(self, client_id: str) -> List[str]:
        """Finde alle Chain IDs, die einer gegebenen Client ID entsprechen."""
        matching_chains = []
        for chain in self.chains_config:
            for source in chain['sources']:
                if source['client_id'] == client_id:
                    matching_chains.append(chain['id'])
        return matching_chains

    async def forward_to_targets(self, chain_id, message):
        chain_config = next((chain for chain in self.chains_config if chain['id'] == chain_id), None)
        if chain_config:
            for target in chain_config.get('targets', []):
                if target['client_id'] in self.mqtt_clients:
                    client = self.mqtt_clients[target['client_id']]
                    # Stellen Sie sicher, dass die Nachricht serialisierbar ist
                    try:
                        # Wenn `message` bereits eine Zeichenkette ist, verwenden Sie sie direkt
                        if isinstance(message, str):
                            message_str = message
                        else:
                            # Versuchen Sie, die Nachricht zu serialisieren, wenn es sich um ein serialisierbares Objekt handelt
                            message_str = json.dumps(message)
                    except TypeError as e:
                        # Protokollieren Sie den Fehler oder handhaben Sie den Fall, wenn die Nachricht nicht serialisierbar ist
                        logger.error(f"Message serialization error: {e}")
                        return  # Beenden Sie die Methode, wenn die Nachricht nicht serialisiert werden kann

                    # Nachricht veröffentlichen
                    await client.publish_message(target['topic'], message_str)
                    logger.info(f"Message sent to {target['client_id']} on topic {target['topic']}")

    async def handle_incoming_message(self, message, client_id):
        logger.info(f"Received message from client: {client_id} on topic {message.topic}: {message.payload.decode()}")
        payload = message.payload.decode()  # Nimmt an, dass die Nutzlast eine Zeichenkette ist
        try:
            decoded_message = json.loads(payload)

        except json.JSONDecodeError:
            # Falls die Nutzlast kein JSON ist, verwenden Sie die rohe Zeichenkette
            decoded_message = payload


        processed_data = await self.process_step(decoded_message, client_id)
        # Finde die zugehörigen Chain IDs für die gegebene Client ID
        chain_ids = self.find_chains_by_client_id(client_id)
        # Iteriere über jede gefundene Chain ID und leite die verarbeitete Nachricht weiter
        for chain_id in chain_ids:
            await self.forward_to_targets(chain_id, processed_data)
        return message
