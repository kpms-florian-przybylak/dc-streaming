import json
import subprocess
import os

from helpers.custom_logging_helper import logger
from typing import Dict, List
import time
# Ermitteln des Basisverzeichnisses des Projekts
base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class RuleChain:
    def __init__(self, chains_config, targets=None, mqtt_clients=None, db_clients=None):
        self.targets = targets if targets is not None else []
        self.chain = []
        self.mqtt_clients = mqtt_clients
        self.chains_config = chains_config
        self.initialize_chain()
        self.last_query_time = {}
        self.db_clients = db_clients

    def initialize_chain(self):
        # Initialisieren der Verarbeitungskette basierend auf den übergebenen Schritten
        # und Vorbereiten der Ziele.
        for chain in self.chains_config:
            print("Initializing step", chain)
    def get_last_update_time(self, db_id, query):
        print("get_last_update_time", query, db_id)
        # Implementiere eine Methode, um den Zeitstempel der letzten relevanten Datenänderung zu ermitteln.
        return time.time()
    async def execute_sql_query(self, query, db_id, input_message):
        # Bestimme, ob die Daten seit der letzten Abfrage geändert wurden
        last_update_time = self.get_last_update_time(db_id, query)

        if db_id not in self.last_query_time or self.last_query_time[db_id] < last_update_time:
            # Führe die Abfrage aus, wenn es Änderungen gab oder die Abfrage noch nie ausgeführt wurde
            db_client = self.db_clients[db_id]
            result = db_client.execute_query(query)
            result_list = [dict(row) for row in result]
            # Aktualisiere den Zeitstempel der letzten erfolgreichen Abfrage
            self.last_query_time[db_id] = time.time()
            return result_list  # Oder modifiziere die input_message basierend auf dem Ergebnis
        else:
            # Keine Änderungen, kein Bedarf, die Abfrage erneut auszuführen
            return input_message
    async def execute_python_script(self, script_path, input_message):
        full_script_path = os.path.join(base_dir, 'dc-streaming', 'configs', 'external_scripts', script_path)
        print(f"Executing Python script: {full_script_path}")
        # Beispiel für die Ausführung eines Python-Skripts und die Rückgabe eines modifizierten Nachrichtenobjekts
        try:
            completed_process = subprocess.run(['python3', full_script_path, json.dumps(input_message)], timeout=10,
                                               capture_output=True, text=True, check=True)
            if completed_process.returncode != 0:
                print(f"Error executing script {script_path}: {completed_process.stderr}")
                return input_message  # Bei einem Fehler die ursprüngliche Nachricht zurückgeben
            modified_message = json.loads(completed_process.stdout) if completed_process.stdout else input_message
            return modified_message
        except subprocess.CalledProcessError as e:
            print(f"Script execution failed with non-zero exit status: {e.returncode}, {e.stderr}")
        except subprocess.TimeoutExpired:
            print(f"Script execution timed out: {script_path}")
        except Exception as e:
            print(f"Unexpected error executing script {script_path}: {str(e)}")
            return input_message  # Bei einem Fehler die ursprüngliche Nachricht zurückgeben


    async def process_step(self, message, client_id):
        logger.info("Processing step for client_id: %s with message: %s", client_id, message)
        modified_message = message  # Starten mit der ursprünglichen Nachricht
        chain_ids = self.find_chains_by_client_id(client_id)
        for chain_id in chain_ids:
            chain_config = next((chain for chain in self.chains_config if chain['id'] == chain_id), None)
            if chain_config:
                for step in chain_config['processing_steps']:
                    if step['type'] == 'sql_query':
                        modified_message = await self.execute_sql_query(step['query'], step['id'], modified_message)
                    elif step['type'] == 'python_script':
                        modified_message = await self.execute_python_script(step['script_path'], modified_message)
                    else:
                        logger.warning("Unknown step type: %s", step['type'])
                await self.forward_to_targets(chain_id, modified_message)
        return modified_message

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
        payload = message.payload.decode()  # Nimmt an, dass die Nutzlast eine Zeichenkette ist
        logger.info(f"Received message from client: {client_id} on topic {message.topic}: {payload}")
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
