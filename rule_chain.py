import asyncio
import json
import os
import importlib.util
import time
from typing import List

from helpers.custom_logging_helper import logger

# Ermitteln des Basisverzeichnisses des Projekts
script_dir = os.path.dirname(os.path.abspath(__file__))

python_interpreter = os.getenv('PYTHON_INTERPRETER_PATH', 'python3')  # Standardmäßig 'python3', falls nicht definiert




class RuleChain:
    def __init__(self, chains_config, targets=None, mqtt_clients=None, db_clients=None, redis_clients=None):
        self.targets = targets if targets is not None else []
        self.chain = []
        self.mqtt_clients = mqtt_clients if mqtt_clients is not None else {}
        self.db_clients = db_clients if db_clients is not None else {}
        self.redis_clients = redis_clients if redis_clients is not None else {}
        self.chains_config = chains_config
        self.initialize_chain()
        self.last_query_time = {}

    def initialize_chain(self):
        for chain in self.chains_config:
            logger.info("Initializing step: {}".format(chain['id']))

    def get_last_update_time(self, db_id, query):
        logger.info("get_last_update_time", query, db_id)
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


    async def execute_python_script(self, script_path, input_message, client_access):
        """
        Dynamically loads and executes a Python script with the specified input message and client objects.
        This method bypasses the limitations of inter-process communication by directly invoking the script in the same process.
        """
        # Construct the full path to the script
        full_script_path = os.path.join(script_dir, 'configs', 'external_scripts', script_path)

        # Attempt to dynamically load the script as a module
        try:
            spec = importlib.util.spec_from_file_location("external_module", full_script_path)
            external_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(external_module)
        except FileNotFoundError:
            logger.error(f"The script {script_path} was not found.")
            return input_message
        except Exception as e:
            logger.error(f"An error occurred while loading the script {script_path}: {str(e)}")
            return input_message

        # Prepare the client objects for the script based on 'client_access'
        clients = self.prepare_clients_for_script(client_access)

        # Execute the unified function in the script
        try:
            if asyncio.iscoroutinefunction(external_module.process_message):
                processed_message = await external_module.process_message(input_message, clients)
            else:
                processed_message = external_module.process_message(input_message, clients)
            return processed_message
        except AttributeError:
            logger.error(f"The script {script_path} does not have a 'process_message' function.")
        except Exception as e:
            logger.error(f"An error occurred while executing the script {script_path}: {str(e)}")

        return input_message

    def prepare_clients_for_script(self, client_access):
        """
        Prepares a dictionary of actual client instances based on client_access identifiers.
        """
        clients_info = {}
        for client_id in client_access:
            if client_id in self.mqtt_clients:
                # Direkte Übergabe der MQTT Client-Instanz
                clients_info[client_id] = self.mqtt_clients[client_id]
            elif client_id in self.db_clients:
                # Direkte Übergabe der DB Client-Instanz
                clients_info[client_id] = self.db_clients[client_id]
            elif client_id in self.redis_clients:
                # Direkte Übergabe der Redis Client-Instanz
                clients_info[client_id] = self.redis_clients[client_id]
            else:
                logger.warning(f"Client ID {client_id} not found among available clients.")
        return clients_info

    async def process_step(self, message, client_id):
        """
        Process each step in the rule chain with modifications to handle client access.
        """
        modified_message = message
        chain_ids = self.find_chains_by_client_id(client_id)
        for chain_id in chain_ids:
            chain_config = next((chain for chain in self.chains_config if chain['id'] == chain_id), None)
            if chain_config:
                for step in chain_config['processing_steps']:
                    # Inside the process_step method or wherever you call execute_python_script
                    if step['type'] == 'python_script':
                        client_access = step.get('client_access', [])
                        modified_message = await self.execute_python_script(step['script_path'], modified_message,
                                                                            client_access)



                        logger.debug("{}, {}, {}".format(step['script_path'], step['type'], type(modified_message)))

                    if step['type'] == 'sql_query':
                        # Angenommen, execute_sql_query aktualisiert das Wörterbuch basierend auf der Abfrage
                        modified_message = await self.execute_sql_query(step['query'], step['id'], modified_message)
                    elif step['type'] == 'python_script':
                        # Angenommen, execute_python_script kann das Wörterbuch als Argument akzeptieren und es aktualisieren
                        modified_message = await self.execute_python_script(step['script_path'], modified_message,
                                                                            client_access)
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
                # Behandlung für MQTT Targets
                if target['client_id'] in self.mqtt_clients:
                    try:
                        client = self.mqtt_clients[target['client_id']]
                        message_str = json.dumps(message) if not isinstance(message, str) else message
                        await client.publish_message(target['topic'], message_str)
                        logger.info(f"Message sent to MQTT {target['client_id']} on topic {target['topic']}")
                    except Exception as e:
                        logger.error(
                            f"Error sending MQTT message to {target['client_id']} on topic {target['topic']}: {e}")

                # Bulk Insert für PostgreSQL Targets
                elif target['client_type'] == 'postgres':
                    try:
                        db_client = self.db_clients[target['client_id']]
                        # Konvertiere `message` in eine Liste von Dictionaries, falls erforderlich
                        data = message if isinstance(message, list) else [message]
                        # Führe den Bulk Insert aus
                        await db_client.execute_bulk_insert(target['insert_statement'], data,
                                                            target.get('batch_size', 100))
                        logger.info(f"Bulk insert sent to PostgreSQL {target['client_id']}")
                    except Exception as e:
                        logger.error(f"Error performing bulk insert for PostgreSQL {target['client_id']}: {e}")

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
