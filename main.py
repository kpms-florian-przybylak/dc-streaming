

"""
Entry Point for the dc-streaming Service
"""
import asyncio
from typing import Optional, Dict, Any, Set
from asyncio import run
from helpers.custom_logging_helper import logger
from helpers.json_file_manager import JSONFileManager
from mqtt_client import MQTTClient
from rule_chain import RuleChain


async def start_and_monitor_clients(target_clients):
    tasks = [asyncio.create_task(client.run_client()) for client in target_clients.values()]

    # Warte auf ein Abbruchsignal (z.B. KeyboardInterrupt) oder eine andere Bedingung
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    except KeyboardInterrupt:
        logger.info("Shutting down clients...")

    # Beende alle Tasks ordentlich
    for task in tasks:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    logger.info("All clients have been shut down.")
async def initialize_target_client(name: str, specific_configs: Dict[str, Any], client_id: str) -> MQTTClient:
    """
    Initialisiert einen MQTTClient für das angegebene Ziel.
    """
    # Suche die Konfiguration für das gegebene Ziel
    for target_config in specific_configs['mqtt_targets']:
        if target_config['name'] == name:
            # Erstelle eine Instanz des MQTTClient mit der gefundenen Konfiguration
            mqtt_client = MQTTClient(
                host=target_config['server'],
                port=target_config['port'],
                client_id=client_id,
                username=target_config['username'],
                password=target_config['password'],
                topic=target_config['topic']
            )
            return mqtt_client
    # Falls kein passendes Ziel gefunden wurde, gebe None zurück
    return None

async def initialize_used_targets(specific_configs: Dict[str, Any]):
    used_targets: Set[str] = set()

    # Sammle alle genutzten Ziele aus den Datenverarbeitungsketten
    for chain in specific_configs['data_chains']:
        for target in chain['targets']:
            used_targets.add(target)

    # Initialisiere MQTT-Clients nur für die genutzten Ziele
    target_clients = {}
    for name in used_targets:
        client_id = f"client_{name}"  # Generiere eine eindeutige Client-ID
        # Achte darauf, den korrigierten Funktionsnamen hier zu verwenden
        client = await initialize_target_client(name, specific_configs, client_id)
        if client:
            target_clients[name] = client

    return target_clients




def validate_and_get_configs(config_managers) -> Optional[Dict[str, Any]]:
    """
    Validates the configuration files for MQTT, OPC, and Handshake.

    Returns:
        Optional[Dict[str, Any]]: Validated configurations or None if any of them are invalid.
    """
    # Validate each configuration using the respective manager
    validated_data = {key: manager.get_validated_json() for key, manager in config_managers.items()}
    if None in validated_data.values():
        logger.danger("Failed to validate configurations.")
        return None

    # Ensure at least one source and one target are defined
    chain_config = validated_data.get('chain_config', {})
    if not chain_config.get('mqtt_sources') and not chain_config.get('postgres_sources'):
        logger.danger("At least one source (MQTT or Postgres) must be defined.")
        return None
    if not chain_config.get('mqtt_targets'):
        logger.danger("At least one MQTT target must be defined.")
        return None

    return validated_data
def extract_specific_configs(validated_data):
    if not validated_data:
        return None

    mqtt_sources = []
    postgres_sources = []
    mqtt_targets = []
    data_chains = []

    chain_config = validated_data.get('chain_config')
    if chain_config:
        mqtt_sources = chain_config.get('mqtt_sources', [])
        postgres_sources = chain_config.get('postgres_sources', [])
        mqtt_targets = chain_config.get('mqtt_targets', [])
        data_chains = chain_config.get('data_processing_chains', [])

    return {
        "mqtt_sources": mqtt_sources,
        "postgres_sources": postgres_sources,
        "mqtt_targets": mqtt_targets,
        "data_chains": data_chains
    }
def map_sources_and_targets_to_chains(configs):
    source_to_chain_map = {}
    target_to_chain_map = {}

    # Zuordnen von Quellen und Zielen zu den Datenverarbeitungsketten
    for chain in configs['data_chains']:
        for step in chain['steps']:
            if 'source_name' in step:
                source_to_chain_map.setdefault(step['source_name'], []).append(chain['name'])
        for target in chain['targets']:
            target_to_chain_map.setdefault(target, []).append(chain['name'])

    # Identifizieren von ungenutzten Quellen und Zielen
    all_sources = {src['name'] for src in configs['mqtt_sources']}
    all_sources.update(src['name'] for src in configs['postgres_sources'])
    unused_sources = all_sources.difference(source_to_chain_map.keys())

    all_targets = {tgt['name'] for tgt in configs['mqtt_targets']}
    unused_targets = all_targets.difference(target_to_chain_map.keys())

    # Loggen der ungenutzten Quellen und Ziele
    if unused_sources:
        logger.warning(f"Unused sources: {unused_sources}. These connections will not be established until mapped in chains.")
    if unused_targets:
        logger.warning(f"Unused targets: {unused_targets}. These connections will not be established until mapped in chains.")

    return source_to_chain_map, target_to_chain_map, unused_sources, unused_targets


async def main():
    logger.info("Validating configurations...")
    chain_config_path = "./configs/chain_config_file.json"
    # Define configuration file paths
    configs: Dict[str, str] = {
        'chain_config': chain_config_path
    }

    # Create JSON file manager for each config
    config_managers: Dict[str, JSONFileManager] = {key: JSONFileManager(path) for key, path in configs.items()}
    validated_data = {key: manager.get_validated_json() for key, manager in config_managers.items()}
    JSONFileManager(chain_config_path)
    new_configs = validate_and_get_configs(config_managers)
    specific_configs = extract_specific_configs(new_configs)
    target_clients = await initialize_used_targets(specific_configs)

    # Starte und überwache die Clients
    await start_and_monitor_clients(target_clients)


if __name__ == '__main__':
    asyncio.run(main())






