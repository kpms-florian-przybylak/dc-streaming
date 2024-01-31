

"""
Entry Point for the dc-streaming Service
"""
import asyncio
from typing import  Dict

from client_manager import ClientManager
from config_manager import validate_and_get_configs, extract_specific_configs
from helpers.custom_logging_helper import logger
from helpers.json_file_manager import JSONFileManager





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
    if new_configs:
        logger.info("New configurations loaded successfully.")
        specific_configs = extract_specific_configs(new_configs)
        #arget_clients = await initialize_used_targets(specific_configs)
        print(specific_configs)
        # Starte und überwache die Clients
        #await start_and_monitor_clients(target_clients)

        client_manager = ClientManager(specific_configs)
        #await client_manager.initialize_and_run_clients()


    else:
        logger.error("Failed to load new configurations.")
if __name__ == '__main__':
    asyncio.run(main())






