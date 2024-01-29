

"""
Entry Point for the dc-streaming Service
"""
from typing import Optional, Dict, Any

from helpers.custom_logging_helper import logger
from helpers.json_file_manager import JSONFileManager


def validate_and_get_configs(config_managers) -> Optional[Dict[str, Any]]:
    """
    Validates the configuration files for MQTT, OPC, and Handshake.

    Returns:
        Optional[Dict[str, Any]]: Validated configurations or None if any of them are invalid.
    """
    # Validate each configuration using the respective manager
    validated_data = {key: manager.get_validated_json() for key, manager in config_managers.items()}
    # If any configuration is invalid (has value None), log and return None
    if None in validated_data.values():
        logger.danger("Failed to validate configurations.")
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

if __name__ == '__main__':
    logger.info("Validating configurations...")
    chain_config_path = "./configs/chain_config_file.json"
    # Define configuration file paths
    configs: Dict[str, str] = {
        'chain_config': chain_config_path
    }

    # Create JSON file manager for each config
    config_managers: Dict[str, JSONFileManager] = {key: JSONFileManager(path) for key, path in
                                                        configs.items()}
    validated_data = {key: manager.get_validated_json() for key, manager in config_managers.items()}
    JSONFileManager(chain_config_path)
    new_configs = validate_and_get_configs(config_managers)
    specific_configs = extract_specific_configs(new_configs)
    logger.info(specific_configs)