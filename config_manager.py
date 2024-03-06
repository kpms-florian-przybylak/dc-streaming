from typing import Optional, Dict, Any

from helpers.custom_logging_helper import logger
from helpers.json_file_manager import JSONFileManager

def load_and_validate_configs(config_path: str):
    """
    Lädt und validiert die Konfigurationen aus der angegebenen Datei.
    """
    logger.info(f"Loading configurations from {config_path}.")
    config_manager = JSONFileManager(config_path)
    validated_data = config_manager.get_validated_json()
    if validated_data is None:
        logger.error("Failed to validate configurations.")
        return None
    logger.info("Configurations validated successfully.")
    return validated_data

def extract_specific_configs(validated_data):
    if not validated_data:
        return None
    mqtt_clients = []
    postgres_clients = []
    redis_clients = []
    valid_data_processing_chains = []

    chain_config = validated_data.get('chain_config')
    if chain_config:
        mqtt_clients = chain_config.get('mqtt_clients', [])
        postgres_clients = chain_config.get('postgres_clients', [])
        redis_clients = chain_config.get('redis_clients', [])
        data_processing_chains = chain_config.get('data_processing_chains', [])

        # Filter chains that have at least one source and one target
        for chain in data_processing_chains:
            if chain.get('sources') and chain.get('targets'):
                valid_data_processing_chains.append(chain)

    return {
        "mqtt_clients": mqtt_clients,
        "postgres_clients": postgres_clients,
        "redis_clients": redis_clients,
        "data_processing_chains": valid_data_processing_chains
    }

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

    # Ensure at least one source and one target are defined in the chain_config, if required
    return validated_data
