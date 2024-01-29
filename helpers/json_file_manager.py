import json
from jsonschema import validate, ValidationError
from helpers.custom_logging_helper import logger
import os
from typing import Optional, Dict, Any


def read_json(file_path: str) -> Optional[Dict[str, Any]]:
    """
    Reads a JSON file and returns its contents.

    Args:
        file_path (str): The path to the JSON file.

    Returns:
        Optional[Dict[str, Any]]: The contents of the JSON file or None if an error occurs.
    """
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        logger.error(f"Error reading JSON file: {e}")
        return None


class JSONFileManager:
    def __init__(self, file_path: str) -> None:
        """
        Initializes the JSONFileManager with the provided file path.

        Args:
            file_path (str): The path of the JSON file to be managed.
        """
        self.file_path: str = file_path

    @staticmethod
    def delete_json_file(path: str) -> None:
        """Deletes a JSON file if it exists."""
        if os.path.exists(path) and path.endswith('.json'):
            os.remove(path)
            logger.info(f"File {path} has been deleted.")
        else:
            logger.info(f"File {path} either does not exist or is not a JSON file.")

    @staticmethod
    async def write_json_to_file(data: Dict[str, Any], path: str) -> None:
        """
        Asynchronously writes a JSON object to a file.

        Args:
            data (Dict[str, Any]): The data to be written to the file in JSON format.
            path (str): The path of the file where the data will be written.

        Returns:
            None
        """
        # Open the file in write mode
        with open(path, "w") as config_file:
            # Write the JSON data to the file
            json.dump(data, config_file, indent=4)

        # Log the completion of writing the JSON data to the file
        logger.info(f"JSON data saved to '{path}'.")

    def get_validated_json(self, schema: Optional[Dict[str, Any]] = None) -> Optional[Dict[str, Any]]:
        """
        Retrieves and validates the JSON data against a provided schema.

        Args:
            schema (Optional[Dict[str, Any]]): The JSON schema against which to validate. If not provided, only structure validation is done.

        Returns:
            Optional[Dict[str, Any]]: The validated JSON data or None if invalid.
        """
        data = read_json(self.file_path)
        if data is None:
            logger.error(f"Invalid JSON data structure in file: {self.file_path}")
            return None

        if schema:
            try:
                validate(instance=data, schema=schema)
            except ValidationError as e:
                logger.error(f"Invalid JSON Schema: {e}")
                return None

        return data