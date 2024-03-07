from helpers.custom_json_encoder import custom_json_dumps, custom_json_loads
from helpers.custom_logging_helper import logger


async def get_group_data(redis_client, db_client):
    cache_key = "groups_data"
    groups_data = None
    try:
        cached_data = await redis_client.get(cache_key)
        print("cached_data", cached_data)
        if cached_data:
            try:
                groups_data = custom_json_loads(cached_data)
                logger.info("Retrieved group data from cache.")
                logger.debug(f"Cached groups data: {groups_data}")
            except Exception as e:
                logger.error(f"Error decoding cached groups data: {e}")
                groups_data = None

        if not cached_data or groups_data is None:
            try:
                query = "SELECT entity_object_id, group_id FROM data_pipeline.entity_objects WHERE group_id IS NOT NULL"
                result_stream = db_client.execute_query(query)
                groups_data = [result for result in result_stream]

                logger.debug(f"Group meta data before caching: {len(groups_data)}")
                # Inside get_group_data, after fetching data from the database
                for group in groups_data:
                    group_id = group['group_id']
                    entity_object_id = group['entity_object_id']
                    redis_client.set(entity_object_id, custom_json_dumps(group_id))
                logger.info("Retrieved group data from database and updated cache.")
                logger.info("Retrieved group data from database and updated cache.")
            except Exception as e:
                logger.error(f"Error fetching group data from database: {e}")
                raise
    except Exception as e:
        logger.error(f"Unexpected error in get_group_data: {e}")
        raise




async def invalidate_group_data_cache(redis_client):
    cache_key = "groups_data"
    await redis_client.delete(cache_key)
    logger.info("Group data cache invalidated.")


async def process_message(input_message, clients):
    """
    Processes the incoming message and caches it using a Redis client.
    Assumes 'redis1' is the key for accessing the Redis client in the 'clients' dict.
    """
    # Zugriff auf den Redis-Client, der über die 'clients' übergeben wurde
    redis_client = clients['redis1']
    db_client = clients['db1']

    # Beispielhaftes Caching der Nachricht unter dem Schlüssel 'latest_message'
    #await redis_client.set('latest_message', input_message)
    logger.info("Cached the latest message.")

    # Abfrage und Zwischenspeichern der Gruppendaten unter Verwendung der optimierten Funktion
    groups_data = await get_group_data(redis_client, db_client)
    return input_message
