from helpers.custom_json_encoder import custom_json_dumps, custom_json_loads
from helpers.custom_logging_helper import logger


async def get_group_data_initial(redis_client, db_client):
    """
    This function is intended to be called once to initialize the cache with group data.
    """
    cache_key = "groups_data"
    try:
        query = "SELECT entity_object_id, group_id FROM data_pipeline.entity_objects WHERE group_id IS NOT NULL"
        result_stream = db_client.execute_query(query)
        groups_data = [result for result in result_stream]

        # Cache each group data individually
        for group in groups_data:
            group_id = group['group_id']
            entity_object_id = group['entity_object_id']
            # Assuming each entity_object_id is unique and can be used as a Redis key
            await redis_client.set(f"group_data:{entity_object_id}", custom_json_dumps(group_id))

        logger.info("Initial group data loaded into cache.")
    except Exception as e:
        logger.error(f"Error initializing group data cache: {e}")
        raise


async def update_group_data_cache(redis_client, entity_object_id, group_id):
    """
    Updates or removes the cache for a specific group entry depending on whether group_id is null.
    """
    try:
        if group_id is not None:
            # Update the cache with the new group_id
            redis_client.set(f"group_data:{entity_object_id}", custom_json_dumps(group_id))
            logger.info(f"Cache updated for entity_object_id {entity_object_id} with group_id {group_id}.")
        else:
            # Remove the key from the cache if group_id is null
            redis_client.delete(f"group_data:{entity_object_id}")
            logger.info(f"Cache entry removed for entity_object_id {entity_object_id} as group_id is null.")
    except Exception as e:
        logger.error(f"Error updating/removing group data cache for entity_object_id {entity_object_id}: {e}")


async def initialize_group_data(redis_client, db_client):
        cache_key = "groups_data"
        groups_data = None
        try:
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                try:
                    groups_data = custom_json_loads(cached_data)
                    #logger.debug("Retrieved group data from cache.")
                    #logger.debug(f"Cached groups data: {groups_data}")
                except Exception as e:
                    logger.error(f"Error decoding cached groups data: {e}")
                    groups_data = None

            if not cached_data or groups_data is None:
                try:
                    query = "SELECT entity_object_id, group_id FROM data_pipeline.entity_objects WHERE group_id IS NOT NULL"
                    result_stream = db_client.execute_query(query)
                    groups_data = [result for result in result_stream]

                    #logger.debug(f"Group meta data before caching: {len(groups_data)}")
                    # Inside get_group_data, after fetching data from the database
                    for group in groups_data:
                        group_id = group['group_id']
                        entity_object_id = group['entity_object_id']
                        redis_client.set(entity_object_id, custom_json_dumps(group_id))
                    logger.info("Retrieved group data from database and updated cache.")
                except Exception as e:
                    logger.error(f"Error fetching group data from database: {e}")
                    raise
        except Exception as e:
            logger.error(f"Unexpected error in get_group_data: {e}")
            raise
        logger.info("Initial group data load and cache update completed.")
async def update_last_section_id_in_redis(redis_client, db_client):
    """
    Fetches the last section ID from the database using DBClient and updates a Redis entry with it.
    This function handles the case where the table might be empty.
    """
    # Define the query to fetch the last section ID
    query = "SELECT MAX(id) as last_section_id FROM public.section"

    try:
        result_stream = db_client.execute_query(query)
        result = next(result_stream, None)  # Attempt to fetch the first (and only) row from the generator

        if result is not None and result['last_section_id'] is not None:
            last_section_id = result['last_section_id']
            # Update Redis asynchronously with the last section ID
            await redis_client.set("last_section_id", custom_json_dumps(last_section_id))
            logger.info(f"Updated Redis with the last section ID: {last_section_id}")
        else:
            # The table might be empty or the query failed to fetch the expected result
            # Decide on your handling here. For example, logging and setting a placeholder or skipping.
            logger.info("The section table is empty or the last section ID could not be determined.")
            # Optional: Set a placeholder or default value in Redis if needed
            # await redis_client.set("last_section_id", "default_value")
    except Exception as e:
        logger.error(f"Error updating last section ID in Redis: {e}")




async def initialize(clients):
    db_client = clients['db1']
    batchdata_db_client = clients["db2"]
    redis_client = clients['redis1']
    await initialize_group_data(redis_client, db_client)
    await update_last_section_id_in_redis(redis_client, batchdata_db_client)
    return
async def process_message(input_message, clients):
    """
    Processes the incoming message, updating product data keys based on Redis values
    for existing keys only. Items with keys not found in Redis are not included in the output.
    """
    redis_client = clients['redis1']
    topic = input_message.get("topic", None)  # Extrahiert das Topic aus der eingehenden Nachricht

    if topic == 'sectiondata':
        payload = input_message.get("data")
        section_id = payload.get("id")
        if section_id is not None:
            redis_client.set("current_section_id", custom_json_dumps(section_id))
            logger.info(f"Section ID {section_id} set as current_section_id in Redis.")



    if "trigger_message" in input_message:
        product_data  = input_message["trigger_message"]
        await update_group_data_cache(redis_client, product_data ["entity_object_id"], product_data ["group_id"])
        logger.info("Processed database change trigger message.")
    else:
        payload = input_message.get("data", {})

        product_data = payload

        # Prepare a new product data list to hold updated items where keys exist in Redis
        updated_product_data = []

        # Iterate through each item in product data
        for item in product_data.get("data",{}):
            if not isinstance(item, dict):
                logger.error(f"Unexpected item format in product_data: {item}, {product_data}")
                continue
            for key, value in item.items():
                # Attempt to retrieve a new key from Redis using the original key
                new_key = await redis_client.get(key)
                if new_key:
                    new_key_decoded = custom_json_loads(new_key)  # Assuming the stored value is JSON serialized
                    # Add the item with the new key to the updated product data
                    updated_product_data.append({new_key_decoded: value})

        # Only include items in the output where keys were found in Redis
        input_message['data'] = updated_product_data

        #logger.debug("Processed product data with keys updated to existing Redis mappings.")

    return input_message