import asyncio

import redis
import time
from helpers.custom_logging_helper import logger

class RedisClient:

    def __init__(self, client_id, host='localhost', port=6379, db=0, password=None, max_retries=-1, retry_delay=1, check_interval=10):
        """
        Initializes a new instance of the RedisClient.

        :param client_id: An identifier for the Redis client instance.
        :param host: The hostname of the Redis server. Defaults to 'localhost'.
        :param port: The port of the Redis server. Defaults to 6379.
        :param db: The database number. Defaults to 0.
        :param password: The password for authenticating with Redis. Defaults to None.
        :param max_retries: Maximum number of retry attempts to connect. -1 for infinite retries.
        :param retry_delay: Delay between retry attempts in seconds.
        :param check_interval: How often to check the connection in seconds.
        """
        self.client_id = client_id
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.check_interval = check_interval
        self.connection = None
        self.loop = asyncio.get_event_loop()
        self.ensure_connection()
        self.loop.create_task(self.connection_check_loop())

    async def connection_check_loop(self):
        """
        Periodically checks the Redis connection and tries to reconnect if necessary.
        """
        while True:
            await asyncio.sleep(self.check_interval)
            if not self.connection or not self.ping():
                logger.warning(f"Redis client '{self.client_id}': connection lost. Attempting to reconnect.")
                self.ensure_connection()

    def ensure_connection(self):
        """
        Ensures that the client is connected to the Redis database, retrying if necessary.
        """
        attempt = 0
        while self.max_retries == -1 or attempt < self.max_retries:
            if self.connect():
                return
            attempt += 1 if self.max_retries != -1 else 0
            logger.error(f"Failed to reconnect Redis client '{self.client_id}'. Attempt {attempt} of {self.max_retries if self.max_retries != -1 else 'infinite'}. Retrying in {self.retry_delay} seconds.")
            time.sleep(self.retry_delay)
        if self.max_retries != -1:
            logger.error(f"Could not reconnect Redis client '{self.client_id}' after several attempts.")

    def connect(self):
        """
        Attempts to establish a connection to the Redis database.
        """
        try:
            self.connection = redis.Redis(host=self.host, port=self.port, db=self.db, password=self.password, decode_responses=True)
            if self.ping():
                logger.success(f"Successfully connected Redis client '{self.client_id}'.")
                return True
        except redis.exceptions.ConnectionError:
            logger.error(f"Failed to connect Redis client '{self.client_id}'.")
        return False

    def ping(self):
        """
        Checks if the Redis connection is alive.
        """
        try:
            self.connection.ping()
            return True
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError):
            return False
    def set(self, key, value):
        """
        Sets a value in Redis under a specified key.
        """
        if self.connection:
            try:
                self.connection.set(key, value)
                logger.info(f"Value '{value}' was stored under the key '{key}'.")
            except Exception as e:
                logger.error(f"Error saving the value: {e}")
        else:
            logger.error("Unable to store the value, as connection to Redis is not available.")

    async def get(self, key):
        """
        Retrieves a value from Redis by a specified key asynchronously and ensures the method is always awaitable.
        """
        if not self.connection:
            logger.error("Unable to retrieve the value, as connection to Redis is not available.")
            return None

        try:
            value = self.connection.get(key)
            if value is not None:
                logger.info(f"Value under the key '{key}': {value}")
            else:
                logger.info(f"Key '{key}' does not exist in Redis.")
        except Exception as e:
            logger.error(f"Error retrieving the value: {e}")
            value = None

        # Der Trick hier ist, `asyncio.sleep(0)` zu nutzen, um sicherzustellen,
        # dass die Methode eine Coroutine bleibt, unabhängig vom Wert von `value`.
        await asyncio.sleep(0)
        return value
