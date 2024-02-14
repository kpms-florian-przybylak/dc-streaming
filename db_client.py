import asyncio
import json

from sqlalchemy import create_engine, exc, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from helpers.custom_json_encoder import custom_json_dumps
from helpers.custom_logging_helper import logger
from uuid import uuid4



class DBClient:
    def __init__(self, client_id, connection_string, retry_limit=-1, retry_interval=10):
        self.connection_string = connection_string
        self.retry_limit = retry_limit
        self.retry_interval = retry_interval
        self.engine = None
        self.session = None
        self.client_id = client_id or str(uuid4())
        # Generiere eine eindeutige ID für diese Instanz

    async def connect(self):
        self.engine = create_engine(self.connection_string)
        Session = sessionmaker(bind=self.engine)
        self.session = Session()
    async def start_periodic_verification(self, interval=30):
        """
        Starts periodic verification of the database connection.
        :param interval: Interval in seconds between verifications.
        """
        while True:
            try:
                await self.verify_connection_async()
                logger.info("Connection verification successful.")
            except Exception as e:
                logger.error(f"Connection verification failed: {e}")
            await asyncio.sleep(interval)
    async def verify_connection_async(self):
        attempt = 0
        while self.retry_limit == -1 or attempt < self.retry_limit:
            try:
                # Versuche, eine einfache Abfrage auszuführen, um die Verbindung zu überprüfen
                self.session.execute(text("SELECT 1"))
                logger.info(f"Database connection verified. Client ID: {self.client_id}")
                return
            except exc.SQLAlchemyError as e:
                logger.error(f"Failed to verify database connection: {e}. Client ID: {self.client_id}")
                # Führe ein Rollback durch, um sicherzustellen, dass die Transaktion zurückgerollt wird
                if self.session:
                    self.session.rollback()
                if self.retry_limit == -1 or attempt < self.retry_limit - 1:
                    logger.info(f"Retrying to connect after {self.retry_interval} seconds... Client ID: {self.client_id}")
                    await asyncio.sleep(self.retry_interval)
                else:
                    logger.error(f"Maximum retry attempts reached. Failed to establish a database connection. Client ID: {self.client_id}")
                    raise
            attempt += 1

    async def connect_and_verify(self):
        await self.connect()
        await self.verify_connection_async()

    def execute_query(self, query):
        """
        Executes a SQL query and streams the results.
        """
        if not self.session:
            logger.info("Session is not established. Attempting to reconnect and verify.")
            self.connect_and_verify()

        try:
            # Verwende `yield_per` für Streaming
            result_proxy = self.session.execute(text(query)).yield_per(100)  # Anpassen nach Bedarf
            for row in result_proxy:
                logger.debug(f"Row data: {row}")
                try:
                    # Convert the row to a dictionary
                    row_dict = row._asdict()
                    logger.debug(f"Row as dict: {row_dict}")
                    yield row_dict
                except Exception as e:
                    logger.error(f"Error converting row to dict: {e}")

        except SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            self.session.rollback()
            raise

    async def start_polling_query(self, query, polling_interval, processing_chain):
        while True:
            try:
                result_stream = self.execute_query(query)
                for result in result_stream:
                    # Verwende den angepassten Encoder für die JSON-Serialisierung
                    json_result = custom_json_dumps(result)
                    await processing_chain.process_step(json_result, self.client_id)
                logger.info(f"Polling query executed: {query}")
            except Exception as e:
                logger.error(f"Failed to execute polling query: {e}")
            await asyncio.sleep(polling_interval)
    def close(self):
        """
        Closes the database connection.
        """
        if self.session:
            self.session.close()
            logger.info("Database connection closed.")
