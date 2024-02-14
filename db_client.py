import asyncio
from sqlalchemy import create_engine, exc, text
from sqlalchemy.orm import sessionmaker
from helpers.custom_logging_helper import logger
from uuid import uuid4



class DBClient:
    def __init__(self, connection_string, retry_limit=-1, retry_interval=10):
        self.connection_string = connection_string
        self.retry_limit = retry_limit
        self.retry_interval = retry_interval
        self.engine = None
        self.session = None
        # Generiere eine eindeutige ID für diese Instanz
        self.instance_id = str(uuid4())

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
                logger.info(f"Database connection verified. Instance ID: {self.instance_id}")
                return
            except exc.SQLAlchemyError as e:
                logger.error(f"Failed to verify database connection: {e}. Instance ID: {self.instance_id}")
                # Führe ein Rollback durch, um sicherzustellen, dass die Transaktion zurückgerollt wird
                if self.session:
                    self.session.rollback()
                if self.retry_limit == -1 or attempt < self.retry_limit - 1:
                    logger.info(f"Retrying to connect after {self.retry_interval} seconds... Instance ID: {self.instance_id}")
                    await asyncio.sleep(self.retry_interval)
                else:
                    logger.error(f"Maximum retry attempts reached. Failed to establish a database connection. Instance ID: {self.instance_id}")
                    raise
            attempt += 1

    async def connect_and_verify(self):
        await self.connect()
        await self.verify_connection_async()

    def execute_query(self, query):
        """
        Executes a SQL query and returns the result.
        """
        if not self.session:
            logger.info("Session is not established. Attempting to reconnect and verify.")
            self.connect_and_verify()

        try:
            result = self.session.execute(query)
            return result
        except exc.SQLAlchemyError as e:
            logger.error(f"Query execution failed: {e}")
            self.session.rollback()
            raise

    def close(self):
        """
        Closes the database connection.
        """
        if self.session:
            self.session.close()
            logger.info("Database connection closed.")
