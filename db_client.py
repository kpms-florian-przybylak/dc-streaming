import asyncio
import json

import asyncpg

from sqlalchemy import create_engine, exc, text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import sessionmaker

from helpers.custom_json_encoder import custom_json_dumps
from helpers.custom_logging_helper import logger
from uuid import uuid4

# TODO: add db target
# TODO: Versionierung
# TODO:  grouplogik implementieren
# TODO: Testen
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

    async def create_trigger(self, trigger_config):
        try:
            # Erstellen der Trigger-Funktion
            create_function_sql = text(f"""
            CREATE OR REPLACE FUNCTION notify_{trigger_config['trigger_name']}()
            RETURNS TRIGGER AS $$
            BEGIN
                IF ({trigger_config['condition']}) THEN
                    PERFORM pg_notify('{trigger_config['trigger_name']}', row_to_json(NEW)::text);
                END IF;
                RETURN NEW;
            END;
            $$ LANGUAGE plpgsql;
            """)

            # Erstellen des Triggers
            create_trigger_sql = text(f"""
            DROP TRIGGER IF EXISTS {trigger_config['trigger_name']}_trigger ON {trigger_config['table']};
            CREATE TRIGGER {trigger_config['trigger_name']}_trigger
            AFTER INSERT OR UPDATE ON {trigger_config['table']}
            FOR EACH ROW EXECUTE FUNCTION notify_{trigger_config['trigger_name']}();
            """)
            print(create_trigger_sql, create_function_sql)
            # Ausführen der SQL-Befehle über die SQLAlchemy-Engine
            with self.engine.begin() as conn:
                conn.execute(create_function_sql)
                conn.execute(create_trigger_sql)
                logger.info(f"Trigger {trigger_config['trigger_name']} created on {trigger_config['table']}.")

        except SQLAlchemyError as e:
            logger.error(f"Failed to create trigger {trigger_config['trigger_name']} on {trigger_config['table']}: {e}")

    async def trigger_exists(self, trigger_name, table_name):
        check_trigger_sql = text(f"""
        SELECT EXISTS (
            SELECT 1
            FROM pg_trigger
            WHERE NOT tgisinternal
            AND tgname = '{trigger_name}_trigger'
        );
        """)
        result = self.session.execute(check_trigger_sql)
        return result.scalar()

    async def listen_to_notifications(self, trigger_name, processing_chain):
        """
        Modified to accept a processing_chain parameter and use a dynamically created handler.
        """
        try:
            conn = await asyncpg.connect(self.connection_string)

            async def notification_handler(conn, pid, channel, payload):
                """
                Handle notifications with access to the processing_chain.
                """
                try:
                    notification_data = json.loads(payload)
                    logger.info(f"Notification received on channel {channel}: {notification_data}")
                    # Pass the data to the processing chain
                    await processing_chain.process_step(notification_data, self.client_id)
                except json.JSONDecodeError:
                    logger.error(f"Error decoding JSON from notification on channel {channel}")
                except Exception as e:
                    logger.error(f"Error handling notification from {channel}: {e}")

            # Use the custom notification_handler that captures processing_chain
            await conn.add_listener(trigger_name, notification_handler)
            logger.info(f"DB Client subscribed to {trigger_name} notifications.")

            try:
                await asyncio.Future()  # Run indefinitely until cancelled or an error occurs
            finally:
                await conn.close()
                logger.info("DB connection closed.")
        except Exception as e:
            logger.error(f"Failed to establish connection for listening to notifications: {e}")



    async def listen_for_triggers(self, trigger_config, processing_chain):
        if not self.engine:
            logger.info("Engine is not established. Attempting to reconnect and verify.")
            await self.connect()

        trigger_name = trigger_config['trigger_name']
        table_name = trigger_config['table']

        # Überprüfe, ob der Trigger bereits existiert
        exists = await self.trigger_exists(trigger_name, table_name)
        if not exists:
            logger.info(f"Trigger {trigger_name} does not exist. Creating now.")
        else:
            logger.info(f"Trigger {trigger_name} already exists. Recreaing...")
        await self.create_trigger(trigger_config)

        await self.listen_to_notifications(trigger_name, processing_chain)

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
                #logger.debug(f"Row data: {row}")
                try:
                    # Convert the row to a dictionary
                    row_dict = row._asdict()
                    #logger.debug(f"Row as dict: {row_dict}")
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

    async def execute_bulk_insert(self, insert_statement, data, batch_size=100):
        try:
            # Aufteilen der Daten in Batches
            for i in range(0, len(data), batch_size):
                batch_data = data[i:i + batch_size]
                # Bereite eine Liste von Statements vor, die ausgeführt werden sollen
                statements = [text(insert_statement).bindparams(**record) for record in batch_data]
                # Ausführen der Statements in einem Batch
                self.session.execute(text("BEGIN"))  # Starte eine Transaktion, falls nicht automatisch verwaltet
                for stmt in statements:
                    self.session.execute(stmt)
                self.session.commit()  # Commit nach dem Ausführen der Batches
                logger.info(f"Bulk insert completed for {len(batch_data)} records.")
        except Exception as e:
            logger.error(f"Failed to execute bulk insert: {e}")
            self.session.rollback()  # Rollback im Fehlerfall

    def close(self):
        """
        Closes the database connection.
        """
        if self.session:
            self.session.close()
            logger.info("Database connection closed.")
