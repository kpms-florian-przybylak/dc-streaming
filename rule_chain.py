from helpers.custom_logging_helper import logger


class RuleChain:
    def __init__(self, steps):
        self.steps = steps
        self.chain = []
        self.initialize_chain()

    def initialize_chain(self):
        # Initialisieren der Verarbeitungskette basierend auf den übergebenen Schritten
        for step in self.steps:
            print("step", step)

    async def execute_chain(self, message):
        # Ausführen der Verarbeitungskette auf die gegebene Nachricht
        for step in self.chain:
            message = await step(message)
        return message

    async def handle_incoming_message(self, message, client_id):
        logger.info(f"Received message from client: {client_id} on topic {message.topic}: {message.payload.decode()}")
        return message

    async def handle_processing_message(self, message):
        print("processing", message)
        return message
    async def handle_outgoing_message(self, message):
        print("out", message)
        return message