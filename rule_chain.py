class RuleChain:
    def __init__(self, steps):
        self.steps = steps
        self.chain = []
        self.initialize_chain()

    def initialize_chain(self):
        # Initialisieren der Verarbeitungskette basierend auf den übergebenen Schritten
        for step in self.steps:
            if step["type"] == "mqtt_source":
                self.chain.append(self.handle_mqtt_source)
            elif step["type"] == "sql_query":
                self.chain.append(self.handle_sql_query)
            elif step["type"] == "python_script":
                self.chain.append(self.handle_python_script)

    async def execute_chain(self, message):
        # Ausführen der Verarbeitungskette auf die gegebene Nachricht
        for step in self.chain:
            message = await step(message)
        return message

    async def handle_mqtt_source(self, message):
        # Logik für MQTT-Quellen
        return message

    async def handle_sql_query(self, message):
        # Logik für SQL-Abfragen
        return message

    async def handle_python_script(self, message):
        # Logik für die Ausführung eines Python-Skripts
        return message
