import json
from datetime import datetime, date, time
from decimal import Decimal

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        elif isinstance(obj, date):
            return obj.isoformat()
        elif isinstance(obj, time):
            return obj.isoformat()
        elif isinstance(obj, Decimal):
            # Konvertiere Decimal zu einem String, um Präzisionsverlust zu vermeiden
            return str(obj)
        # Optional: Behandlung für weitere Datentypen hinzufügen
        return super().default(obj)

def custom_json_dumps(data):
    return json.dumps(data, cls=CustomJSONEncoder)

def custom_json_loads(s):
    def json_decoder(obj):
        for key, value in obj.items():
            try:
                # Versuche, Strings, die datetime Objekte darstellen könnten, zu parsen
                obj[key] = datetime.fromisoformat(value)
            except (TypeError, ValueError):
                # Versuche, Strings, die Decimal Objekte darstellen könnten, zu parsen
                if isinstance(value, str):
                    try:
                        obj[key] = Decimal(value)
                    except ValueError:
                        pass
        return obj

    return json.loads(s, object_hook=json_decoder)