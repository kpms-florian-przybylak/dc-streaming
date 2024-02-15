import json
import sys


def process_input(input_dict):
    """
    Verarbeitet die übergebene Eingabe als Dictionary und erstellt ein Dictionary für einen Datensatz,
    der in die test_table eingefügt werden kann.
    """
    # Direkte Verarbeitung des Eingabe-Dictionarys
    # Hier könntest du die Logik anpassen, um mit den Werten aus dem Dictionary zu arbeiten
    records = [input_dict]  # Direkte Verwendung des Eingabe-Dictionarys als Datensatz

    if records:
        return records
    else:
        return [{'error': "Keine gültigen Daten zum Einfügen."}]


def main():
    if len(sys.argv) > 1:
        input_dict = sys.argv[1]
        # Verarbeitet die Eingabe und erstellt Datensätze für den Bulk Insert
        records = process_input(input_dict)


    else:
        records = [{'error': "Kein Argument übergeben."}]

    # Ausgabe der Datensätze als JSON, vorbereitet für den Bulk Insert
    print(json.dumps(records, ensure_ascii=False))


if __name__ == "__main__":
    main()
