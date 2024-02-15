import json
import sys


def process_input(input_arg):
    """
    Verarbeitet die übergebene Eingabe und erstellt ein Dictionary für einen Datensatz,
    der in die test_table eingefügt werden kann.
    """
    # Angenommen, input_arg ist eine kommaseparierte Liste von Werten,
    # die als separate Datensätze eingefügt werden sollen.
    values = input_arg.split(',') if input_arg else []
    records = [{'value': value.strip()} for value in values if value.strip()]

    if records:
        return records
    else:
        return [{'error': "Keine gültigen Daten zum Einfügen."}]


def main():
    if len(sys.argv) > 1:
        input_arg = sys.argv[1]
        # Verarbeitet die Eingabe und erstellt Datensätze für den Bulk Insert
        records = process_input(input_arg)


    else:
        records = [{'error': "Kein Argument übergeben."}]

    # Ausgabe der Datensätze als JSON, vorbereitet für den Bulk Insert
    print(json.dumps(records, ensure_ascii=False))


if __name__ == "__main__":
    main()
