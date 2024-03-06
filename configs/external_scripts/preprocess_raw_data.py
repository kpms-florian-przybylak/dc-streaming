import json
import sys
import pandas as pd

# Initialisierung leerer DataFrames für Metadaten, um sie im Speicher zu halten
entities_df = pd.DataFrame()
connections_df = pd.DataFrame()

def update_dataframe_from_json(json_data, df_name):
    """
    Aktualisiert einen globalen DataFrame basierend auf dem übergebenen JSON-Datensatz.
    """
    global entities_df, connections_df

    if df_name == 'entities':
        entities_df = pd.json_normalize(json_data)
    elif df_name == 'connections':
        connections_df = pd.json_normalize(json_data)

def process_input_based_on_structure(input_dict):
    """
    Verarbeitet die übergebene Eingabe basierend auf ihrer Struktur und aktualisiert
    entsprechende globale DataFrames.
    """
    # Bestimmung der Struktur des Eingabe-Dictionarys durch Prüfung der Schlüssel
    for key in input_dict.keys():
        if key.lower().endswith('entities'):
            update_dataframe_from_json(input_dict[key], 'entities')
        elif key.lower().endswith('connections'):
            update_dataframe_from_json(input_dict[key], 'connections')
        else:
            # Hier können Sie anpassen, wie mit anderen Datentypen umgegangen werden soll
            pass

    # Beispiel, wie man die aktualisierten DataFrames nutzen könnte
    # Hier könnte Logik hinzugefügt werden, um die DataFrames für weitere Verarbeitung zu nutzen

def main():
    if len(sys.argv) > 1:
        try:
            input_dict = json.loads(sys.argv[1])
            process_input_based_on_structure(input_dict)
            records = {'success': "Daten erfolgreich verarbeitet."}
        except json.JSONDecodeError:
            records = {'error': "Ungültiges JSON-Format."}
    else:
        records = {'error': "Kein Argument übergeben."}

    # Ausgabe der Verarbeitungsergebnisse als JSON
    print(json.dumps(records, ensure_ascii=False))

if __name__ == "__main__":
    main()
