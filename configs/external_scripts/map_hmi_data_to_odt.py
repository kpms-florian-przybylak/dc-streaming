import json
import sys


def process_input(input_arg):
    """
    Verarbeitet die übergebene Eingabe und gibt ein Ergebnis als Dictionary zurück.
    """
    output = {}
    if input_arg:
        output['message'] = f"{input_arg}"
    else:
        output['error'] = "Kein Argument übergeben."
    return output


def main():
    if len(sys.argv) > 1:
        input_arg = sys.argv[1]
        # Verarbeitet die Eingabe und gibt das Ergebnis zurück
        output = process_input(input_arg)
    else:
        output = {"error": "Kein Argument übergeben."}

    # Ausgabe als JSON
    print(json.dumps(output))


if __name__ == "__main__":
    main()
