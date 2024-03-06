import json
import sys

import redis

class RedisConnection:
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(RedisConnection, cls).__new__(cls)
            cls._instance.connection = redis.Redis(host='192.168.10.100', port=6379, db=0, decode_responses=True)
        return cls._instance

    def get_connection(self):
        return self.connection


def main():
    records = {}
    try:
        redis_connection = RedisConnection().get_connection()
        redis_connection.set('test_key', 'Hello, Redis!')
    except Exception as e:
        if len(sys.argv) > 1:
            try:
                input_dict = sys.argv[1]
                records = input_dict
            except json.JSONDecodeError:
                records = {}
    finally:
        # Ausgabe der Verarbeitungsergebnisse als JSON
        print(records)
if __name__ == "__main__":
    main()
