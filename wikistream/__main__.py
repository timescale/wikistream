import asyncio
import aiohttp
import json
import psycopg2
import psycopg2.extras
from aiosseclient import aiosseclient

class Wikistream:
    def __init__(self):
        self.BATCH_SIZE = 50
        self.CONFIG = {
            "host": "localhost",
            "user": "postgres",
            "password": "password",
            "dbname": "wikistream"
        }
        self.record_list = []
        self.connection = None

    def start(self):
        self.setup_database()

        self.connection = psycopg2.connect(
            host=self.CONFIG["host"],
            user=self.CONFIG["user"],
            password=self.CONFIG["password"],
            dbname=self.CONFIG["dbname"]
        )
        cursor = self.connection.cursor()

        try:
            loop = asyncio.get_event_loop()
            loop.run_until_complete(self.stream(cursor))
        finally:
            cursor.close()
            self.connection.close()

    async def stream(self, cursor):
        async for event in aiosseclient('https://stream.wikimedia.org/v2/stream/recentchange'):
            print(event)
            self.save(event, cursor)

    def setup_database(self):
        self.create_database()
        self.create_table()

    def create_database(self):
        connection = psycopg2.connect(
            host=self.CONFIG["host"],
            user=self.CONFIG["user"],
            password=self.CONFIG["password"]
        )
        connection.autocommit = True
        cursor = connection.cursor()

        try:
            dbname = self.CONFIG["dbname"]
            cursor.execute(f"CREATE DATABASE {dbname};")
        except psycopg2.errors.DuplicateDatabase:
            print(f"Database {dbname} already exists.")
        finally:
            cursor.close()
            connection.close()

    def create_table(self):
        connection = psycopg2.connect(
            host=self.CONFIG["host"],
            user=self.CONFIG["user"],
            password=self.CONFIG["password"],
            dbname=self.CONFIG["dbname"]
        )
        cursor = connection.cursor()

        try:
            cursor.execute(f"CREATE TABLE events (time TIMESTAMPTZ NOT NULL, raw JSONB NOT NULL, domain TEXT NOT NULL, wiki_user TEXT NOT NULL, comment_length NUMERIC NOT NULL);")
            cursor.execute("SELECT create_hypertable('events', 'time');")
            cursor.execute("ALTER TABLE events SET (timescaledb.compress, timescaledb.compress_segmentby = 'wiki_user');")
            cursor.execute("SELECT add_compress_chunks_policy('events', INTERVAL '5 minutes');")
            connection.commit()
        except psycopg2.errors.DuplicateTable:
            print("Table events already exists.")
        finally:
            cursor.close()
            connection.close()

    def save(self, event, cursor):
        parsed = json.loads(str(event))

        time = parsed["meta"]["dt"]
        wiki_user = parsed["user"]
        comment_length = len(parsed["comment"])
        domain = parsed["meta"]["domain"]

        record = [time, wiki_user, domain, comment_length, str(event)]
        self.record_list.append(record)

        if len(self.record_list) >= self.BATCH_SIZE:
            print("Saving records...")
            psycopg2.extras.execute_batch(cursor, "INSERT INTO events (time, wiki_user, domain, comment_length, raw) VALUES (%s, %s, %s, %s, %s)", self.record_list)
            self.connection.commit()
            self.record_list = []

ws = Wikistream()
ws.start()
