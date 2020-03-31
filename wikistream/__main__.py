import asyncio
import aiohttp
import sqlalchemy

import re
import json
import time
import datetime
from datetime import datetime, timezone

class Wikistream:
    def __init__(self, config={}):
        self.config = {
            "host": "localhost",
            "port": 5432,
            "user": "postgres",
            "password": "password",
            "dbname": "wikistream",
            "table": "events",
            "batch": 50,
            "retries": [0, 1, 1, 2, 3, 5, 8, 13, 21, 34],
            "log_level": "info"
        }
        self.config.update(config)
        self.log_levels = ["debug", "info", "warn", "error", "fatal"]
        self.wikimedia_url = "https://stream.wikimedia.org/v2/stream/recentchange"
        self.database_url = f"postgresql://{self.config['user']}:{self.config['password']}@{self.config['host']}:{self.config['port']}/{self.config['dbname']}"
        self.safe_database_url = re.sub(self.config["password"], "********", self.database_url)
        self.engine = sqlalchemy.create_engine(self.database_url, executemany_mode="batch")

        self.queued_events = []
        self.table = None

    def start(self):
        self.create_table()
        self.log("info", f"Connected to {self.database_url}")
        self.log("info", f"Streaming from {self.wikimedia_url}...")

        while True:
            for attempt, duration in enumerate(self.config["retries"]):
                attempt_time = time.time()
                try:
                    self.log("debug", f"Starting the run loop... {attempt}/{len(self.config['retries'])}")
                    self.run_loop()
                except asyncio.TimeoutError:
                    self.log("warn", f"Timed out attempting to stream from '{self.wikimedia_url}'. Retrying in {duration} seconds... ({attempt}/{len(self.config['retries'])})")
                    time.sleep(duration)
                    error_time = time.time()
                    if ((error_time - attempt_time) > max(self.config["retries"]) + 1):
                        self.log("debug", f"Resetting retry counter after {error_time - attempt_time} seconds.")
                        break
                    else:
                        self.log("debug", f"Retrying after {error_time - attempt_time} seconds...")
            else:
                self.log("fatal", f"Timed out {len(self.config['retries'])} times trying to stream from '{self.wikimedia_url}'.")
                break

    def run_loop(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.stream())

    async def stream(self):
        async for event in self.aiosseclient(self.wikimedia_url):
            self.save(event)

    async def aiosseclient(self, url, last_id=None, **kwargs):
        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        # The SSE spec requires making requests with Cache-Control: nocache
        kwargs['headers']['Cache-Control'] = 'no-cache'

        # The 'Accept' header is not required, but explicit > implicit
        kwargs['headers']['Accept'] = 'text/event-stream'
        
        if last_id:
            kwargs['headers']['Last-Event-ID'] = last_id
        
        async with aiohttp.ClientSession() as session:
            response = await session.get(url, **kwargs)
            lines = []
            async for line in response.content:
                line = line.decode('utf8')

                if line == '\n' or line == '\r' or line == '\r\n':
                    if lines[0] == ':ok\n':
                        lines = []
                        continue

                    yield Event.parse(''.join(lines))
                    lines = []
                else:
                    lines.append(line)

    def create_table(self):
        metadata = sqlalchemy.MetaData()
        self.table = sqlalchemy.Table(self.config["table"], metadata,
            sqlalchemy.Column("time", sqlalchemy.DateTime, nullable=False),
            sqlalchemy.Column("wiki_user", sqlalchemy.Text, nullable=False),
            sqlalchemy.Column("comment_length", sqlalchemy.Integer, nullable=False),
            sqlalchemy.Column("domain", sqlalchemy.Text, nullable=False),
            sqlalchemy.Column("raw", sqlalchemy.dialects.postgresql.JSONB, nullable=False),
        )
        metadata.create_all(self.engine)

        with self.engine.connect() as connection:
            table = self.config["table"]

            try:
                connection.execute(f"SELECT create_hypertable('{table}', 'time');")
            except sqlalchemy.exc.DatabaseError as error:
                if "already a hypertable" not in str(error):
                    self.log("error", f"Error attempting to convert '{table}' into a hypertable.", { "error": str(error), "db": self.safe_database_url })
                    raise error
                else:
                    self.log("debug", f"Table '{table}' is already a hypertable.", { "db": self.safe_database_url })
            try:
                connection.execute(f"ALTER TABLE {table} SET (timescaledb.compress, timescaledb.compress_segmentby = 'wiki_user');")
            except sqlalchemy.exc.NotSupportedError as error:
                if "compressed chunks already exist" not in str(error):
                    self.log("error", f"Error attempting to alter hypertable '{table}' for compression.", { "error": str(error), "db": self.safe_database_url })
                    raise error
                else:
                    self.log("debug", f"Table '{table}' already contains compressed chunks so it could not be altered for compression.")
            try:
                connection.execute(f"SELECT add_compress_chunks_policy('{table}', INTERVAL '10 minutes');")
            except sqlalchemy.exc.ProgrammingError as error:
                if "compress chunks policy already exists" not in str(error):
                    self.log("error", f"Error attempting to create a compress chunks policy for '{table}' for compression.", { "error": str(error), "db": self.safe_database_url })
                    raise error
                else:
                    self.log("debug", f"Table {table} already has a compress chunks policy.", { "db": self.safe_database_url })

    def save(self, event):
        parsed = json.loads(str(event))

        new_event = {
            "time": parsed["meta"]["dt"],
            "wiki_user": parsed["user"],
            "comment_length": len(parsed["comment"]),
            "domain": parsed["meta"]["domain"],
            "raw": str(event)
        }

        self.queued_events.append(new_event)

        if (len(self.queued_events) > self.config["batch"]):
            self.log("debug", f"Inserting {len(self.queued_events)} events...", { "insert_count": len(self.queued_events), "db": self.safe_database_url })
            with self.engine.connect() as connection:
                connection.execute(self.table.insert(), self.queued_events)
                self.queued_events = []
        else:
            self.log("debug", f"Queueing event...", { "queued_event_count": len(self.queued_events), "event": parsed })

    def log(self, level, message, data={}):
        if self.log_levels.index(level) >= self.log_levels.index(self.config["log_level"]):
            log_info = {
                "level": level,
                "time": str(datetime.now(timezone.utc)),
                "tag": "wikistream",
                "message": message
            }
            log_info.update(data)
            print(json.loads(json.dumps(log_info)))

class Event(object):

    sse_line_pattern = re.compile('(?P<name>[^:]*):?( ?(?P<value>.*))?')

    def __init__(self, data='', event='message', id=None, retry=None):
        self.data = data
        self.event = event
        self.id = id
        self.retry = retry

    def dump(self):
        lines = []
        if self.id:
            lines.append('id: %s' % self.id)

        # Only include an event line if it's not the default already.
        if self.event != 'message':
            lines.append('event: %s' % self.event)

        if self.retry:
            lines.append('retry: %s' % self.retry)

        lines.extend('data: %s' % d for d in self.data.split('\n'))
        return '\n'.join(lines) + '\n\n'

    @classmethod
    def parse(cls, raw):
        """
        Given a possibly-multiline string representing an SSE message, parse it
        and return a Event object.
        """
        msg = cls()
        for line in raw.splitlines():
            m = cls.sse_line_pattern.match(line)
            if m is None:
                # Malformed line.  Discard but warn.
                warnings.warn('Invalid SSE line: "%s"' % line, SyntaxWarning)
                continue

            name = m.group('name')
            if name == '':
                # line began with a ":", so is a comment.  Ignore
                continue
            value = m.group('value')

            if name == 'data':
                # If we already have some data, then join to it with a newline.
                # Else this is it.
                if msg.data:
                    msg.data = '%s\n%s' % (msg.data, value)
                else:
                    msg.data = value
            elif name == 'event':
                msg.event = value
            elif name == 'id':
                msg.id = value
            elif name == 'retry':
                msg.retry = int(value)

        return msg

    def __str__(self):
        return self.data
