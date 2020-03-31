# Wikistream

A Python package that uses [`aiosseclient`](https://github.com/ebraminio/aiosseclient) from stream edits to Wikimedia properties around the world and store them in [TimescaleDB](https://timescale.com).

This package assumes that you're accessing a PostgreSQL database on your local
machine that already has the TimescaleDB extension installed and enabled. [TimescaleDB Docker Image](https://hub.docker.com/r/timescale/timescaledb/)

It also assumes that said database has a user `postgres` with a password of `password`. If you're using the aforementioned Docker image to setup TimescaleDB you can set the password with an environment variable:

```
POSTGRES_PASSWORD=password
```
You don't need to do anything to set the user to `postgres`, it's already the default.

## That's a lot of assumptions, seems pretty inflexible.

You're right, it is. This is intended for use with a TimescaleDB demo application
and is probably not fit for much else. You're of course welcome to the code anyway,
perhaps you will find something useful.

## Usage
Create a Wikistream:

```python
ws = Wikistream()

# default config:
# host: localhost
# port: 5432
# user: postgres
# password: password
# dbname: wikistream
# table: events
```

Start it:

```python
ws.start()
```

Alternatively pass in your own artisanal handcrafted configuration fancy-pants:

```python
config = {
    "host": "weird_host",
    "port": "port_with_likely_conflicts",
    "user": "confusing_pg_username",
    "password": "easily_guessed_password",
    "dbname": "something_less_cool_than_wikistream_prolly",
    "table": "arbitrary and ambiguous table name"
}

ws = Wikistream(config)
ws.start()
```

## Batching
Rather than insert each record individually Wikistream will save up 50 at a time
and use `execute_batch` to insert them all at once. You can change the batch size
in the config:

```python
config = {
    "batch": 50
}
```

## Do overs
Occasionally Wikistream has trouble streaming new events from the Wikimedia service
that's providing our stream ([https://stream.wikimedia.org/v2/stream/recentchange](https://stream.wikimedia.org/v2/stream/recentchange))
because the internet is a hellscape.

In the unlikely event of a water landing and/or connection error Wikistream will take a brief nap,
or rather a series of naps with decreasing brevity, before surrendering to the will of the tubes.

You can configure sleepy time by providing an array of sleep durations in the config:

```python
config = {
    "retries": [0, 1, 1, 2, 3, 5, 8, 13, 21, 34]
}

# these are the default values, durations in seconds
```

## Logging
Wikistream provides 5 log levels of increasing severity: `debug`, `info`, `warn`, `error` and `fatal`.

You can set `log_level` in your config to see all messages at that level or greater:

```python
config = {
    "log_level": "info"
    
    # the default of 'info' will show all of the log messages except 'debug'
    # a log_level of 'error' would only show logs with level 'error' and 'fatal'
}
```

## License
MIT
Copyright (c) 2020 Jonan Scheffler ([LICENSE](https://github.com/timescale/wikistream/LICENSE.md))
