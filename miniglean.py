#!/usr/bin/env -S uv run --script

import sqlite3
import json

GLEAN_DB = sqlite3.connect("glean.db")
GLEAN_DB.execute('pragma journal_mode=wal')
GLEAN_DB.execute("""
CREATE TABLE IF NOT EXISTS telemetry(
  id TEXT NOT NULL,
  ping TEXT NOT NULL,
  lifetime TEXT NOT NULL,
  labels TEXT NOT NULL, -- can't be null or ON CONFLICT won't work
  value BLOB NOT NULL,
  updated_at TEXT NOT NULL DEFAULT (DATETIME('now')),
  UNIQUE(id, ping, labels)
)
""")
GLEAN_DB.commit()

class Metric:
    name: str
    lifetime: str
    send_in_pings: [str]
    label: str

    def __init__(self, name, lifetime, send_in_pings=None):
        self.name = name
        self.lifetime = lifetime
        self.send_in_pings = send_in_pings or ["metrics"]
        self.label = None

    def record(self, fn):
        global GLEAN_DB
        pings = self.send_in_pings

        labels = ''
        if self.label:
            labels = self.label

        cur = GLEAN_DB.cursor()
        for ping in pings:
            print(f"Recording for {self.name} in {ping}")
            value = cur.execute("SELECT value FROM telemetry WHERE id = ?1 AND ping = ?2 AND labels = ?3", [self.name, ping, labels]).fetchone()
            newvalue = fn(value and value[0])
            cur.execute("""
                INSERT INTO telemetry (id, ping, lifetime, labels, value, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5, DATETIME('now'))
                ON CONFLICT(id, ping, labels) DO UPDATE SET lifetime = excluded.lifetime, value = excluded.value, updated_at = excluded.updated_at
            """, [
                self.name,
                ping,
                self.lifetime,
                labels,
                newvalue,
            ])
        GLEAN_DB.commit()


class Ping:
    name: str

    def __init__(self, name):
        self.name = name

    def submit(self):
        cur = GLEAN_DB.cursor()

        data = {}
        for row in cur.execute("SELECT id, value, labels FROM telemetry WHERE ping = ?1", [self.name]).fetchall():
            print(row)
            id, value, labels = row
            if labels:
                if id not in data:
                    data[id] = {}
                data[id][labels] = value
            else:
                data[id] = value

        cur.execute("DELETE FROM telemetry WHERE ping = ?1 AND lifetime = 'ping'", [self.name])

        print(json.dumps(data))
        GLEAN_DB.commit()

class Counter(Metric):
    def add(self, amount=1):
        self.record(lambda value: int(value or 0)+amount)


class LabeledCounter(Metric):
    def get(self, label):
        c = Counter(self.name, self.lifetime, self.send_in_pings)
        c.label = label
        return c


class StringMetric(Metric):
    def set(self, value):
        self.record(lambda _: value)

c = Counter("starts", "user")
c.add()

c = Counter("clicks", "ping")
c.add(2)
c.add(2)

lc = LabeledCounter("errors", "ping")
lc.get("starts").add(1)

s = StringMetric("reason", "ping")
s.set("cli")

metrics_ping = Ping("metrics")
metrics_ping.submit()

c.add(2)
metrics_ping.submit()
