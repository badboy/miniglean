#!/usr/bin/env -S uv run --script

import sqlite3
import json
from datetime import datetime

GLEAN_START_TIME = datetime.now()
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

def get_ping_info(ping: str):
    global GLEAN_START_TIME

    seq = Counter(f"sequence#{ping}", "user", ["glean_ping_info"])
    seq.add(1)

    start_time = StringMetric(f"start#{ping}", "user", ["glean_ping_info"])
    st = start_time.get_value() or str(GLEAN_START_TIME)

    end_time = str(datetime.now())
    start_time.set(end_time)

    ping_info = {
        "seq": seq.get_value(),
        "start_time": st,
        "end_time": end_time,
    }
    return ping_info

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

        cur = GLEAN_DB.cursor()

        labels = ''
        if self.label:
            labels = self.label
            label_cnt = cur.execute("SELECT COUNT(DISTINCT labels) FROM telemetry WHERE id = ?1", [self.name]).fetchone()[0]
            if label_cnt >= 16:
                labels = "__other__"

        for ping in pings:
            # print(f"Recording for {self.name} in {ping}")
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

    def get_value(self, ping=None):
        global GLEAN_DB
        ping = ping or self.send_in_pings[0]

        cur = GLEAN_DB.cursor()

        labels = ''
        if self.label:
            labels = self.label

        value = cur.execute("SELECT value FROM telemetry WHERE id = ?1 AND ping = ?2 AND labels = ?3", [self.name, ping, labels]).fetchone()
        return value and value[0]

class Ping:
    name: str

    def __init__(self, name):
        self.name = name

    def submit(self):
        cur = GLEAN_DB.cursor()

        metrics = {}
        for row in cur.execute("SELECT id, value, labels FROM telemetry WHERE ping = ?1", [self.name]).fetchall():
            id, value, labels = row
            if labels:
                if id not in metrics:
                    metrics[id] = {}
                metrics[id][labels] = value
            else:
                metrics[id] = value

        cur.execute("DELETE FROM telemetry WHERE ping = ?1 AND lifetime = 'ping'", [self.name])

        payload = {
            "ping_info": get_ping_info(self.name),
            "metrics": metrics
        }

        print("payload:", json.dumps(payload))
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

for i in range(20):
    lc.get(f"label{i}").add(1)

s = StringMetric("reason", "ping")
s.set("cli")

metrics_ping = Ping("metrics")
metrics_ping.submit()

c.add(2)
metrics_ping.submit()
