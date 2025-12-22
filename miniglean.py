#!/usr/bin/env -S uv run --script

import sqlite3
import json
import uuid
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
GLEAN_DB.execute("""
CREATE TABLE IF NOT EXISTS pending_pings(
  id TEXT NOT NULL,
  ping TEXT NOT NULL,
  payload TEXT NOT NULL,
  metadata TEXT NOT NULL,
  tries INTEGER DEFAULT 0,
  updated_at TEXT NOT NULL DEFAULT (DATETIME('now')),
  UNIQUE(id, ping)
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
            labels = label_check(self, cur)

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
                if "," in labels:
                    labels = labels.split(",")
                    if labels[0] not in metrics[id]:
                        metrics[id][labels[0]] = {}
                    metrics[id][labels[0]][labels[1]] = value
                else:
                    metrics[id][labels] = value
            else:
                metrics[id] = value

        cur.execute("DELETE FROM telemetry WHERE ping = ?1 AND lifetime = 'ping'", [self.name])

        payload = {
            "ping_info": get_ping_info(self.name),
            "metrics": metrics
        }

        metadata = json.dumps({})
        doc_id = str(uuid.uuid4())

        payload_json = json.dumps(payload, indent=2)
        cur.execute("""
            INSERT INTO pending_pings (id, ping, payload, metadata)
                VALUES (?1, ?2, ?3, ?4)
            """, [
            doc_id,
            self.name,
            payload_json,
            metadata,
        ])

        print("payload:", payload_json)
        GLEAN_DB.commit()


class Counter(Metric):
    def add(self, amount=1):
        self.record(lambda value: int(value or 0)+amount)


def label_check(this, cur):
    label = this.label
    if "," in label:
        labels = label.split(",")
        key = labels[0]
        cat = labels[1]

        # keys AND cats are static:
        if this.allowed_keys and this.allowed_cats:
            if key not in this.allowed_keys:
                key = "__other__"
            if cat not in this.allowed_cats:
                cat = "__other__"
        else:
            existing_labels = cur.execute("SELECT DISTINCT labels FROM telemetry WHERE id = ?1", [this.name]).fetchall()
            existing_labels = [lab[0].split(",") for lab in existing_labels]
            keys = { lab[0] for lab in existing_labels }
            cats = { lab[1] for lab in existing_labels }

            # keys is static
            if this.allowed_keys and key not in this.allowed_keys:
                key = "__other__"
            elif key not in keys and len(keys) >= 16:
                key = "__other__"

            # cat is static
            if this.allowed_cats and cat not in this.allowed_cats:
                cat = "__other__"
            elif cat not in cats and len(cats) >= 16:
                cat = "__other__"

            pass

        label = f"{key},{cat}"
    else:
        if this.allowed_labels:
            if label not in this.allowed_labels:
                label = "__other__"
        else:
            existing_labels = cur.execute("SELECT DISTINCT labels FROM telemetry WHERE id = ?1", [this.name]).fetchall()
            existing_labels = { lab[0] for lab in existing_labels }
            if label not in existing_labels and len(existing_labels) >= 16:
                label = "__other__"

    return label

class LabeledCounter(Metric):
    allowed_labels: set

    def __init__(self, name, lifetime, send_in_pings=None, allowed_labels=None):
        super().__init__(name, lifetime, send_in_pings)
        self.allowed_labels = allowed_labels

    def get(self, label):
        c = Counter(self.name, self.lifetime, self.send_in_pings)
        c.label = label
        c.allowed_labels = self.allowed_labels
        return c

class DualLabeledCounter(Metric):
    allowed_keys: set
    allowed_cats: set

    def __init__(self, name, lifetime, send_in_pings=None, allowed_keys=None, allowed_cats=None):
        super().__init__(name, lifetime, send_in_pings)
        self.allowed_keys = allowed_keys
        self.allowed_cats = allowed_cats
        self.allowed_labels = None

    def get(self, key, cat):
        c = Counter(self.name, self.lifetime, self.send_in_pings)
        c.label = f"{key},{cat}"
        c.allowed_keys = self.allowed_keys
        c.allowed_cats = self.allowed_cats
        return c


class StringMetric(Metric):
    def set(self, value):
        self.record(lambda _: value)


metrics_ping = Ping("metrics")

c = Counter("starts", "user")
c.add()

c = Counter("clicks", "ping")
c.add(2)
c.add(2)

lc = LabeledCounter("errors", "ping")
lc.get("label0").add(1)

slc = LabeledCounter("static-labels", "ping", ["metrics"], {"predefined"})
slc.get("predefined").add(1)
slc.get("unknown").add(1)

dlc = DualLabeledCounter("dual-labels", "ping")
dlc.get("key0", "cat0").add(1)
dlc.get("key0", "cat1").add(1)
dlc.get("key1", "cat0").add(1)

sdlc = DualLabeledCounter("static-dual-labels", "ping", ["metrics"], {"predefined-key"}, {"predefined-cat"})
sdlc.get("predefined-key", "cat0").add(1)
sdlc.get("predefined-key", "predefined-cat").add(1)
sdlc.get("key1", "predefined-cat").add(1)

metrics_ping.submit()

#for i in range(20):
#    lc.get(f"label{i}").add(1)
#
#s = StringMetric("reason", "ping")
#s.set("cli")
#
#c.add(2)
#metrics_ping.submit()
#
#c.add(42)
#s = StringMetric("reason", "ping")
#s.set("cli")
#
#for i in range(20):
#    lc.get(f"label{i}").add(1)
