#!/usr/bin/env -S uv run --script

import sqlite3
import json
import uuid
import random
from datetime import datetime
from collections import defaultdict

MAX_LABELS = 16
MAX_TRIES = 3

GLEAN_START_TIME = datetime.now()
GLEAN_DB = sqlite3.connect("glean.db")
GLEAN_DB.execute("pragma journal_mode=wal")
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

        labels = ""
        if self.label:
            labels = label_check(self, cur)

        for ping in pings:
            # print(f"Recording for {self.name} in {ping}")
            value = cur.execute(
                "SELECT value FROM telemetry WHERE id = ?1 AND ping = ?2 AND labels = ?3",
                [self.name, ping, labels],
            ).fetchone()
            newvalue = fn(value and value[0])
            cur.execute(
                """
                INSERT INTO telemetry (id, ping, lifetime, labels, value, updated_at)
                VALUES (?1, ?2, ?3, ?4, ?5, DATETIME('now'))
                ON CONFLICT(id, ping, labels) DO UPDATE SET lifetime = excluded.lifetime, value = excluded.value, updated_at = excluded.updated_at
            """,
                [
                    self.name,
                    ping,
                    self.lifetime,
                    labels,
                    newvalue,
                ],
            )
        GLEAN_DB.commit()

    def get_value(self, ping=None):
        global GLEAN_DB
        ping = ping or self.send_in_pings[0]

        cur = GLEAN_DB.cursor()

        labels = ""
        if self.label:
            labels = self.label

        value = cur.execute(
            "SELECT value FROM telemetry WHERE id = ?1 AND ping = ?2 AND labels = ?3",
            [self.name, ping, labels],
        ).fetchone()
        return value and value[0]


class Ping:
    name: str

    def __init__(self, name):
        self.name = name

    def submit(self):
        cur = GLEAN_DB.cursor()

        metrics = defaultdict(lambda: defaultdict(dict))
        for row in cur.execute(
            "SELECT id, value, labels FROM telemetry WHERE ping = ?1", [self.name]
        ).fetchall():
            id, value, labels = row
            if labels:
                if "," in labels:
                    labels = labels.split(",")
                    metrics[id][labels[0]][labels[1]] = value
                else:
                    metrics[id][labels] = value
            else:
                metrics[id] = value

        cur.execute(
            "DELETE FROM telemetry WHERE ping = ?1 AND lifetime = 'ping'", [self.name]
        )

        payload = {"ping_info": get_ping_info(self.name), "metrics": metrics}

        metadata = json.dumps({})
        doc_id = str(uuid.uuid4())

        payload_json = json.dumps(payload)
        cur.execute(
            """
            INSERT INTO pending_pings (id, ping, payload, metadata)
                VALUES (?1, ?2, ?3, ?4)
            """,
            [
                doc_id,
                self.name,
                payload_json,
                metadata,
            ],
        )
        GLEAN_DB.commit()


class Uploader:
    def get_upload_task(self):
        sql = """
UPDATE pending_pings SET tries = tries+1, updated_at = DATETIME('now')
WHERE id = (
    SELECT id FROM pending_pings ORDER BY updated_at, tries LIMIT 1
) RETURNING id, tries, ping, payload, metadata;
        """

        global GLEAN_DB
        global MAX_TRIES

        print("Getting upload task")
        cur = GLEAN_DB.cursor()
        while True:
            value = cur.execute(sql).fetchone()

            if not value:
                print("no ping to upload")
                break

            if value[1] > MAX_TRIES:
                cur.execute("DELETE FROM pending_pings WHERE id = ?1", [value[0]])
                GLEAN_DB.commit()

            print(f"Uploading ping '{value[2]}' ({value[0]})")
            if random.random() < 0.5:
                print(f"Upload succeeded for {value[0]}")
                cur.execute("DELETE FROM pending_pings WHERE id = ?1", [value[0]])
                GLEAN_DB.commit()
            else:
                print(f"Upload failed for {value[0]}")


class Counter(Metric):
    def add(self, amount=1):
        self.record(lambda value: int(value or 0) + amount)


def label_check(this, cur):
    global MAX_LABELS

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
            existing_labels = cur.execute(
                "SELECT DISTINCT labels FROM telemetry WHERE id = ?1", [this.name]
            ).fetchall()
            existing_labels = [lab[0].split(",") for lab in existing_labels]
            keys = {lab[0] for lab in existing_labels}
            cats = {lab[1] for lab in existing_labels}

            # keys is static
            if this.allowed_keys and key not in this.allowed_keys:
                key = "__other__"
            elif key not in keys and len(keys) >= MAX_LABELS:
                key = "__other__"

            # cat is static
            if this.allowed_cats and cat not in this.allowed_cats:
                cat = "__other__"
            elif cat not in cats and len(cats) >= MAX_LABELS:
                cat = "__other__"

            pass

        label = f"{key},{cat}"
    else:
        if this.allowed_labels:
            if label not in this.allowed_labels:
                label = "__other__"
        else:
            existing_labels = cur.execute(
                "SELECT DISTINCT labels FROM telemetry WHERE id = ?1", [this.name]
            ).fetchall()
            existing_labels = {lab[0] for lab in existing_labels}
            if label not in existing_labels and len(existing_labels) >= MAX_LABELS:
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

    def get_value(self, ping=None):
        global GLEAN_DB
        ping = ping or self.send_in_pings[0]

        cur = GLEAN_DB.cursor()

        value = cur.execute(
            "SELECT labels, value FROM telemetry WHERE id = ?1 AND ping = ?2",
            [self.name, ping],
        ).fetchall()
        return value and dict(value)


class DualLabeledCounter(Metric):
    allowed_keys: set
    allowed_cats: set

    def __init__(
        self, name, lifetime, send_in_pings=None, allowed_keys=None, allowed_cats=None
    ):
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

    def get_value(self, ping=None):
        global GLEAN_DB
        ping = ping or self.send_in_pings[0]

        cur = GLEAN_DB.cursor()

        row = cur.execute(
            "SELECT labels, value FROM telemetry WHERE id = ?1 AND ping = ?2",
            [self.name, ping],
        ).fetchall()
        values = defaultdict(dict)
        for [labels, value] in row:
            [key, cat] = labels.split(",")
            values[key][cat] = value

        return values


class StringMetric(Metric):
    def set(self, value):
        self.record(lambda _: value)


if __name__ == "__main__":
    metrics_ping = Ping("metrics")

    uploader = Uploader()
    uploader.get_upload_task()

    c = Counter("starts", "user")
    c.add()

    c = Counter("clicks", "ping")
    c.add(2)
    c.add(2)

    lc = LabeledCounter("errors", "ping")
    lc.get("label0").add(1)

    metrics_ping.submit()

    slc = LabeledCounter("static-labels", "ping", ["metrics"], {"predefined"})
    slc.get("predefined").add(1)
    slc.get("unknown").add(1)

    dlc = DualLabeledCounter("dual-labels", "ping")
    dlc.get("key0", "cat0").add(1)
    dlc.get("key0", "cat1").add(1)
    dlc.get("key1", "cat0").add(1)

    sdlc = DualLabeledCounter(
        "static-dual-labels",
        "ping",
        ["metrics"],
        {"predefined-key"},
        {"predefined-cat"},
    )
    sdlc.get("predefined-key", "cat0").add(1)
    sdlc.get("predefined-key", "predefined-cat").add(1)
    sdlc.get("key1", "predefined-cat").add(1)

    metrics_ping.submit()

    uploader.get_upload_task()


def test_counter():
    c = Counter("starts", "user")
    c.add()

    assert 1 == c.get_value()

    c = Counter("clicks", "ping")
    c.add(2)
    c.add(2)

    assert 4 == c.get_value()


def test_string():
    s = StringMetric("reason", "ping")
    s.set("cli")

    assert "cli" == s.get_value()


def test_labeled_counter():
    lc = LabeledCounter("errors", "ping")
    lc.get("label0").add(1)
    assert {"label0": 1} == lc.get_value()


def test_labeled_counter_many():
    lc = LabeledCounter("many-errors", "ping")
    for i in range(20):
        lc.get(f"label{i}").add(1)

    exp = {
        "label0": 1,
        "label1": 1,
        "label2": 1,
        "label3": 1,
        "label4": 1,
        "label5": 1,
        "label6": 1,
        "label7": 1,
        "label8": 1,
        "label9": 1,
        "label10": 1,
        "label11": 1,
        "label12": 1,
        "label13": 1,
        "label14": 1,
        "label15": 1,
        "__other__": 4,
    }
    assert exp == lc.get_value()


def test_labeled_counter_static():
    lc = LabeledCounter("static-labeled", "ping", ["metrics"], {"predefined"})
    lc.get("predefined").add(1)
    lc.get("random").add(1)
    assert {"predefined": 1, "__other__": 1} == lc.get_value()


def test_dual_labeled_counter():
    lc = DualLabeledCounter("dual-counter", "ping")
    lc.get("key0", "cat0").add(1)
    lc.get("key0", "cat1").add(1)
    lc.get("key1", "cat0").add(1)

    exp = {"key0": {"cat0": 1, "cat1": 1}, "key1": {"cat0": 1}}
    assert exp == lc.get_value()


def test_dual_labeled_counter_static():
    lc = DualLabeledCounter("static-dual-counter", "ping", ["metrics"], {"key0"})
    lc.get("key0", "cat0").add(1)
    lc.get("key0", "cat1").add(1)
    lc.get("key1", "cat0").add(1)

    exp = {"key0": {"cat0": 1, "cat1": 1}, "__other__": {"cat0": 1}}
    assert exp == lc.get_value()

    lc = DualLabeledCounter("static-dual-counter2", "ping", ["metrics"], None, {"cat0"})
    lc.get("key0", "cat0").add(1)
    lc.get("key0", "cat1").add(1)
    lc.get("key1", "cat0").add(1)

    exp = {"key0": {"cat0": 1, "__other__": 1}, "key1": {"cat0": 1}}
    assert exp == lc.get_value()

    lc = DualLabeledCounter(
        "static-dual-counter3", "ping", ["metrics"], {"key0"}, {"cat0"}
    )
    lc.get("key0", "cat0").add(1)
    lc.get("key0", "cat1").add(1)
    lc.get("key1", "cat0").add(1)

    exp = {"key0": {"cat0": 1, "__other__": 1}, "__other__": {"cat0": 1}}
    assert exp == lc.get_value()


def test_dual_labeled_counter_many():
    lc = DualLabeledCounter("dual-labeled-many", "ping")

    for key in range(20):
        for cat in range(5):
            lc.get(f"key{key}", f"cat{cat}").add(1)

    exp = {
        "__other__": {
            "cat0": 4,
            "cat1": 4,
            "cat2": 4,
            "cat3": 4,
            "cat4": 4,
        }
    }
    for key in range(16):
        for cat in range(5):
            k = f"key{key}"
            if k not in exp:
                exp[k] = {}
            exp[k][f"cat{cat}"] = 1

    assert exp == lc.get_value()


def test_dual_labeled_counter_many2():
    lc = DualLabeledCounter("dual-labeled-many2", "ping")

    for key in range(5):
        for cat in range(20):
            lc.get(f"key{key}", f"cat{cat}").add(1)

    exp = {}
    for key in range(5):
        k = f"key{key}"
        exp[k] = {"__other__": 4}
        for cat in range(16):
            exp[k][f"cat{cat}"] = 1

    assert exp == lc.get_value()
