#!/usr/bin/env python3
"""
HRE sensor fleet: the always-on telemetry source. Run it as a service and leave it running.

  * Every sensor (all municipalities, water and sewer, 190 with the default wards) reports normal telemetry
    every --interval seconds (default 300), forever, whether or not GNS3 is running.
  * Normal behaviour is a pure function of (sensor, metric, time), so it is identical after a restart, a gap
    fill, or when a GNS3 node takes over. Faults are applied on top of it and then removed again.
  * Report times are a fixed grid per sensor (every 300 s at the sensor's own offset), so re-sending a slot
    produces exactly the same row and the database ignores the duplicate.

Playing nicely with the GNS3 nodes
  If a GNS3 node script is publishing a sensor, the fleet stays silent for that sensor. When the node goes
  quiet (GNS3 stopped) the fleet fills the hole it left with the sensor's normal series and carries on, so the
  dashboard and the history never show a gap. Use --no-yield to make the fleet the only source.

After the fleet itself was down (reboot, maintenance)
  With --dsn (a read-only database login) it finds each sensor's newest stored reading and fills forward from
  there, up to --max-fill. Without --dsn it simply resumes.

Control (the same messages the fault scripts and hre_control.py already use)
  hre/sim/power/...   loadshedding switches: affected sensors go silent, like a real outage
  hre/sim/fault/...   {"kind": "...", "ramp": s?, "duration": s?}  target = <sensor_id> | <municipality>-<water|sewer> | all
  A critical reading is sent immediately and repeated every --fast-interval until it recovers.

Ground truth for model evaluation
  Every fault the fleet applies or ends is appended to <state-dir>/labels.jsonl (sensor, kind, start, end).
  Active faults survive a restart.

Examples
  python3 hre_fleet.py --dry-run --once                       # print one report per sensor, no broker
  python3 hre_fleet.py --host 127.0.0.1 --user hre_fleet --password SECRET
  python3 hre_fleet.py ... --municipality harare --types water   # only a slice
Run as a service: see hre-fleet.service.
"""
import argparse
import collections
import fcntl
import json
import math
import os
import random
import signal
import socket
import sys
import time
import zlib
from datetime import datetime, timedelta, timezone

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None
try:
    import psycopg2
except ImportError:
    psycopg2 = None

MUNICIPALITIES = ["harare", "chitungwiza", "epworth", "ruwa"]
WARD_COUNT = {"harare": 45, "chitungwiza": 25, "epworth": 7, "ruwa": 18}     # wards that have sensors
HARARE_TZ = timezone(timedelta(hours=2))

# Limits carried by the sensor firmware. Must match the `baseline` table.
BASELINE = {
    "water": {
        "pressure":  dict(nmin=2.0,  nmax=4.0,  cmin=1.0,  cmax=5.5),
        "flow_rate": dict(nmin=8.0,  nmax=16.0, cmin=2.0,  cmax=25.0),
        "level":     dict(nmin=40.0, nmax=90.0, cmin=20.0, cmax=98.0),
    },
    "sewer": {
        "flow_rate": dict(nmin=3.0,  nmax=9.0,  cmin=0.5,  cmax=15.0),
        "level":     dict(nmin=15.0, nmax=55.0, cmin=5.0,  cmax=85.0),
    },
}

# Fault models (same as the per-VLAN node scripts): metric -> target value; ramp = seconds to reach it.
FAULTS = {
    "water": {
        "burst":         {"ramp": 0,   "effects": {"pressure": lambda b: b["cmin"] * 0.3,
                                                   "flow_rate": lambda b: b["cmax"] * 1.1}},
        "leak":          {"ramp": 600, "effects": {"pressure": lambda b: b["cmin"] - 0.2 * (b["nmin"] - b["cmin"]),
                                                   "flow_rate": lambda b: b["nmax"] + 0.5 * (b["cmax"] - b["nmax"])}},
        "low_pressure":  {"ramp": 0,   "effects": {"pressure": lambda b: b["nmin"] - 0.5 * (b["nmin"] - b["cmin"])}},
        "high_pressure": {"ramp": 0,   "effects": {"pressure": lambda b: b["cmax"] + 0.1 * (b["cmax"] - b["nmax"])}},
        "reservoir_low": {"ramp": 900, "effects": {"level": lambda b: b["cmin"] - 0.2 * (b["nmin"] - b["cmin"])}},
        "supply_loss":   {"ramp": 0,   "effects": {"pressure": lambda b: b["cmin"] - 0.2 * (b["nmin"] - b["cmin"]),
                                                   "flow_rate": lambda b: b["cmin"] * 0.25}},
    },
    "sewer": {
        "blockage":     {"ramp": 300, "effects": {"level": lambda b: b["cmax"] + 0.1 * (b["cmax"] - b["nmax"]),
                                                  "flow_rate": lambda b: b["cmin"] * 0.4}},
        "overflow":     {"ramp": 0,   "effects": {"level": lambda b: 95.0,
                                                  "flow_rate": lambda b: b["cmax"] * 1.1}},
        "surcharge":    {"ramp": 120, "effects": {"level": lambda b: b["nmax"] + 0.5 * (b["cmax"] - b["nmax"])}},
        "infiltration": {"ramp": 600, "effects": {"flow_rate": lambda b: b["nmax"] + 0.5 * (b["cmax"] - b["nmax"])}},
    },
}

# ---- NORMAL MODEL (identical in hre_fleet.py and the node scripts) ----------------------------------------
# A sensor's normal behaviour is a pure function of (sensor, metric, time): the same inputs always give the
# same value, on any machine and across restarts. That is what "retaining the pattern" means: a restart, a
# gap fill, or a GNS3 node taking over produces exactly the series that would have been produced without
# the interruption. Faults are applied ON TOP of this series.
_SMOOTH = ((47 * 60, 0.05), (3.3 * 3600, 0.05), (11 * 3600, 0.05))     # slow wander: (period seconds, amplitude)


def normal_value(sensor_id, sensor_type, metric, b, t):
    loc = datetime.fromtimestamp(t, HARARE_TZ)
    h = loc.hour + loc.minute / 60.0 + loc.second / 3600.0
    cos = lambda x: math.cos(2 * math.pi * x)
    if metric == "pressure":
        day = -cos((h - 7) / 12)                      # dips at the morning and evening demand peaks
    elif metric == "flow_rate":
        day = cos((h - 7) / 12)                       # peaks around 07:00 and 19:00
    elif sensor_type == "water":
        day = cos((h - 6) / 24)                       # reservoir fills overnight
    else:
        day = cos((h - 9) / 12)                       # sewer level follows the flow
    amp = 0.5 * (0.8 if loc.weekday() >= 5 else 1.0)  # quieter weekends
    seed = zlib.crc32(f"{sensor_id}|{metric}".encode())
    offset = (seed % 100) / 100.0 * 0.2               # each sensor sits a little higher or lower
    smooth = sum(a * math.sin(2 * math.pi * t / p + ((seed >> (i * 5)) % 628) / 100.0) for i, (p, a) in enumerate(_SMOOTH))
    white = (random.Random(zlib.crc32(f"{sensor_id}|{metric}|{int(t)}".encode())).random() * 2 - 1) * 0.08
    mid, half = (b["nmin"] + b["nmax"]) / 2, (b["nmax"] - b["nmin"]) / 2
    return mid + half * (amp * day + offset + smooth + white)      # never leaves the normal band (max 0.93 of it)
# ------------------------------------------------------------------------------------------------------------


def iso(t):
    return datetime.fromtimestamp(t, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def atomic_write(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(obj, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def parse_wards(text):
    out = []
    for part in text.split(","):
        part = part.strip()
        if "-" in part:
            a, b = part.split("-")
            out.extend(range(int(a), int(b) + 1))
        elif part:
            out.append(int(part))
    return sorted(set(out))


class Sensor:
    def __init__(self, muni, ward, stype, interval):
        self.muni, self.ward, self.type = muni, ward, stype
        self.id = f"{muni}-{stype}-{ward:02d}"
        self.offset = zlib.crc32(self.id.encode()) % int(interval)       # fixed slot inside every interval
        self.next_slot = 0.0
        self.ext_last = 0.0          # last time a GNS3 node published this sensor
        self.yielded = False
        self.critical = False
        self.last_tx = 0.0
        self.last_sample = 0.0
        self.was_off = False
        self.boot_at = 0.0


class Fleet:
    def __init__(self, a):
        self.a = a
        munis = MUNICIPALITIES if a.municipality == "all" else [a.municipality]
        self.sensors = {}
        for m in munis:
            wards = parse_wards(a.wards) if a.wards else list(range(1, WARD_COUNT[m] + 1))
            for w in wards:
                for t in a.types.split(","):
                    s = Sensor(m, w, t.strip(), a.interval)
                    self.sensors[s.id] = s
        self.power = {}
        self.faults = {}
        self.fillq = collections.deque()
        self.sent = self.acked = self.filled = 0
        self.sent_since_ack = 0
        self.last_ack = time.time()
        self.connected = False
        self.fatal = None
        self.closing = False
        self.running = True
        self.yield_ok = not a.no_yield
        self.tele_mid = None
        self.client = None
        self.cid = f"hre-fleet-{a.node or socket.gethostname()}"
        os.makedirs(a.state_dir, exist_ok=True)
        self.faults_path = os.path.join(a.state_dir, "faults.json")
        self.labels_path = os.path.join(a.state_dir, "labels.jsonl")
        self.load_faults()

    # ---------------------------------------------------------------- faults + labels
    def label(self, event, s, f, when):
        rec = {"time": iso(when), "event": event, "sensor_id": s.id, "municipality": s.muni, "vlan": s.type,
               "kind": f["kind"], "fault_start": iso(f["start"]), "ramp_s": f["ramp"],
               "fault_until": iso(f["until"]) if f["until"] else None}
        with open(self.labels_path, "a") as fh:
            fh.write(json.dumps(rec) + "\n")

    def load_faults(self):
        try:
            saved = json.load(open(self.faults_path))
        except (OSError, ValueError):
            return
        now = time.time()
        for sid, f in saved.items():
            if sid in self.sensors and (f["until"] is None or f["until"] > now):
                self.faults[sid] = f
        if self.faults:
            print(f"restored {len(self.faults)} active fault(s) from {self.faults_path}", flush=True)

    def save_faults(self):
        atomic_write(self.faults_path, self.faults)

    def apply_fault(self, target, spec):
        kind = spec.get("kind")
        if target == "all":
            ids = list(self.sensors)
        elif target in self.sensors:
            ids = [target]
        else:
            ids = [i for i, s in self.sensors.items() if target == f"{s.muni}-{s.type}"]
        if not ids:
            return
        now = time.time()
        if kind == "clear":
            for i in ids:
                f = self.faults.pop(i, None)
                if f:
                    self.label("fault_end", self.sensors[i], f, now)
            self.save_faults()
            print(f"fault cleared: {target}", flush=True)
            return
        n = 0
        for i in ids:
            s = self.sensors[i]
            model = FAULTS[s.type].get(kind)
            if not model:
                continue
            ramp = float(spec["ramp"]) if spec.get("ramp") is not None else model["ramp"]
            f = {"kind": kind, "start": now, "ramp": ramp, "until": now + float(spec["duration"]) if spec.get("duration") else None}
            if i in self.faults:
                self.label("fault_end", s, self.faults[i], now)
            self.faults[i] = f
            self.label("fault_start", s, f, now)
            n += 1
        if n:
            self.save_faults()
            print(f"FAULT '{kind}' on {target}: {n} sensor(s)", flush=True)
        else:
            print(f"ignored: '{kind}' does not apply to {target}", flush=True)

    def expire_faults(self, now):
        gone = [i for i, f in self.faults.items() if f["until"] and now >= f["until"]]
        for i in gone:
            f = self.faults.pop(i)
            self.label("fault_end", self.sensors[i], f, f["until"])
            print(f"fault on {i} ended (duration elapsed)", flush=True)
        if gone:
            self.save_faults()

    # ---------------------------------------------------------------- readings
    def reading(self, s, t):
        f = self.faults.get(s.id)
        model = None
        p = 1.0
        if f and f["start"] <= t and (f["until"] is None or t < f["until"]):
            model = FAULTS[s.type].get(f["kind"])
            if f["ramp"] > 0:
                p = min(1.0, max(0.0, (t - f["start"]) / f["ramp"]))
        out = {}
        for metric, b in BASELINE[s.type].items():
            v = normal_value(s.id, s.type, metric, b, t)
            if model and metric in model["effects"]:
                v = v + p * (model["effects"][metric](b) - v)
            out[metric] = round(max(0.0, v), 2)
        return out

    def is_critical(self, s, r):
        return any(v < BASELINE[s.type][m]["cmin"] or v > BASELINE[s.type][m]["cmax"] for m, v in r.items())

    def powered(self, s):
        return not any(self.power.get(k) is False for k in ("all", f"{s.muni}/all", f"{s.muni}/{s.ward}"))

    def slot_after(self, s, t):
        iv = self.a.interval
        return (math.floor((t - s.offset) / iv) + 1) * iv + s.offset

    # ---------------------------------------------------------------- publishing
    def emit(self, s, t, readings, trigger, backfill=False):
        packet = {"sensor_id": s.id, "ts": iso(t), "readings": readings, "trigger": trigger, "src": "fleet"}
        if backfill:
            packet["backfill"] = True
        if self.a.dry_run:
            self.sent += 1
            print(f"hre/telemetry/{s.muni}/{s.id} {json.dumps(packet)}", flush=True)
            return
        self.client.publish(f"hre/telemetry/{s.muni}/{s.id}", json.dumps(packet), qos=1)
        self.sent += 1
        self.sent_since_ack += 1
        if trigger != "schedule" or not backfill:
            s.last_tx = time.time()

    def fill(self, s, from_t, now, reason):
        """queue the normal-series slots of sensor s in (from_t, now]; pumped at --fill-rate per second."""
        from_t = max(from_t, now - self.a.max_fill)
        t = self.slot_after(s, from_t)
        limit = self.slot_after(s, now)                  # every slot up to and including 'now'; the schedule owns the rest
        n = 0
        while t < limit:
            self.fillq.append((s, t))
            t += self.a.interval
            n += 1
        if n:
            print(f"filling {n} slot(s) for {s.id} ({reason})", flush=True)
        return n

    def pump_fill(self):
        n = 0
        while self.fillq and n < self.a.fill_rate:
            s, t = self.fillq.popleft()
            if self.powered(s):
                r = self.reading(s, t)
                self.emit(s, t, r, "schedule")
                s.critical = self.is_critical(s, r)
                self.filled += 1
            n += 1

    # ---------------------------------------------------------------- MQTT
    def connect(self):
        a = self.a
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.cid)
        except AttributeError:
            self.client = mqtt.Client(client_id=self.cid)
        if a.user:
            self.client.username_pw_set(a.user, a.password)
        self.client.will_set(f"hre/status/{self.cid}", "offline", qos=1, retain=True)
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.client.on_message = self.on_message
        self.client.on_publish = self.on_publish
        self.client.on_subscribe = self.on_subscribe
        self.client.reconnect_delay_set(1, 30)
        self.client.max_inflight_messages_set(200)
        while self.running:
            try:
                self.client.connect(a.host, a.port, keepalive=30)
                break
            except OSError as e:
                print(f"broker {a.host}:{a.port} unreachable ({e}); retrying in 3 s", flush=True)
                time.sleep(3)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc, *extra):
        ok = (rc == 0) if isinstance(rc, int) else not getattr(rc, "is_failure", False)
        if not ok:
            self.fatal = f"broker refused the connection: {rc}"
            print(f"ERROR: {self.fatal}. Check --user / --password.", flush=True)
            return
        self.connected = True
        self.last_ack = time.time()
        client.subscribe([("hre/sim/power/#", 1), ("hre/sim/fault/#", 1)])
        if self.yield_ok:
            _, self.tele_mid = client.subscribe("hre/telemetry/#", 0)
        client.publish(f"hre/status/{self.cid}", "online", qos=1, retain=True)
        print(f"connected to {self.a.host}:{self.a.port} as {self.cid} | {len(self.sensors)} sensors", flush=True)

    def on_disconnect(self, client, userdata, *args):
        self.connected = False
        if not self.closing:
            print("disconnected from the broker; reconnecting automatically", flush=True)

    def on_publish(self, client, userdata, mid, *extra):
        self.acked += 1
        self.sent_since_ack = 0
        self.last_ack = time.time()

    def on_subscribe(self, client, userdata, mid, granted, *extra):
        if mid == self.tele_mid:
            bad = any((g == 128) if isinstance(g, int) else getattr(g, "is_failure", False) for g in granted)
            if bad:
                self.yield_ok = False
                print("WARNING: this user cannot read hre/telemetry/#, so the fleet cannot yield to GNS3 nodes "
                      "(give it 'topic read hre/telemetry/#' or run with --no-yield)", flush=True)

    def on_message(self, client, userdata, msg):
        topic = msg.topic
        if topic.startswith("hre/telemetry/"):
            if not self.yield_ok:
                return
            try:
                p = json.loads(msg.payload)
            except ValueError:
                return
            if p.get("src") != "fleet":
                s = self.sensors.get(p.get("sensor_id"))
                if s:
                    s.ext_last = time.time()
            return
        payload = msg.payload.decode("utf-8", "ignore").strip()
        if topic.startswith("hre/sim/power/"):
            scope = topic[len("hre/sim/power/"):]
            if not payload:
                if self.power.pop(scope, None) is not None:
                    print(f"power restored for scope '{scope}'", flush=True)
                return
            try:
                grid = bool(json.loads(payload).get("grid", True))
            except (ValueError, AttributeError):
                return
            if grid:
                self.power.pop(scope, None)
            else:
                self.power[scope] = False
                print(f"LOADSHEDDING scope '{scope}'", flush=True)
        elif topic.startswith("hre/sim/fault/"):
            if msg.retain:                                       # faults are live events, never replayed
                return
            try:
                spec = json.loads(payload)
            except ValueError:
                return
            if isinstance(spec, dict):
                self.apply_fault(topic[len("hre/sim/fault/"):], spec)

    # ---------------------------------------------------------------- database gap fill
    def db_fill(self, now):
        if not self.a.dsn:
            return
        if psycopg2 is None:
            print("WARNING: --dsn given but psycopg2 is not installed (pip install psycopg2-binary); skipping gap fill", flush=True)
            return
        try:
            conn = psycopg2.connect(self.a.dsn)
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute("select sensor_id, extract(epoch from max(time)) from telemetry group by sensor_id")
                newest = {sid: float(t) for sid, t in cur.fetchall()}
            conn.close()
        except Exception as e:
            print(f"WARNING: could not read the newest readings ({e}); skipping gap fill", flush=True)
            return
        total = 0
        for sid, s in self.sensors.items():
            t = newest.get(sid)
            if t is not None and now - t > 1.5 * self.a.interval:
                total += self.fill(s, t, now, "fleet was down")
        print(f"database gap fill: {total} slot(s) queued for {len(self.sensors)} sensors", flush=True)

    # ---------------------------------------------------------------- main loop
    def step(self, now):
        a = self.a
        for s in self.sensors.values():
            if not self.powered(s):
                s.was_off = True
                continue
            if s.was_off:                                        # power is back: report soon after "boot"
                s.was_off = False
                s.boot_at = now + random.uniform(1, 5)
                s.critical = False
                s.next_slot = self.slot_after(s, now)
            if s.boot_at and now >= s.boot_at:
                s.boot_at = 0.0
                self.emit(s, int(now), self.reading(s, now), "schedule")
            if self.yield_ok and s.ext_last and now - s.ext_last < a.yield_grace:
                s.yielded = True                                 # a GNS3 node is the source right now
                s.next_slot = self.slot_after(s, now)
                continue
            if s.yielded:                                        # the node went quiet: fill the hole it left
                s.yielded = False
                self.fill(s, s.ext_last, now, "GNS3 node stopped")
                s.next_slot = self.slot_after(s, now)
            while s.next_slot <= now:                            # scheduled slot(s)
                t = s.next_slot
                if now - t > a.max_fill:
                    s.next_slot = self.slot_after(s, now)
                    break
                self.emit(s, t, self.reading(s, t), "schedule")
                s.next_slot += a.interval
            f = self.faults.get(s.id)
            if f and now - s.last_sample >= a.sample_interval:   # local sampling only matters while a fault is active
                s.last_sample = now
                r = self.reading(s, now)
                crit = self.is_critical(s, r)
                if crit and (not s.critical or now - s.last_tx >= a.fast_interval):
                    self.emit(s, int(now), r, "threshold")       # critical: report immediately
                elif s.critical and not crit:
                    self.emit(s, int(now), r, "recovery")
                s.critical = crit
            elif not f and s.critical:
                s.critical = False
                self.emit(s, int(now), self.reading(s, now), "recovery")

    def run(self):
        a = self.a
        lock = open(os.path.join(a.state_dir, "fleet.lock"), "w")
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            sys.exit("another hre_fleet.py is already running with this --state-dir (two fleets would double the data)")
        if not a.dry_run:
            self.connect()
            time.sleep(1.5)
        now = time.time()
        for s in self.sensors.values():
            s.next_slot = self.slot_after(s, now)
        if a.once:
            for s in self.sensors.values():
                if self.powered(s):
                    self.emit(s, int(now), self.reading(s, now), "schedule")
            self.finish()
            return
        self.db_fill(now)
        print(f"fleet running: {len(self.sensors)} sensors, report every {a.interval:.0f} s, "
              f"yield to GNS3 nodes: {self.yield_ok}, state in {a.state_dir}", flush=True)
        last_stats = last_loop = now
        while self.running and not self.fatal:
            now = time.time()
            self.expire_faults(now)
            self.step(now)
            self.pump_fill()
            if now - last_stats >= 60:
                off = sum(1 for s in self.sensors.values() if not self.powered(s))
                yl = sum(1 for s in self.sensors.values() if s.yielded)
                print(f"[{iso(now)}] sent={self.sent} acked={self.acked} filled={self.filled} queue={len(self.fillq)} "
                      f"yielding_to_gns3={yl} unpowered={off} faults={len(self.faults)} connected={self.connected}", flush=True)
                last_stats = now
                if self.connected and self.sent_since_ack and now - self.last_ack > 300:
                    print("no acknowledgement for 5 minutes while connected: exiting so the service restarts me", flush=True)
                    os._exit(1)
            time.sleep(max(0.0, 1.0 - (time.time() - now)))
        self.finish()

    def finish(self):
        if self.client and not self.fatal:
            deadline = time.time() + 10
            while self.sent_since_ack and time.time() < deadline:
                time.sleep(0.2)
        self.closing = True
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
        if self.fatal:
            print(f"FAILED: {self.fatal}", flush=True)
            sys.exit(1)
        print(f"stopped: {self.sent} packets sent", flush=True)

    def stop(self, *_):
        self.running = False


def main():
    env = os.environ.get
    p = argparse.ArgumentParser(description="HRE sensor fleet (always-on telemetry source)")
    p.add_argument("--host", default=env("MQTT_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(env("MQTT_PORT", "1883")))
    p.add_argument("--user", default=env("MQTT_USER"))
    p.add_argument("--password", default=env("MQTT_PASSWORD"))
    p.add_argument("--municipality", default="all", choices=MUNICIPALITIES + ["all"])
    p.add_argument("--types", default="water,sewer")
    p.add_argument("--wards", default=None, help="override the ward numbers, e.g. 1-5 (default: every ward of each municipality)")
    p.add_argument("--interval", type=float, default=300, help="normal report period, seconds")
    p.add_argument("--sample-interval", type=float, default=30, help="local sampling period while a fault is active")
    p.add_argument("--fast-interval", type=float, default=30, help="repeat period while critical")
    p.add_argument("--no-yield", action="store_true", help="always publish, even if a GNS3 node publishes the same sensor")
    p.add_argument("--yield-grace", type=float, default=450, help="seconds of silence before the fleet takes a sensor back")
    p.add_argument("--dsn", default=env("HRE_DSN"), help="read-only database login, enables gap fill after the fleet was down")
    p.add_argument("--max-fill", type=float, default=7 * 86400, help="never fill more than this far back, seconds")
    p.add_argument("--fill-rate", type=int, default=200, help="gap-fill packets per second")
    p.add_argument("--state-dir", default=env("HRE_FLEET_STATE", os.path.expanduser("~/.hre-fleet")))
    p.add_argument("--once", action="store_true", help="send one report per sensor and exit")
    p.add_argument("--dry-run", action="store_true", help="print packets, do not connect")
    p.add_argument("--node", help="name used in the MQTT client id (default: host name)")
    a = p.parse_args()
    if mqtt is None and not a.dry_run:
        sys.exit("paho-mqtt is not installed: pip install paho-mqtt")
    f = Fleet(a)
    signal.signal(signal.SIGINT, f.stop)
    signal.signal(signal.SIGTERM, f.stop)
    f.run()


if __name__ == "__main__":
    main()
