#!/usr/bin/env python3
"""
HRE water node: water-VLAN telemetry emulator (MQTT)  (VLAN: water)

One copy per topology node (a municipality has one water node and one sewer node). This node behaves like a water sensor cluster: it publishes ONLY
water telemetry for its municipality and accepts ONLY water faults, mirroring the
one-VLAN-per-service design. Run it for several municipalities at the same time.

Reporting (battery friendly, matches the Node-RED alert logic)
  * normal report every --interval seconds (default 300 = 5 min)
  * local sampling every --sample-interval seconds (default 30), no radio
  * a CRITICAL sample is sent immediately (trigger="threshold") and repeated every
    --fast-interval seconds until it recovers, then one recovery report is sent
  * warnings are never sent early: they wait for the next scheduled report

Fault catalogue for this VLAN: burst, leak, low_pressure, high_pressure, reservoir_low, supply_loss

Topics
  publish    hre/telemetry/<municipality>/<sensor_id>        QoS 1
  subscribe  hre/sim/power/#   retained loadshedding switches (simulation)
             hre/sim/fault/#   fault injection: target = <sensor_id> | <municipality>-water | all
                               payload {"kind": "...", "ramp": seconds?, "duration": seconds?} or {"kind":"clear"}

Examples
  python3 hre_water_node.py --municipality harare --dry-run --once
  python3 hre_water_node.py --municipality harare --host 10.0.0.5 --user hre_sensor --password SECRET
  python3 hre_water_node.py --municipality epworth --host 10.0.0.5 --user hre_sensor --password SECRET --fault burst:epworth-water-03
"""
import argparse
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

NODE_TYPE = "water"
MUNICIPALITIES = ["harare", "chitungwiza", "epworth", "ruwa"]
WARD_COUNT = {"harare": 45, "chitungwiza": 25, "epworth": 7, "ruwa": 18}   # wards that have sensors
HARARE_TZ = timezone(timedelta(hours=2))

# Limits carried by the sensor firmware. Must match the `baseline` table.
BASELINE = {
    "pressure":  dict(nmin=2.0,  nmax=4.0,  cmin=1.0,  cmax=5.5),
    "flow_rate": dict(nmin=8.0,  nmax=16.0, cmin=2.0,  cmax=25.0),
    "level":     dict(nmin=40.0, nmax=90.0, cmin=20.0, cmax=98.0),
}

# Fault models for this VLAN. effects: metric -> target value (from that metric's limits).
# ramp = seconds to reach the target (0 = sudden). Can be overridden per injection.
FAULTS = {
    "burst":         {"ramp": 0,   "effects": {"pressure": lambda b: b["cmin"] * 0.3,
                                               "flow_rate": lambda b: b["cmax"] * 1.1}},
    "leak":          {"ramp": 600, "effects": {"pressure": lambda b: b["cmin"] - 0.2 * (b["nmin"] - b["cmin"]),
                                               "flow_rate": lambda b: b["nmax"] + 0.5 * (b["cmax"] - b["nmax"])}},
    "low_pressure":  {"ramp": 0,   "effects": {"pressure": lambda b: b["nmin"] - 0.5 * (b["nmin"] - b["cmin"])}},
    "high_pressure": {"ramp": 0,   "effects": {"pressure": lambda b: b["cmax"] + 0.1 * (b["cmax"] - b["nmax"])}},
    "reservoir_low": {"ramp": 900, "effects": {"level": lambda b: b["cmin"] - 0.2 * (b["nmin"] - b["cmin"])}},
    "supply_loss":   {"ramp": 0,   "effects": {"pressure": lambda b: b["cmin"] - 0.2 * (b["nmin"] - b["cmin"]),
                                               "flow_rate": lambda b: b["cmin"] * 0.25}},
}


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


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


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


class Sensor:
    def __init__(self, muni, ward):
        self.muni, self.ward = muni, ward
        self.id = f"{muni}-{NODE_TYPE}-{ward:02d}"
        self.next_report = 0.0
        self.last_tx = 0.0
        self.last_sample = 0.0
        self.critical = False
        self.was_off = False

    def readings(self, fault, now):
        model = FAULTS.get(fault["kind"]) if fault else None
        if fault and fault.get("ramp", model["ramp"]) > 0:
            p = min(1.0, (now - fault["start"]) / fault["ramp"])
        else:
            p = 1.0
        out = {}
        for metric, b in BASELINE.items():
            v = normal_value(self.id, NODE_TYPE, metric, b, now)
            if model and metric in model["effects"]:
                v = v + p * (model["effects"][metric](b) - v)
            out[metric] = round(max(0.0, v), 2)
        return out

    def is_critical(self, readings):
        return any(v < BASELINE[m]["cmin"] or v > BASELINE[m]["cmax"] for m, v in readings.items())


class Node:
    def __init__(self, args):
        self.args = args
        self.sensors = [Sensor(args.municipality, w) for w in parse_wards(args.wards)]
        self.power = {}      # scope -> False, from retained hre/sim/power/#
        self.faults = {}     # sensor_id -> {"kind", "start", "ramp", "until"}
        self.sent = 0
        self.dropped = 0
        self.infos = []
        self.fatal = None
        self.running = True
        self.client = None
        self.cid = f"hre-{NODE_TYPE}-{args.municipality}-{args.node or socket.gethostname()}-{os.getpid()}"
        for spec in args.fault or []:
            kind, sid = spec.split(":")
            self.apply_fault(sid, {"kind": kind})

    # ------------------------------------------------------------------ faults
    def apply_fault(self, target, spec):
        kind = spec.get("kind")
        mine = {s.id for s in self.sensors}
        ids = mine if target == "all" else (mine if target == f"{self.args.municipality}-{NODE_TYPE}" else ({target} & mine))
        if not ids:
            return
        if kind == "clear":
            for i in ids:
                self.faults.pop(i, None)
            print(f"fault cleared: {target}", flush=True)
            return
        if kind not in FAULTS:
            print(f"ignored: '{kind}' is not a {NODE_TYPE} fault (this node's VLAN: {', '.join(FAULTS)})", flush=True)
            return
        now = time.time()
        ramp = float(spec["ramp"]) if spec.get("ramp") is not None else FAULTS[kind]["ramp"]
        until = now + float(spec["duration"]) if spec.get("duration") else None
        for i in ids:
            self.faults[i] = {"kind": kind, "start": now, "ramp": ramp, "until": until}
        print(f"FAULT '{kind}' on {target} ({len(ids)} sensor(s), ramp {ramp:.0f}s)", flush=True)

    # -------------------------------------------------------------------- MQTT
    def connect(self):
        a = self.args
        try:
            self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=self.cid)
        except AttributeError:                                  # paho-mqtt 1.x
            self.client = mqtt.Client(client_id=self.cid)
        if a.user:
            self.client.username_pw_set(a.user, a.password)
        self.client.will_set(f"hre/status/{self.cid}", "offline", qos=1, retain=True)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.reconnect_delay_set(1, 30)
        t0 = time.time()
        while self.running:
            try:
                self.client.connect(a.host, a.port, keepalive=30)
                break
            except OSError as e:
                print(f"broker {a.host}:{a.port} unreachable ({e}); retrying in 3 s", flush=True)
                if a.once and time.time() - t0 > 15:
                    self.fatal = f"broker {a.host}:{a.port} unreachable"
                    return
                time.sleep(3)
        self.client.loop_start()

    def on_connect(self, client, userdata, flags, rc, *extra):
        ok = (rc == 0) if isinstance(rc, int) else not getattr(rc, "is_failure", False)
        if not ok:
            self.fatal = f"broker refused the connection: {rc}"
            print(f"ERROR: {self.fatal}. Check --user / --password and that the user exists in the broker's "
                  f"password file.", flush=True)
            return
        client.subscribe([("hre/sim/power/#", 1), ("hre/sim/fault/#", 1)])
        client.publish(f"hre/status/{self.cid}", "online", qos=1, retain=True)
        print(f"connected to {self.args.host}:{self.args.port} as {self.cid} | "
              f"{len(self.sensors)} {NODE_TYPE} sensors in {self.args.municipality}", flush=True)

    def on_message(self, client, userdata, msg):
        topic, payload = msg.topic, msg.payload.decode("utf-8", "ignore").strip()
        if topic.startswith("hre/sim/power/"):
            scope = topic[len("hre/sim/power/"):]               # all | <muni>/all | <muni>/<ward>
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
            if msg.retain:                                      # stale retained command: faults are live events only
                return
            try:
                spec = json.loads(payload)
            except ValueError:
                return
            if isinstance(spec, dict):
                self.apply_fault(topic[len("hre/sim/fault/"):], spec)

    # --------------------------------------------------------------- behaviour
    def powered(self, s):
        return not any(self.power.get(k) is False for k in ("all", f"{s.muni}/all", f"{s.muni}/{s.ward}"))

    def transmit(self, s, readings, trigger):
        packet = {"sensor_id": s.id, "ts": now_iso(), "readings": readings, "trigger": trigger}
        s.last_tx = time.time()
        if self.args.dry_run:
            self.sent += 1
            print(f"hre/telemetry/{s.muni}/{s.id} {json.dumps(packet)}", flush=True)
            return
        if not self.client.is_connected():
            self.dropped += 1
            return
        info = self.client.publish(f"hre/telemetry/{s.muni}/{s.id}", json.dumps(packet), qos=self.args.qos)
        self.sent += 1
        if self.args.once:
            self.infos.append(info)

    def step(self, now):
        a = self.args
        for s in self.sensors:
            if not self.powered(s):
                s.was_off = True
                continue
            if s.was_off:                                       # power is back: report soon after "boot"
                s.was_off = False
                s.next_report = now + random.uniform(1, 5)
                s.critical = False
            fault = self.faults.get(s.id)
            if fault and fault["until"] and now >= fault["until"]:
                del self.faults[s.id]
                fault = None
                print(f"fault on {s.id} ended (duration elapsed)", flush=True)
            if now >= s.next_report:                            # scheduled report
                r = s.readings(fault, now)
                crit = s.is_critical(r)
                self.transmit(s, r, "threshold" if crit else ("recovery" if s.critical else "schedule"))
                s.critical, s.next_report, s.last_sample = crit, now + a.interval, now
            elif now - s.last_sample >= a.sample_interval:      # local sample, radio only if critical
                s.last_sample = now
                r = s.readings(fault, now)
                crit = s.is_critical(r)
                if crit and (not s.critical or now - s.last_tx >= a.fast_interval):
                    self.transmit(s, r, "threshold")            # critical: report immediately
                elif s.critical and not crit:
                    self.transmit(s, r, "recovery")             # one report to close the fault
                    s.next_report = now + a.interval
                s.critical = crit

    def run(self):
        a = self.args
        if not a.dry_run:
            self.connect()
            time.sleep(1)
        now = time.time()
        for s in self.sensors:
            if a.once:
                s.next_report = now
            elif a.no_stagger:
                s.next_report = now + 1
            else:                                               # spread the fleet across the interval
                s.next_report = now + random.uniform(0, a.interval)
        last_stats = now
        while self.running and not self.fatal:
            now = time.time()
            self.step(now)
            if a.once:
                break
            if now - last_stats >= 60:
                off = sum(1 for s in self.sensors if not self.powered(s))
                print(f"[{now_iso()}] sent={self.sent} sensors={len(self.sensors)} unpowered={off} "
                      f"faults={len(self.faults)}", flush=True)
                last_stats = now
            time.sleep(0.25)
        undelivered = 0
        if self.client and not self.fatal:
            deadline = time.time() + 10                         # let QoS 1 acks drain
            while self.infos and time.time() < deadline and not all(i.is_published() for i in self.infos):
                time.sleep(0.2)
            undelivered = sum(1 for i in self.infos if not i.is_published())
        if self.client:
            self.client.loop_stop()
            self.client.disconnect()
        failed = self.dropped + undelivered
        if self.fatal:
            print(f"FAILED: {self.fatal}. Nothing was delivered.", flush=True)
            sys.exit(1)
        print(f"done: {self.sent - undelivered} delivered, {failed} not delivered", flush=True)
        if failed:
            sys.exit(1)

    def stop(self, *_):
        self.running = False


def main():
    env = os.environ.get
    p = argparse.ArgumentParser(description="HRE water node: water-VLAN telemetry emulator (MQTT)")
    p.add_argument("--municipality", required=True, choices=MUNICIPALITIES)
    p.add_argument("--host", default=env("MQTT_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(env("MQTT_PORT", "1883")))
    p.add_argument("--user", default=env("MQTT_USER"))
    p.add_argument("--password", default=env("MQTT_PASSWORD"))
    p.add_argument("--wards", default=None, help="ward numbers to simulate, e.g. 1-5 or 1,3,7-9 (default: every ward of the municipality)")
    p.add_argument("--interval", type=float, default=300, help="normal report period, seconds")
    p.add_argument("--sample-interval", type=float, default=30, help="local sampling period, seconds")
    p.add_argument("--fast-interval", type=float, default=30, help="repeat period while critical, seconds")
    p.add_argument("--qos", type=int, default=1, choices=[0, 1])
    p.add_argument("--once", action="store_true", help="send one report per sensor and exit")
    p.add_argument("--no-stagger", action="store_true", help="all sensors report together (burst test)")
    p.add_argument("--dry-run", action="store_true", help="print packets, do not connect")
    p.add_argument("--fault", action="append", metavar="KIND:SENSOR_ID",
                   help="start with a fault, e.g. burst:harare-water-03  (kinds: burst, leak, low_pressure, high_pressure, reservoir_low, supply_loss)")
    p.add_argument("--node", help="name of this topology node (used in the MQTT client id)")
    args = p.parse_args()
    if mqtt is None and not args.dry_run:
        sys.exit("paho-mqtt is not installed: pip install paho-mqtt")
    args.wards = args.wards or f"1-{WARD_COUNT[args.municipality]}"
    node = Node(args)
    signal.signal(signal.SIGINT, node.stop)
    signal.signal(signal.SIGTERM, node.stop)
    node.run()


if __name__ == "__main__":
    main()
