#!/usr/bin/env python3
"""
HRE resilience and latency tests. Run on the edge VM while the node scripts and Node-RED are running.

  latency   how fast a fault travels from the sensor to something a dashboard query can see
  sync      municipality loadshedding / uplink cut + a fault injected during it:
            is it detected and spooled at the edge, and forwarded when connectivity returns?
  flap      intermittent service: the uplink of one municipality drops and returns repeatedly

Examples
  python3 hre_tests.py latency --municipality harare --type water --ward 2 --kind burst --trials 5
  python3 hre_tests.py sync    --municipality ruwa --type water --ward 4 --kind burst --outage 60 \
                               --spool-dir ~/.node-red/hre-edge/spool
  python3 hre_tests.py flap    --municipality epworth --cycles 3 --down 20 --up 10

Common options: --mqtt-host/--mqtt-user/--mqtt-password (user needs read hre/telemetry/# and write hre/sim/#),
                --dsn "postgresql://hre_grafana:PASSWORD@host:port/railway" (read-only role is enough).
Requires: pip install paho-mqtt psycopg2-binary
Exit code 0 = PASS, 1 = FAIL.
"""
import argparse
import json
import os
import statistics
import sys
import threading
import time

try:
    import paho.mqtt.client as mqtt
except ImportError:
    sys.exit("pip install paho-mqtt")
try:
    import psycopg2
except ImportError:
    sys.exit("pip install psycopg2-binary")

MUNIS = ["harare", "chitungwiza", "epworth", "ruwa"]


class Rig:
    def __init__(self, a):
        self.a = a
        self.db = psycopg2.connect(a.dsn)
        self.db.autocommit = True
        self.lock = threading.Lock()
        self.packets = []                       # packets seen on the wire: dicts with recv time
        self.connected = threading.Event()
        cid = f"hre-tests-{os.getpid()}"
        try:
            self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid)
        except AttributeError:
            self.c = mqtt.Client(client_id=cid)
        if a.mqtt_user:
            self.c.username_pw_set(a.mqtt_user, a.mqtt_password)
        self.c.on_connect = self._on_connect
        self.c.on_message = self._on_message
        self.err = None

    def _on_connect(self, client, userdata, flags, rc, *extra):
        ok = (rc == 0) if isinstance(rc, int) else not getattr(rc, "is_failure", False)
        if not ok:
            self.err = str(rc)
        else:
            client.subscribe("hre/telemetry/#", 1)
        self.connected.set()

    def _on_message(self, client, userdata, msg):
        try:
            p = json.loads(msg.payload)
            ts = p["ts"].replace("Z", "+00:00")
            from datetime import datetime
            p["_epoch"] = datetime.fromisoformat(ts).timestamp()
            p["_recv"] = time.time()
            p["_muni"] = msg.topic.split("/")[2]
        except Exception:
            return
        with self.lock:
            self.packets.append(p)

    def open(self):
        self.c.connect(self.a.mqtt_host, self.a.mqtt_port, keepalive=30)
        self.c.loop_start()
        if not self.connected.wait(10) or self.err:
            sys.exit(f"MQTT connection failed ({self.err or 'timeout'})")
        self.offset = self.clock_offset()

    def close(self):
        self.c.loop_stop()
        self.c.disconnect()
        self.db.close()

    def q(self, sql, args=()):
        with self.db.cursor() as cur:
            cur.execute(sql, args)
            return cur.fetchall() if cur.description else []

    def scalar(self, sql, args=()):
        r = self.q(sql, args)
        return r[0][0] if r else None

    def clock_offset(self):
        """database clock minus local clock, seconds (round-trip compensated, median of 7)."""
        xs = []
        for _ in range(7):
            t0 = time.time()
            d = float(self.scalar("select extract(epoch from clock_timestamp())"))
            t1 = time.time()
            xs.append(d - (t0 + t1) / 2)
        return statistics.median(xs)

    def publish(self, topic, payload, retain=False):
        info = self.c.publish(topic, payload if isinstance(payload, str) else json.dumps(payload), qos=1, retain=retain)
        info.wait_for_publish(5)
        return info.is_published()

    def fault(self, sensor, kind, ramp=None):
        spec = {"kind": kind}
        if ramp is not None and kind != "clear":
            spec["ramp"] = ramp
        return self.publish(f"hre/sim/fault/{sensor}", spec)

    def uplink(self, lane, **kw):
        return self.publish(f"hre/sim/uplink/{lane}", dict(kw))

    def wait_for(self, fn, timeout, step=0.05):
        t_end = time.time() + timeout
        while time.time() < t_end:
            v = fn()
            if v:
                return v
            time.sleep(step)
        return None

    def packets_since(self, t0, muni=None, until=None):
        with self.lock:
            return [p for p in self.packets if p["_recv"] >= t0 and (until is None or p["_recv"] <= until)
                    and (muni is None or p["_muni"] == muni)]


def sensor_id(a):
    return f"{a.municipality}-{a.type}-{a.ward:02d}"


def row(label, value, unit="s"):
    print(f"  {label:<46}{value:>10.2f} {unit}" if isinstance(value, float) else f"  {label:<46}{value!s:>10}")


def completeness(rig, muni, t_from, t_until):
    """every reading seen on the wire in the window must be in the database exactly once."""
    pk = rig.packets_since(t_from, muni, t_until)
    expected = {(p["sensor_id"], m, int(p["_epoch"])) for p in pk for m in p["readings"]}
    if not expected:
        return 0, 0, 0
    lo, hi = min(e[2] for e in expected), max(e[2] for e in expected)
    got = rig.q("select sensor_id, metric_type::text, extract(epoch from time)::bigint from telemetry "
                "where sensor_id like %s and time between to_timestamp(%s) and to_timestamp(%s)", (muni + "-%", lo, hi))
    got = {(s, m, int(t)) for s, m, t in got}
    missing = expected - got
    dup = rig.scalar("select count(*) from (select 1 from telemetry where sensor_id like %s and time between "
                     "to_timestamp(%s) and to_timestamp(%s) group by sensor_id, metric_type, time having count(*) > 1) x",
                     (muni + "-%", lo, hi))
    return len(expected), len(missing), int(dup)


# ------------------------------------------------------------------------ latency
def test_latency(rig, a):
    sid = sensor_id(a)
    print(f"\nFAULT DETECTION LATENCY: {a.kind} on {sid}, {a.trials} trial(s), ramp {a.ramp}s")
    print(f"database clock is {rig.offset:+.3f} s from this machine (corrected below)\n")
    rows = []
    for n in range(1, a.trials + 1):
        last = rig.scalar("select coalesce(max(alert_id), 0) from alert where sensor_id = %s", (sid,))
        t0 = time.time()
        rig.fault(sid, a.kind, a.ramp)
        hit = rig.wait_for(lambda: rig.q(
            "select extract(epoch from time), extract(epoch from received_at), severity, source, fault_type "
            "from alert where sensor_id = %s and alert_id > %s and severity <> 'cleared' order by alert_id limit 1",
            (sid, last)), a.timeout)
        t_seen = time.time()
        if not hit:
            print(f"  trial {n}: NOT detected within {a.timeout:.0f} s")
            rows.append(None)
        else:
            a_time, a_recv, sev, src, ft = hit[0]
            a_time, a_recv = float(a_time), float(a_recv) - rig.offset
            r = dict(sensor=max(0.0, a_time - t0), pipeline=max(0.0, a_recv - a_time), seen=max(0.0, t_seen - a_recv),
                     total=t_seen - t0)
            rows.append(r)
            print(f"  trial {n}: {r['total']:6.2f} s total  =  sensor {r['sensor']:5.2f} + edge/MQTT/DB {r['pipeline']:5.2f}"
                  f" + visible {r['seen']:4.2f}   [{ft}, {sev}, source={src}]")
        rig.fault(sid, "clear")
        rig.wait_for(lambda: rig.scalar("select 1 from alert where sensor_id=%s and alert_id>%s and severity='cleared' limit 1",
                                        (sid, last)), a.timeout)
        time.sleep(3)
    ok = [r for r in rows if r]
    print()
    if not ok:
        print("RESULT: FAIL, no fault was detected")
        return False
    for key, label in (("total", "fault -> visible to a dashboard query"), ("sensor", "  sensor notices + transmits"),
                       ("pipeline", "  edge + MQTT + validate + database"), ("seen", "  database -> query sees it")):
        v = [r[key] for r in ok]
        print(f"  {label:<44} min {min(v):5.2f}  mean {statistics.mean(v):5.2f}  max {max(v):5.2f} s")
    worst = max(r["total"] for r in ok) + a.refresh
    print(f"\n  A dashboard shows it at the next auto-refresh: add up to {a.refresh:.0f} s -> worst case on screen about {worst:.0f} s")
    print("  The sensor part is bounded by the node's --sample-interval (default 30 s); the rest is the pipeline.")
    print(f"RESULT: {'PASS' if len(ok) == len(rows) else 'PARTIAL'}: {len(ok)}/{len(rows)} detected")
    return len(ok) == len(rows)


# ---------------------------------------------------------------------------- sync
def test_sync(rig, a):
    sid, muni = sensor_id(a), a.municipality
    control = a.control or next(m for m in MUNIS if m != muni)
    print(f"\nLOADSHEDDING + FAULT + SYNC: {muni} uplink cut for {a.outage:.0f} s, {a.kind} injected on {sid} (control: {control})\n")
    ok = True
    t_win = time.time()
    rig.fault(sid, "clear")
    base_ctl = rig.scalar("select count(*) from telemetry where sensor_id like %s", (control + "-%",))
    t_cut = time.time()
    rig.uplink(muni, down=True, cause=a.cause)
    time.sleep(4)
    t_fault = time.time()
    rig.fault(sid, a.kind, 0)
    sent = rig.wait_for(lambda: [p for p in rig.packets_since(t_fault, muni) if p["sensor_id"] == sid and p.get("trigger") == "threshold"], 60)
    if sent:
        print(f"  the sensor reported the fault {sent[0]['_recv'] - t_fault:.1f} s after injection (immediate critical report)")
    else:
        print("  FAIL: the sensor never reported the fault")
        ok = False
    time.sleep(max(0, a.outage - (time.time() - t_cut)) / 2)
    in_db = rig.scalar("select count(*) from telemetry where sensor_id like %s and received_at > to_timestamp(%s)",
                       (muni + "-%", t_cut + rig.offset))
    alert_db = rig.scalar("select count(*) from alert where sensor_id=%s and received_at > to_timestamp(%s)", (sid, t_cut + rig.offset))
    ctl_db = rig.scalar("select count(*) from telemetry where sensor_id like %s", (control + "-%",)) - base_ctl
    print(f"  during the outage: {muni} rows reaching the database = {in_db} (expect 0) | fault alert in database = {alert_db} (expect 0)")
    print(f"  during the outage: {control} (uplink up) kept flowing: +{ctl_db} rows (expect > 0)")
    ok &= in_db == 0 and alert_db == 0 and ctl_db > 0
    if a.spool_dir:
        cur = os.path.join(os.path.expanduser(a.spool_dir), muni, "current.ndjson")
        counts = {"telemetry": 0, "alert": 0, "edge_event": 0}
        has_fault = False
        if os.path.exists(cur):
            for line in open(cur):
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                counts[r["t"]] = counts.get(r["t"], 0) + 1
                has_fault |= r["t"] == "alert" and r["r"].get("sensor_id") == sid and r["r"].get("severity") != "cleared"
        print(f"  local JSON spool {cur}: {counts['telemetry']} telemetry, {counts['alert']} alert, {counts['edge_event']} event rows"
              f" | contains the fault alert: {has_fault}")
        ok &= has_fault and counts["telemetry"] > 0
    time.sleep(max(0, a.outage - (time.time() - t_cut)))
    t_restore = time.time()
    rig.uplink(muni, down=False, cause=a.cause)
    vis = rig.wait_for(lambda: rig.q("select extract(epoch from time), extract(epoch from received_at), fault_type, source from alert "
                                     "where sensor_id=%s and received_at > to_timestamp(%s) and severity<>'cleared' order by alert_id limit 1",
                                     (sid, t_cut + rig.offset)), 60)
    t_vis = time.time()
    if vis:
        a_time, a_recv, ft, src = float(vis[0][0]), float(vis[0][1]) - rig.offset, vis[0][2], vis[0][3]
        print(f"\n  uplink restored -> fault alert visible in the database after {t_vis - t_restore:.1f} s")
        print(f"  the alert is a '{ft}' (source={src}); detected at {t_fault - t_cut + (a_time - t_fault):.0f} s into the outage, "
              f"delivered {a_recv - a_time:.0f} s after it happened")
        ok &= (a_recv - a_time) >= a.outage / 2
    else:
        print("\n  FAIL: the fault alert never reached the database after the uplink was restored")
        ok = False
    ev = rig.wait_for(lambda: rig.q("select detail->>'rows', detail->>'oldest' from edge_event where municipality=%s and event_type='sync_completed' "
                                    "and received_at > to_timestamp(%s) order by event_id desc limit 1", (muni, t_restore + rig.offset)), 30)
    print(f"  sync_completed event: {('%s rows, oldest record %s' % ev[0]) if ev else 'MISSING'}")
    ok &= bool(ev)
    types = [r[0] for r in rig.q("select event_type from edge_event where municipality=%s and received_at > to_timestamp(%s) order by time",
                                 (muni, t_cut + rig.offset - 5))]
    print(f"  {muni} events in order: {', '.join(types)}")
    others = rig.scalar("select count(*) from edge_event where municipality<>%s and event_type like 'uplink%%' and received_at > to_timestamp(%s)",
                        (muni, t_cut + rig.offset - 5))
    print(f"  uplink events logged for OTHER municipalities: {others} (expect 0)")
    ok &= others == 0 and "uplink_down" in types and "uplink_up" in types
    rig.fault(sid, "clear")
    time.sleep(8)
    t_end = time.time()
    exp, miss, dup = completeness(rig, muni, t_win, t_end - 6)
    print(f"\n  completeness: {exp} readings seen on the wire, {miss} missing from the database, {dup} duplicated")
    ok &= miss == 0 and dup == 0
    print(f"RESULT: {'PASS' if ok else 'FAIL'}")
    return ok


# ---------------------------------------------------------------------------- flap
def test_flap(rig, a):
    muni = a.municipality
    print(f"\nINTERMITTENT SERVICE: {muni} uplink {a.cycles}x ({a.down:.0f} s down / {a.up:.0f} s up)\n")
    t0 = time.time()
    last_ev = rig.scalar("select coalesce(max(event_id),0) from edge_event")
    rig.uplink(muni, flap={"cycles": a.cycles, "down_s": a.down, "up_s": a.up})
    total = a.cycles * (a.down + a.up)
    time.sleep(total + a.settle)
    t_end = time.time()
    ev = rig.q("select event_type, municipality from edge_event where event_id > %s order by time", (last_ev,))
    mine = [e[0] for e in ev if e[1] == muni]
    downs, ups = mine.count("uplink_down"), mine.count("uplink_up")
    others = [e for e in ev if e[1] not in (muni, None) and e[0].startswith("uplink")]
    syncs = mine.count("sync_completed")
    print(f"  {muni}: {downs} uplink_down, {ups} uplink_up, {syncs} sync_completed (expect {a.cycles}/{a.cycles}/>=1)")
    print(f"  other municipalities' uplink events: {len(others)} (expect 0)")
    exp, miss, dup = completeness(rig, muni, t0, t_end - a.settle / 2)
    print(f"  completeness: {exp} readings seen on the wire, {miss} missing from the database, {dup} duplicated")
    if a.spool_dir:
        left = os.listdir(os.path.join(os.path.expanduser(a.spool_dir), muni)) if os.path.isdir(os.path.join(os.path.expanduser(a.spool_dir), muni)) else []
        print(f"  spool folder after the last restore: {left or 'empty'}")
    ok = downs == a.cycles and ups == a.cycles and syncs >= 1 and not others and miss == 0 and dup == 0
    print(f"RESULT: {'PASS' if ok else 'FAIL'}")
    return ok


def main():
    env = os.environ.get
    p = argparse.ArgumentParser(description="HRE latency and resilience tests")
    p.add_argument("test", choices=["latency", "sync", "flap"])
    p.add_argument("--municipality", required=True, choices=MUNIS)
    p.add_argument("--type", choices=["water", "sewer"], default="water")
    p.add_argument("--ward", type=int, default=2)
    p.add_argument("--kind", default="burst", help="fault kind valid for --type (see hre_fault_<type>.py --list)")
    p.add_argument("--ramp", type=float, default=0, help="fault ramp seconds (0 = sudden)")
    p.add_argument("--trials", type=int, default=5)
    p.add_argument("--timeout", type=float, default=90)
    p.add_argument("--refresh", type=float, default=30, help="dashboard auto-refresh interval, seconds")
    p.add_argument("--outage", type=float, default=60, help="sync test: seconds the uplink stays down")
    p.add_argument("--cause", choices=["loadshedding", "network"], default="loadshedding")
    p.add_argument("--control", choices=MUNIS, help="sync test: municipality that must keep flowing (default: another one)")
    p.add_argument("--cycles", type=int, default=3)
    p.add_argument("--down", type=float, default=20)
    p.add_argument("--up", type=float, default=10)
    p.add_argument("--settle", type=float, default=30, help="flap test: seconds to wait after the last restore")
    p.add_argument("--spool-dir", help="edge spool folder (~/.node-red/hre-edge/spool) to inspect the local JSON file")
    p.add_argument("--mqtt-host", default=env("MQTT_HOST", "127.0.0.1"))
    p.add_argument("--mqtt-port", type=int, default=int(env("MQTT_PORT", "1883")))
    p.add_argument("--mqtt-user", default=env("MQTT_USER"))
    p.add_argument("--mqtt-password", default=env("MQTT_PASSWORD"))
    p.add_argument("--dsn", default=env("HRE_DSN"), help="postgresql://user:password@host:port/dbname")
    a = p.parse_args()
    if not a.dsn:
        sys.exit("--dsn (or HRE_DSN) is required")
    rig = Rig(a)
    rig.open()
    try:
        ok = {"latency": test_latency, "sync": test_sync, "flap": test_flap}[a.test](rig, a)
    finally:
        rig.close()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

