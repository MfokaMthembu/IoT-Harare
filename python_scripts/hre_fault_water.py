#!/usr/bin/env python3
"""
HRE water fault injector (water VLAN)

Injects water-VLAN faults into the running water nodes of one municipality (or all of them).
It publishes a control message; the node that owns the sensors makes them behave faulty and
the Node-RED edge has to detect it from the telemetry, exactly like a real fault.

Fault kinds (water VLAN only): burst, leak, low_pressure, high_pressure, reservoir_low, supply_loss

Examples
  python3 hre_fault_water.py --municipality harare --kind burst --ward 3 --host 10.0.0.5 --user hre_edge --password SECRET
  python3 hre_fault_water.py --municipality chitungwiza --kind burst --duration 300        # whole municipality, auto-clears
  python3 hre_fault_water.py --municipality harare --clear                                      # clear all water faults there
  python3 hre_fault_water.py --list
Needs an MQTT user allowed to publish hre/sim/# (hre_edge in the iot_lab setup).
"""
import argparse
import json
import os
import sys
import time

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

NODE_TYPE = "water"
KINDS = ['burst', 'leak', 'low_pressure', 'high_pressure', 'reservoir_low', 'supply_loss']
DESCRIPTIONS = {'burst': 'sudden pipe burst: pressure collapses, flow spikes', 'leak': 'progressive leak: pressure sags, flow creeps up (ramp 10 min)', 'low_pressure': 'pressure drops below normal (warning only)', 'high_pressure': 'pressure above the critical limit (pressure-reducing valve failure)', 'reservoir_low': 'reservoir level falls below the critical limit (ramp 15 min)', 'supply_loss': 'supply cut: pressure and flow both collapse'}


def main():
    env = os.environ.get
    p = argparse.ArgumentParser(description="HRE water fault injector (water VLAN)")
    p.add_argument("--municipality", choices=["harare", "chitungwiza", "epworth", "ruwa", "all"])
    p.add_argument("--kind", choices=KINDS)
    p.add_argument("--ward", type=int, help="ward number (default: every ward of the municipality)")
    p.add_argument("--ramp", type=float, help="seconds to reach full effect (0 = sudden); default per kind")
    p.add_argument("--duration", type=float, help="auto-clear after this many seconds")
    p.add_argument("--clear", action="store_true", help="clear faults instead of injecting")
    p.add_argument("--list", action="store_true", help="list this VLAN's fault kinds and exit")
    p.add_argument("--host", default=env("MQTT_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(env("MQTT_PORT", "1883")))
    p.add_argument("--user", default=env("MQTT_USER"))
    p.add_argument("--password", default=env("MQTT_PASSWORD"))
    p.add_argument("--dry-run", action="store_true")
    a = p.parse_args()

    if a.list:
        for k in KINDS:
            print(f"  {k:<14} {DESCRIPTIONS[k]}")
        return
    if not a.municipality or not (a.kind or a.clear):
        p.error("--municipality and --kind (or --clear) are required")
    munis = ["harare", "chitungwiza", "epworth", "ruwa"] if a.municipality == "all" else [a.municipality]

    msgs = []
    for m in munis:
        target = f"{m}-{NODE_TYPE}-{a.ward:02d}" if a.ward else f"{m}-{NODE_TYPE}"
        payload = {"kind": "clear"} if a.clear else {"kind": a.kind}
        if not a.clear:
            if a.ramp is not None:
                payload["ramp"] = a.ramp
            if a.duration:
                payload["duration"] = a.duration
        msgs.append((f"hre/sim/fault/{target}", json.dumps(payload)))

    if a.dry_run:
        for t, pl in msgs:
            print(t, pl)
        return
    if mqtt is None:
        sys.exit("paho-mqtt is not installed: pip install paho-mqtt")
    try:
        c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"hre-fault-{NODE_TYPE}-{os.getpid()}")
    except AttributeError:
        c = mqtt.Client(client_id=f"hre-fault-{NODE_TYPE}-{os.getpid()}")
    if a.user:
        c.username_pw_set(a.user, a.password)
    result = {}
    def on_connect(client, userdata, flags, rc, *extra):
        result["ok"] = (rc == 0) if isinstance(rc, int) else not getattr(rc, "is_failure", False)
        result["rc"] = rc
    c.on_connect = on_connect
    try:
        c.connect(a.host, a.port, keepalive=30)
    except OSError as e:
        sys.exit(f"broker {a.host}:{a.port} unreachable: {e}")
    c.loop_start()
    for _ in range(50):
        if "ok" in result:
            break
        time.sleep(0.1)
    if not result.get("ok"):
        c.loop_stop()
        sys.exit(f"FAILED: broker refused the connection ({result.get('rc')}). Check --user / --password.")
    infos = [c.publish(t, pl, qos=1) for t, pl in msgs]
    for i in infos:
        i.wait_for_publish(5)
    ok = all(i.is_published() for i in infos)
    for (t, pl), i in zip(msgs, infos):
        print(("sent  " if i.is_published() else "NOT SENT ") + t + " " + pl)
    c.loop_stop()
    c.disconnect()
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
