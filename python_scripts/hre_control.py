#!/usr/bin/env python3
"""
HRE simulation control: restore power, clear faults, show what is switched off.

  python3 hre_control.py --action status
  python3 hre_control.py --action power-on                 # restore every loadshedding switch
  python3 hre_control.py --action power-on --scope harare  # only Harare (harare/all and harare/<ward>)
  python3 hre_control.py --action clear-faults
  python3 hre_control.py --action reset-all                # power-on + clear-faults

How it works
  * Power switches are RETAINED messages hre/sim/power/<scope>. "Power on" means deleting them
    (an empty retained message), which also tells every running node immediately.
  * Fault commands are live events and are NEVER retained. A retained "clear" would be replayed
    to a node every time it reconnects and silently wipe the faults it is meant to be showing.
  * Every command is confirmed: the script subscribes to hre/sim/# and waits for the broker to echo
    each message back. If the broker refused it (ACL) or never received it, you get an error and a
    non-zero exit code instead of a false "success".

Needs an MQTT user that can read AND write hre/sim/#  (hre_edge: "topic readwrite hre/sim/#").
Exit codes: 0 confirmed, 1 failed, 2 sent but could not be verified (no read access).
"""
import argparse
import json
import os
import sys
import threading
import time

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None

POWER = "hre/sim/power/"
FAULT = "hre/sim/fault/"


class Control:
    def __init__(self, a):
        self.a = a
        self.connected = threading.Event()
        self.conn_error = None
        self.sub_ok = None
        self.sub_done = threading.Event()
        self.retained = {}                 # topic -> payload text (retained messages found at subscribe time)
        self.echo = set()                  # (topic, payload bytes) seen after we subscribed
        self.collecting = True
        cid = f"hre-control-{os.getpid()}"
        try:
            self.c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=cid)
        except AttributeError:             # paho-mqtt 1.x
            self.c = mqtt.Client(client_id=cid)
        if a.user:
            self.c.username_pw_set(a.user, a.password)
        self.c.on_connect = self.on_connect
        self.c.on_subscribe = self.on_subscribe
        self.c.on_message = self.on_message

    def on_connect(self, client, userdata, flags, rc, *extra):
        ok = (rc == 0) if isinstance(rc, int) else not getattr(rc, "is_failure", False)
        if not ok:
            self.conn_error = str(rc)
        self.connected.set()

    def on_subscribe(self, client, userdata, mid, granted, *extra):
        bad = any((g == 128) if isinstance(g, int) else getattr(g, "is_failure", False) for g in granted)
        self.sub_ok = not bad
        self.sub_done.set()

    def on_message(self, client, userdata, msg):
        if self.collecting:
            if msg.retain and msg.payload:
                self.retained[msg.topic] = msg.payload.decode("utf-8", "ignore")
        else:
            self.echo.add((msg.topic, bytes(msg.payload)))

    # ---------------------------------------------------------------- session
    def open(self):
        a = self.a
        try:
            self.c.connect(a.host, a.port, keepalive=30)
        except OSError as e:
            sys.exit(f"FAILED: broker {a.host}:{a.port} unreachable: {e}")
        self.c.loop_start()
        if not self.connected.wait(10):
            sys.exit("FAILED: no answer from the broker within 10 s")
        if self.conn_error:
            sys.exit(f"FAILED: broker refused the connection ({self.conn_error}). Check --user / --password.")
        self.c.subscribe([(POWER + "#", 1), (FAULT + "#", 1)])
        self.sub_done.wait(5)
        time.sleep(1.5)                    # retained messages arrive right after the subscription
        self.collecting = False

    def close(self):
        self.c.loop_stop()
        self.c.disconnect()

    def publish_all(self, msgs):
        """msgs: [(topic, payload str, retain)] -> returns (acked, echoed)."""
        infos = []
        for topic, payload, retain in msgs:
            infos.append(self.c.publish(topic, payload, qos=1, retain=retain))
        for i in infos:
            i.wait_for_publish(8)
        acked = all(i.is_published() for i in infos)
        echoed = True
        if self.sub_ok:                    # the broker sends our own messages back only if it accepted them
            want = {(t, p.encode()) for t, p, _ in msgs}
            deadline = time.time() + 5
            while time.time() < deadline and not want <= self.echo:
                time.sleep(0.1)
            echoed = want <= self.echo
        return acked, echoed


def in_scope(topic, scope):
    if scope == "all":
        return True
    rest = topic[len(POWER):]
    return rest == scope or rest.startswith(scope + "/")


def main():
    env = os.environ.get
    p = argparse.ArgumentParser(description="HRE simulation control (MQTT)")
    p.add_argument("--host", default=env("MQTT_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(env("MQTT_PORT", "1883")))
    p.add_argument("--user", default=env("MQTT_USER"))
    p.add_argument("--password", default=env("MQTT_PASSWORD"))
    p.add_argument("--action", choices=["power-on", "clear-faults", "reset-all", "status"], default="reset-all")
    p.add_argument("--scope", default="all", help="power scope for power-on: all, harare, harare/all, harare/3")
    a = p.parse_args()
    if mqtt is None:
        sys.exit("paho-mqtt is not installed: pip install paho-mqtt")

    ctl = Control(a)
    ctl.open()

    power_found = {t: v for t, v in ctl.retained.items() if t.startswith(POWER)}
    fault_found = {t: v for t, v in ctl.retained.items() if t.startswith(FAULT)}

    if a.action == "status":
        off = {t[len(POWER):]: v for t, v in power_found.items() if '"grid": false' in v.replace('":false', '": false')}
        print(f"power switches OFF ({len(off)}): " + (", ".join(sorted(off)) or "none"))
        stale = [t[len(FAULT):] for t in fault_found]
        print(f"retained fault commands ({len(stale)}): " + (", ".join(stale) or "none") +
              ("   <- stale, run clear-faults to remove" if stale else ""))
        if ctl.sub_ok is False:
            print("note: this user cannot read hre/sim/#, so the list above may be incomplete")
        ctl.close()
        return

    msgs = []
    if a.action in ("power-on", "reset-all"):
        targets = sorted(t for t in power_found if in_scope(t, a.scope))
        if not targets and a.scope != "all":
            targets = [POWER + a.scope]                    # nothing known: clear the exact switch anyway
        if not targets and a.scope == "all":
            targets = [POWER + "all"]
        msgs += [(t, "", True) for t in targets]           # empty retained message = delete the switch
        print(f"power: clearing {len(targets)} switch(es): " + ", ".join(t[len(POWER):] for t in targets))
    if a.action in ("clear-faults", "reset-all"):
        msgs.append((FAULT + "all", json.dumps({"kind": "clear"}), False))      # live event, not retained
        for t in sorted(fault_found):                       # remove stale retained fault commands
            msgs.append((t, "", True))
        print(f"faults: clear sent to every node; {len(fault_found)} stale retained command(s) removed")

    acked, echoed = ctl.publish_all(msgs)

    verified = None
    if a.action in ("power-on", "reset-all") and ctl.sub_ok:
        # confirm from a fresh connection that no retained switch is left in scope
        ctl.close()
        again = Control(a)
        again.open()
        left = sorted(t for t in again.retained if t.startswith(POWER) and in_scope(t, a.scope))
        again.close()
        verified = not left
        if left:
            print("STILL SWITCHED OFF: " + ", ".join(t[len(POWER):] for t in left))
    else:
        ctl.close()

    if not acked:
        sys.exit("FAILED: the broker did not acknowledge every message")
    if ctl.sub_ok is False:
        print("sent and acknowledged, but this user cannot read hre/sim/#, so delivery could not be verified.\n"
              "Give the user 'topic readwrite hre/sim/#' in the broker ACL for confirmed commands.")
        sys.exit(2)
    if not echoed:
        sys.exit("FAILED: the broker acknowledged but did not accept the messages (check the ACL: this user needs "
                 "write access to hre/sim/#)")
    if verified is False:
        sys.exit("FAILED: some power switches are still on")
    print("confirmed: the broker accepted every command" + (" and no power switch is left off" if verified else ""))


if __name__ == "__main__":
    main()
