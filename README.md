# IoT-Harare

**An IoT-based monitoring network for real-time detection of water distribution and sewage system faults in Harare.**

BSc (Honours) Computing dissertation artefact by Edwell Dube (2230237), Botho University.

The artefact is a simulated, city-scale monitoring network for Harare, Chitungwiza, Epworth and Ruwa. There are 190 simulated sensors, one water sensor and one sewer sensor in each of 95 wards. Readings travel through this pipeline:

```
Python sensor scripts (Docker nodes in GNS3 / hre-fleet service)
        │  MQTT, QoS 1, TCP 1883
        ▼
Mosquitto broker ──► Node-RED (validate → detect faults → store-and-forward)
   (edge Ubuntu VM, 172.20.200.1)
        │  SQL
        ▼
PostgreSQL + TimescaleDB (cloud, Railway) ──► Grafana dashboards
```

> **Placeholders:** values in `<angle brackets>` are specific to your copy of the project, such as passwords, the database link and exact script arguments. Replace them before following the steps.

---

## Contents

1. [Prerequisites](#1-prerequisites)
2. [Importing the artefact](#2-importing-the-artefact)
3. [Connecting GNS3 to the edge VM](#3-connecting-gns3-to-the-edge-vm-tap-interface)
4. [Installing Mosquitto (MQTT)](#4-installing-mosquitto-mqtt-on-the-ubuntu-vm)
5. [Installing Node-RED](#5-installing-node-red-on-the-ubuntu-vm)
6. [Connecting to the database](#6-connecting-to-the-database)
7. [Running the sensor simulation](#7-running-the-sensor-simulation)
8. [Testing the system](#8-testing-the-system)
9. [Troubleshooting](#9-troubleshooting)

---

## 1. Prerequisites

| Component | Version / note |
|---|---|
| Host | Ubuntu 22.04 or 24.04 VM (the **edge unit**), at least 4 vCPU and 8 GB RAM |
| GNS3 | GNS3 GUI and server 2.2.x, with Docker support enabled |
| Router image | Cisco IOSv (licensed, so **not included** in this repository) |
| Docker | Docker Engine, for the sensor-node containers |
| Python | Python 3.10+ with `paho-mqtt` |
| Database client | `psql` (package `postgresql-client`) |

---

## 2. Importing the artefact

### 2.1 Clone the repository

```bash
git clone https://github.com/MfokaMthembu/IoT-Harare.git
cd IoT-Harare
```

### 2.2 Add the Cisco IOSv router image to GNS3

In GNS3, go to **Edit → Preferences → QEMU VMs → New** and select your IOSv `.qcow2` image. Give the template the name used in the project (`<IOSv template name>`).

### 2.3 Load the sensor-node Docker image

The sensor nodes run from a committed image named `gns3/telemetry-node:1`. It contains Ubuntu, Python 3, Paho MQTT, the Mosquitto clients, iproute2, iputils-ping and iperf3.

```bash
# If the image is supplied as an archive:
docker load -i <telemetry-node.tar>

# Confirm that it is available:
docker images | grep telemetry-node
```

In GNS3, go to **Edit → Preferences → Docker containers → New**, select `gns3/telemetry-node:1`, and set **1 adapter**.

> The image must be committed. GNS3 recreates containers from their original image whenever a project is closed, so packages installed by hand inside a running container are lost.

### 2.4 Import the GNS3 project

Go to **File → Import portable project** and choose `<IoT-Harare.gns3project>`.

The topology contains:

- **Core routers:** H-Metro-01, H-Metro-02 and H-Metro-03, in a meshed backbone.
- **Municipality routers:** Harare, Chitungwiza, Epworth and Ruwa, each with an 802.1Q trunk on `Fa0/0` to its own switch.
- **Sensor nodes:** Docker containers on the water and sewer access ports.
- **Cloud node:** bound to `tap1` and linked to H-Metro-01 `Fa0/0` (see Section 3).

The router configurations are stored in the routers within the project, with copies in `<configs/>`.

### 2.5 Addressing summary

| Municipality | Mgmt VLAN 99 | Water VLAN | Sewer VLAN |
|---|---|---|---|
| Harare | 172.20.1.0/28 | 110: 172.20.10.0/26 | 111: 172.20.10.64/26 |
| Chitungwiza | 172.20.2.0/28 | 120: 172.20.20.0/27 | 121: 172.20.20.32/27 |
| Epworth | 172.20.3.0/28 | 130: 172.20.30.0/28 | 131: 172.20.30.16/28 |
| Ruwa | 172.20.4.0/28 | 140: 172.20.40.0/27 | 141: 172.20.40.32/27 |

- **Router IDs (Loopback0):** 172.20.99.1–.3 for the core routers and 172.20.99.11–.14 for the municipality routers.
- **Exit link:** H-Metro-01 is 172.20.200.2/30 and the edge host (broker) is 172.20.200.1/30.

---

## 3. Connecting GNS3 to the edge VM (TAP interface)

The routers reach the broker through a dedicated TAP interface on the edge VM.

### 3.1 Set up the TAP interface

```bash
# Create tap1 and give it the edge address
sudo ip tuntap add dev tap1 mode tap user $USER
sudo ip addr add 172.20.200.1/30 dev tap1
sudo ip link set tap1 up

# Route the whole sensor network back through H-Metro-01
sudo ip route add 172.20.0.0/16 via 172.20.200.2 dev tap1

# Let the routers reach the internet through the VM (optional)
sudo sysctl -w net.ipv4.ip_forward=1
sudo iptables -t nat -C POSTROUTING -s 172.20.0.0/16 -o <uplink-iface> -j MASQUERADE 2>/dev/null \
  || sudo iptables -t nat -A POSTROUTING -s 172.20.0.0/16 -o <uplink-iface> -j MASQUERADE
```

The `-C … || -A …` pattern checks for the rule before adding it, so running the commands again does not create duplicate rules.

### 3.2 Recreate it at boot

Commands typed by hand are lost on reboot. In the artefact, a systemd unit (`<hre-tap.service>`) runs these commands at boot, and it must start **before** Mosquitto:

```bash
sudo systemctl enable --now <hre-tap.service>
```

### 3.3 Check the connection

```bash
ping -c 3 172.20.200.2   # H-Metro-01 should reply
```

---

## 4. Installing Mosquitto (MQTT) on the Ubuntu VM

### 4.1 Install

```bash
sudo apt update
sudo apt install -y mosquitto mosquitto-clients
```

### 4.2 Configure listeners and authentication

Create `/etc/mosquitto/conf.d/hre.conf`:

```conf
# Sensors connect over the GNS3 network; Node-RED connects locally
listener 1883 172.20.200.1
listener 1883 127.0.0.1

allow_anonymous false
password_file /etc/mosquitto/passwd
acl_file      /etc/mosquitto/acl

persistence true
```

### 4.3 Create the accounts

Each account is a username and password, and the topic ACL file controls what it may do.

```bash
sudo mosquitto_passwd -c /etc/mosquitto/passwd nodered          # -c only for the FIRST user
sudo mosquitto_passwd    /etc/mosquitto/passwd <sim-control-user>
sudo mosquitto_passwd    /etc/mosquitto/passwd <harare-sensor-user>
sudo mosquitto_passwd    /etc/mosquitto/passwd <chitungwiza-sensor-user>
sudo mosquitto_passwd    /etc/mosquitto/passwd <epworth-sensor-user>
sudo mosquitto_passwd    /etc/mosquitto/passwd <ruwa-sensor-user>
```

### 4.4 Restrict each account's topics

Create `/etc/mosquitto/acl`. Each client may use only its own topics, and only the simulation-control user may publish to `hre/sim/#`.

```conf
# Node-RED: reads everything, writes status
user nodered
topic read  hre/#
topic write hre/status/#

# Simulation control (fault injection, load-shedding, uplink tests)
user <sim-control-user>
topic write hre/sim/#
topic read  hre/#

# Example sensor account: Harare only
user <harare-sensor-user>
topic write hre/telemetry/harare/#
topic write hre/labels/harare/#
topic read  hre/sim/#
topic write hre/status/#
# …repeat for chitungwiza, epworth and ruwa
```

### 4.5 Restart and check

```bash
sudo chown mosquitto:mosquitto /etc/mosquitto/passwd /etc/mosquitto/acl
sudo chmod 640 /etc/mosquitto/passwd /etc/mosquitto/acl
sudo systemctl restart mosquitto && sudo systemctl enable mosquitto
sudo ss -ltnp | grep 1883        # should list both 127.0.0.1 and 172.20.200.1
```

### 4.6 MQTT topics used by the artefact

| Topic | Purpose |
|---|---|
| `hre/telemetry/<municipality>/<sensor_id>` | Sensor readings (QoS 1) |
| `hre/labels/<municipality>/<sensor_id>` | Fault ground truth, stored in `fault_label` |
| `hre/sim/power/…` | Load-shedding switches (retained; an empty message deletes one) |
| `hre/sim/fault/…` | Fault commands, targeting a sensor, a municipality VLAN or all sensors |
| `hre/sim/uplink/…` | Cut or restore one municipality's uplink |
| `hre/status/<client>` | Online and offline status (Last Will) |

---

## 5. Installing Node-RED on the Ubuntu VM

### 5.1 Install

The official Node-RED installer for Debian and Ubuntu installs Node.js LTS and Node-RED, and sets up a systemd service.

```bash
bash <(curl -sL https://github.com/node-red/linux-installers/releases/latest/download/update-nodejs-and-nodered-deb)
sudo systemctl enable --now nodered.service
```

The editor runs at `http://<edge-vm-ip>:1880`.

### 5.2 Secure the editor

Set `adminAuth` in `~/.node-red/settings.js`. You can generate a password hash with `node-red admin hash-pw`.

### 5.3 Install the extra node

From the **Menu → Manage palette → Install** tab, install the PostgreSQL node used by the flows (`<node-red-contrib-postgresql>`).

### 5.4 Import the flows

1. Go to **Menu → Import** and select `<flows.json>` from this repository.
2. Credentials are not exported with flows, so re-enter them:
   - **MQTT broker node:** server `127.0.0.1`, port `1883`, username `nodered` and its password.
   - **PostgreSQL node:** the connection details from Section 6.
3. Create the spool folders, one per lane:
   ```bash
   mkdir -p <spool-path>/{harare,chitungwiza,epworth,ruwa,edge}
   ```
4. Click **Deploy**.

The flow is organised into one lane per municipality plus an edge lane. Each lane has its own MQTT subscription, validator, fault evaluator, database connection and spool folder.

---

## 6. Connecting to the database

The database (PostgreSQL with TimescaleDB) is hosted on Railway. **You do not need to install PostgreSQL.** Connect with the shared link instead.

> The connection link is shared privately with examiners. It contains a password, so it is **not** stored in this repository.

### 6.1 Connect with `psql`

```bash
sudo apt install -y postgresql-client

psql "postgresql://postgres:ea1234@maglev.proxy.rlwy.net:16330/railway"
```

### 6.2 Connect from Node-RED or Grafana

Use the host, port, database, user and password from the same link, with SSL **required**.

### 6.3 Quick checks once connected

```sql
\dt                                   -- list tables
SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';
SELECT count(*) FROM telemetry;       -- readings stored so far
SELECT * FROM baseline;               -- normal and critical bands per metric
SELECT * FROM sensor_status LIMIT 10; -- online, offline and suppressed sensors
```

### 6.4 Recreate the database (optional)

To build your own copy, run the single migration script against an empty PostgreSQL database that has TimescaleDB available:

```bash
psql "<your-own-db-link>" -f <migration.sql>
```

---

## 7. Running the sensor simulation

### 7.1 Install the Python dependency on the edge VM

```bash
sudo apt install -y python3-pip
pip install --break-system-packages paho-mqtt
```

### 7.2 The scripts

| Script | Use |
|---|---|
| `hre_fleet` | Always-on source for all 190 sensors, run as the systemd service `hre-fleet` |
| `hre_water_node.py`, `hre_sewer_node.py` | Run on GNS3 Docker nodes, one per VLAN per municipality, so that traffic crosses the simulated network |
| `hre_fault_water.py`, `hre_fault_sewer.py` | Inject faults (options `--ramp`, `--duration`, `--clear`) |
| `hre_control.py` | Restore power, clear faults and show what is switched off |
| `hre_tests.py` | Latency, load-shedding-with-sync and flapping-uplink tests |

### 7.3 Start the fleet

```bash
sudo systemctl enable --now hre-fleet
journalctl -u hre-fleet -f        # watch it publish
```

### 7.4 Run a node script on a GNS3 Docker node

Open the node's console in GNS3:

```bash
dhclient eth0 2>/dev/null || true     # get an address from the router's DHCP pool
ip addr show eth0                     # e.g. Harare water node: 172.20.10.11
python3 hre_water_node.py <municipality> <args>
```

When a node starts publishing for a sensor, the fleet stops publishing for it. If the node is silent for 450 seconds, the fleet takes the sensor back.

---

## 8. Testing the system

Run the tests in order. Each one gives an expected result you can compare against.

### 8.1 Network checks (router consoles)

| Command | Run on | Expected |
|---|---|---|
| `show ip ospf neighbor` | Any router | All neighbours in `FULL` state |
| `show ip route ospf` | Any router | Routes to every sensor subnet and to 172.20.200.0/30 |
| `show ip dhcp binding` | Municipality router | Leases for its sensor nodes |
| `show access-lists SENSOR-IN` | Municipality router | Match counters increasing |
| `show ip ssh` | Any router | SSH version 2 |

To check SSH from the management network:

```bash
ssh <admin-user>@172.20.99.11     # should succeed
telnet 172.20.99.11               # should be refused
```

### 8.2 ACL check (sensor-node console)

```bash
ping -c 2 172.20.99.11            # expected: FAILS (sensors may not reach routers)
mosquitto_pub -h 172.20.200.1 -p 1883 -u <harare-sensor-user> -P '<password>' \
  -t hre/status/test -m hello     # expected: succeeds (TCP 1883 to the broker is allowed)
```

### 8.3 Watch all traffic (edge VM, keep this terminal open)

```bash
mosquitto_sub -h 127.0.0.1 -u nodered -P '<password>' -t 'hre/#' -v
```

### 8.4 Publish a reading with a sensor account (sensor node)

Payload fields: `sensor_id`, `ts`, `readings{}`, `trigger` (`schedule`, `threshold` or `recovery`) and `src`.

```bash
mosquitto_pub -h 172.20.200.1 -u <harare-sensor-user> -P '<password>' -q 1 \
  -t hre/telemetry/harare/<sensor_id> \
  -m '{"sensor_id":"<sensor_id>","ts":<timestamp>,"readings":{"pressure":3.1,"flow":12.0},"trigger":"schedule","src":"manual"}'
```

**Expected:** the reading appears in the subscriber terminal, passes Node-RED validation, and is stored:

```sql
SELECT * FROM telemetry WHERE sensor_id = '<sensor_id>' ORDER BY time DESC LIMIT 1;
```

### 8.5 Negative tests: each should be rejected

| What to publish | Expected result |
|---|---|
| `-m 'not json'` | Rejected (structure) in the Node-RED log |
| A `sensor_id` not in the registry | Rejected (unknown sensor) |
| Topic says `harare`, but the payload sensor belongs to `ruwa` | Rejected (topic mismatch) |
| `"pressure": 40` | Rejected (outside the physical range of 0–25 bar) |
| A `ts` 10 minutes in the future | Rejected (time) |
| `<harare-sensor-user>` publishing to `hre/telemetry/ruwa/...` | Refused by the Mosquitto ACL |
| A sensor account publishing to `hre/sim/...` | Refused by the Mosquitto ACL |

### 8.6 Inject faults (edge VM)

```bash
# Sudden critical water fault: expect an alert within about 30 s
python3 hre_fault_water.py <municipality-or-ward> burst --duration 120

# Developing (ramped) water leak: expect an alert within about 10–14 min
python3 hre_fault_water.py <municipality-or-ward> leak --ramp 600 --duration 900

# Sewer blockage
python3 hre_fault_sewer.py <municipality-or-ward> blockage --duration 300

# Clear the faults
python3 hre_fault_water.py <municipality-or-ward> --clear
python3 hre_fault_sewer.py <municipality-or-ward> --clear
```

**Expected:**

- Critical faults (burst, high pressure, supply loss, overflow) appear in **Open critical faults** on the metro and municipal dashboards.
- Warning faults (low pressure, surcharge, infiltration) appear after two consecutive readings, about 10 minutes.
- Dashboards refresh every 30 seconds.

The ten fault signatures are:

| Network | Fault | Signature |
|---|---|---|
| Water | Burst | Pressure low AND flow high, both critical |
| Water | Leak | Pressure low AND flow high, not both critical |
| Water | Supply loss | Pressure low AND flow low |
| Water | Low pressure | Pressure low only (warning) |
| Water | High pressure | Pressure high |
| Water | Reservoir low | Level low |
| Sewer | Blockage | Level high AND flow low |
| Sewer | Overflow | Level high AND flow high, or level critical alone |
| Sewer | Surcharge | Level high, warning band only |
| Sewer | Infiltration | Flow high only (warning) |

### 8.7 Load-shedding and uplink tests

```bash
python3 hre_control.py <status>      # show what is switched off
python3 hre_tests.py <test-name>     # latency | load-shedding-with-sync | flapping-uplink
python3 hre_control.py <restore>     # restore power and clear faults
```

**Expected:**

- During a cut, the affected wards turn red on **Ward connectivity**.
- Readings are written to that lane's spool folder.
- After the link is restored, the readings are synchronised with no loss and no duplicates.

To confirm that nothing was lost or duplicated:

```sql
SELECT sensor_id, time, count(*) FROM telemetry
GROUP BY sensor_id, time HAVING count(*) > 1;     -- expected: 0 rows
```

### 8.8 Dashboards

The live dashboards are at:
https://hre-water-sewer-monitoring-system.up.railway.app/d/hre-metro/3a8d27b

The dashboards include:

- **Metro dashboard:** all four municipalities.
- **Municipal dashboards:** use the municipality variable at the top.

Log in with `<grafana-user>`. The credentials are shared privately.

---

## 9. Troubleshooting

| Symptom | Check |
|---|---|
| Sensors cannot reach the broker | Is `tap1` up? Does `ping 172.20.200.2` from the VM work? Does `show ip route` on the municipality router include 172.20.200.0/30? |
| Mosquitto fails to start | `tap1` must exist before Mosquitto binds to 172.20.200.1; start the TAP service first. Check `journalctl -u mosquitto`. |
| `Connection refused: not authorised` | Wrong username or password, or the topic is not allowed in `/etc/mosquitto/acl` |
| Node-RED receives readings but stores nothing | Check the PostgreSQL node credentials (SSL required) and the lane's up/down state; readings may be waiting in the spool |
| Docker node has no IP address | Check the switch access port VLAN and run `show ip dhcp pool` on the router |
| Packages missing on a node after restart | Make sure the template uses the committed image `gns3/telemetry-node:1` |

---

## Project resources

- **Grafana:** see Section 8.8.
- **Email alert relay:** `grafana-brevo-relay/` forwards Grafana alerts by email through Brevo. It needs the environment variables `BREVO_API_KEY`, `ALERT_FROM_EMAIL`, `ALERT_TO_EMAIL` and `RELAY_SECRET`.
