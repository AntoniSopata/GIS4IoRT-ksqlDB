# Data Formats (Kafka topics & WebSockets)

This document provides the specification for messages transmitted within the GIS4IoRT-ksqlDB system.

## 1. Speed
Monitors the average speed of a robot within a time window.

* **Kafka Topic:** robot_speed_alerts
* **Producer:** ksqlDB (query defined in `app/adapters/ksqldb/routers/speed.py`)
* **WebSocket Format:** Identical to Kafka.

### JSON Structure
| Field | Type | Description |
| :--- | :--- | :--- |
| `ROBOT_NAME` | String | Name of the robot (cast from ID) |
| `WINDOW_START` | String | Start of the measurement window (HH:mm:ss) |
| `WINDOW_END` | String | End of the measurement window (HH:mm:ss) |
| `CURRENT_X` | Double | Last known X position |
| `CURRENT_Y` | Double | Last known Y position |
| `ACTIVE_CONFIGS` | String | Names of active configurations (separated by `\|`) |
| `AVG_SPEED_MPS` | Double | Average speed in m/s |
| `TIME_WINDOW_S` | Double | Actual duration of the window in seconds |
| `SAMPLE_COUNT` | BigInt | Number of samples in the window |

**Example:**
```json
Kafka Topic:
COMMAND:
docker compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic robot_speed_alerts

OUTPUT:
{"ROBOT_NAME":"101","WINDOW_START":"17:45:46","WINDOW_END":"17:45:48","CURRENT_X":-21.241063777217388,"CURRENT_Y":22.269139615234398,"ACTIVE_CONFIGS":"spe1","AVG_SPEED_MPS":1.4988533133365194,"TIME_WINDOW_S":1.300000,"SAMPLE_COUNT":27}
```
```json
WebSocket:
COMMAND:
wscat -c ws://localhost:8000/ksqldb/ws
Connected (press CTRL+C to quit)
> {"action": "subscribe", "config_name": "spe1", "type": "speed"}


OUTPUT:
< {"status":"subscribed","config_name":"spe1","type":"speed","topic":"robot_speed_alerts"}
< {"ROBOT_NAME":"101","WINDOW_START":"17:47:24","WINDOW_END":"17:47:26","CURRENT_X":-8.115405056560983,"CURRENT_Y":-2.4281276464781234,"ACTIVE_CONFIGS":"spe1","AVG_SPEED_MPS":1.305318693846173,"TIME_WINDOW_S":1.648,"SAMPLE_COUNT":34}

```

---

## 2. Humidity Alerts
Detects when a robot is in proximity to a sensor with humidity exceeding a specified threshold.

* **Kafka Topic:** robot_humidity_alerts
* **Producer:** ksqlDB (query defined in `app/adapters/ksqldb/routers/humidity.py`)
* **WebSocket Format:** Identical to Kafka.

### JSON Structure
| Field | Type | Description |
| :--- | :--- | :--- |
| `type` | String | Constant value humidity |
| `robot` | String | Robot ID |
| `ts` | BigInt | Event timestamp (ms) |
| `lat` | Double | Robot latitude |
| `lon` | Double | Robot longitude |
| `sensor` | String | Sensor ID (human-readable) |
| `humidity` | Double | Detected humidity level |
| `threshold` | Double | Alarm threshold |
| `distance_m` | Double | Distance to the sensor in meters |

**Example:**
```json
Kafka Topic:
COMMAND:
docker compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic robot_humidity_alerts

OUTPUT:
{"type":"humidity","robot":"102","ts":1778782060859,"lat":46.33932013564585,"lon":3.4336897753581073,"sensor":"101","humidity":90.9,"threshold":1.0,"distance_m":24.48701014573961}
```
```json
WebSocket:
COMMAND:
wscat -c ws://localhost:8000/ksqldb/ws
Connected (press CTRL+C to quit)
> {"action": "subscribe", "config_name": "global", "type": "humidity"}

OUTPUT:
< {"status":"subscribed","config_name":"global","type":"humidity","topic":"robot_humidity_alerts"}
< {"type":"humidity","robot":"101","ts":1778782173694,"lat":46.339335967464294,"lon":3.43370632389033,"sensor":"101","humidity":90.9,"threshold":1.0,"distance_m":25.4350835740415}
```

---

## 3. Geofencing
Detects when a robot moves outside a defined zone.

* **Kafka Topic:** robot_geofence_alerts
* **Producer:** ksqlDB (query defined in `app/adapters/ksqldb/routers/geofence.py`)
* **WebSocket Format:** Identical to Kafka.

### JSON Structure
| Field | Type | Description |
| :--- | :--- | :--- |
| `type` | String | Constant value geofence |
| `robot` | String | Robot ID |
| `ts` | BigInt | Event timestamp (ms) |
| `lat` | Double | Latitude |
| `lon` | Double | Longitude |
| `msg` | String | Status (e.g., OUTSIDE) |
| `zones` | String | Zone/configuration names (separated by `\|`) |

**Example:**
```json
Kafka Topic:
COMMAND:
docker compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic robot_geofence_alerts

OUTPUT:
{"type":"geofence","robot":"101","ts":1778781000312,"lat":46.33908161929049,"lon":3.433750654215595,"msg":"OUTSIDE","zones":"geo1"}
```
```json
WebSocket:
COMMAND:
wscat -c ws://localhost:8000/ksqldb/ws
Connected (press CTRL+C to quit)
> {"action": "subscribe", "config_name": "geo1", "type": "geofence"}


OUTPUT:
< {"status":"subscribed","config_name":"geo1","type":"geofence","topic":"robot_geofence_alerts"}
< {"type":"geofence","robot":"101","ts":1778781138803,"lat":46.339060599349644,"lon":3.4336959976372388,"msg":"OUTSIDE","zones":"geo1"}
```

---

## 4. Collisions
Detects when the distance between two robots is too small.

* **Kafka Topic:** robot_collision_alerts
* **Producer:** `collision_detector.py` (Python script)
* **WebSocket Format:** Identical to Kafka.

### JSON Structure
| Field | Type | Description |
| :--- | :--- | :--- |
| `robot1_id` | String | ID of the first robot |
| `robot2_id` | String | ID of the second robot |
| `distance_m` | Float | Distance between them |
| `threshold_m` | Float | Collision threshold |
| `timestamp` | BigInt | Detection time |
| `config_name`| String | Name of the active configuration |
| `robot1_position` | Object | Position {latitude, longitude, altitude, gps_timestamp} |
| `robot2_position` | Object | Position {latitude, longitude, altitude, gps_timestamp} |
| `severity` | String | `CRITICAL` (<3m) or `WARNING` (<6m) |

**Example:**
```json
Kafka Topic:
COMMAND:
docker compose exec broker kafka-console-consumer --bootstrap-server localhost:9092 --topic robot_collision_alerts

OUTPUT:
{"robot1_id": "101", "robot2_id": "102", "distance_m": 5.96, "threshold_m": 6.0, "timestamp": 1778779722754, "config_name": "col1", "robot1_position": {"latitude": 46.33914162416166, "longitude": 3.433641337792802, "altitude": 366.96456633415073, "gps_timestamp": 1778779722751}, "robot2_position": {"latitude": 46.33908804999217, "longitude": 3.433645194505264, "altitude": 366.78828661423177, "gps_timestamp": 1778779722709}, "severity": "WARNING"}
```

```json
WebSocket:
COMMAND:
wscat -c ws://localhost:8000/ksqldb/ws
Connected (press CTRL+C to quit)
> {"action": "subscribe", "config_name": "global", "type": "collision"}

OUTPUT:
< {"status":"subscribed","config_name":"global","type":"collision","topic":"robot_collision_alerts"}
< {"robot1_id":"101","robot2_id":"102","distance_m":5.63,"threshold_m":6.0,"timestamp":1778779631361,"config_name":"col1","robot1_position":{"latitude":46.33908968363555,"longitude":3.433747637394223,"altitude":366.8369153002277,"gps_timestamp":1778779631358},"robot2_position":{"latitude":46.33910141793832,"longitude":3.4336763176297254,"altitude":366.8130084602162,"gps_timestamp":1778779631315},"severity":"WARNING"}
```
