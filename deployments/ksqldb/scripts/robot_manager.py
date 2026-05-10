import threading, time, json, os, requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

KAFKA_BROKER = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
KSQLDB_URL   = os.getenv('KSQLDB_URL', 'http://ksqldb-server:8088')
HEALTH_TOPIC = 'robot_health'
TIMEOUT_SECONDS = 65

# static topics that always exist, rest are dynamic
STATIC_TOPICS = {'robot_health', 'robot_registration'}

class RobotManager:
    def __init__(self):
        self.robots = {}
        self.lock = threading.Lock()
        
        # tracks streams already registered in ksqlDB
        self.registered_streams: set[str] = set()

        #self.admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER])
        self.consumer = KafkaConsumer(
            'robot_health', 'robot_registration',
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        self.producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            key_serializer=lambda x: str(x).encode('utf-8')
        )
        
        # sync existing ksqlDB streams on startup
        self._sync_existing_ksql_streams()

    def _sync_existing_ksql_streams(self):
        """On startup download stream list already registered"""
        try:
            resp = requests.get(f"{KSQLDB_URL}/streams", timeout=5)
            if resp.status_code == 200:
                for stream in resp.json():
                    self.registered_streams.add(stream['topic'].lower())
                print(f"[KSQL] Known streams on startup: {self.registered_streams}")
        except requests.RequestException as e:
            print(f"[KSQL] Couldn't download stream list (ksqlDB not ready?): {e}")

    def _ensure_kafka_topic(self, topic_name: str):
        """Creates topic if it doesn't exist"""
        try:
            admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER])
        except Exception as e:
            print(f"[KAFKA] Can't connect with AdminClient: {e}")
            return

        try:
            existing = set(admin.list_topics())
            if topic_name in existing:
                return
            
            admin.create_topics([NewTopic(
                name=topic_name,
                num_partitions=2,
                replication_factor=1,
                topic_configs={"retention.ms": "3600000"}
            )])
            print(f"[KAFKA] Created topic: {topic_name}")
        except TopicAlreadyExistsError:
            pass
        except Exception as e:
            print(f"[KAFKA] Error on operation on topic: {e}")
        finally:
            admin.close()

    def _register_ksql_stream(self, topic_name: str, schema: list[dict], retries: int = 5):
        """
        Sends CREATE STREAM to ksqlDB.
        schema = [{'name': 'position_x', 'type': 'DOUBLE'}, ...]
        """
        if topic_name in self.registered_streams:
            return  # already registered, skip

        stream_name = topic_name.upper().replace('-', '_') + "_STREAM"
        
        # build column list
        columns = "robot_id VARCHAR KEY,\n  " + ",\n  ".join(
            f"{col['name']} {col['type']}" for col in schema
        )
        
        ksql = f"""
            CREATE STREAM IF NOT EXISTS {stream_name} (
              {columns}
            ) WITH (
              KAFKA_TOPIC='{topic_name}',
              VALUE_FORMAT='JSON',
              TIMESTAMP='timestamp'
            );
        """
            
        for attempt in range(retries):
            try:
                resp = requests.post(
                    f"{KSQLDB_URL}/ksql",
                    json={"ksql": ksql, "streamsProperties": {}},
                    headers={"Content-Type": "application/vnd.ksql.v1+json"},
                    timeout=10
                )
                
                if resp.status_code == 200:
                    print(f"[KSQL] Registered stream: {stream_name} (topic: {topic_name})")
                    self.registered_streams.add(topic_name)
                    return
                else:
                    print(f"[KSQL] Denied query for stream: {stream_name}: {resp.status_code} {resp.text}")
            except requests.RequestException as e:
                print(f"[KSQL] Connection error: {e}")
            
            print(f"[KSQL] Attempt {attempt+1}/{retries} in 5s...")
            time.sleep(5)
            
        print(f"[KSQL] Couldn't register {topic_name} Try count: {retries}")


    def _handle_registration(self, payload: dict):
        """Handle robot registration and create topics and streams"""
        serial = payload.get('serial_number')
        topics = payload.get('topics', [])
        
        for topic_def in topics:
            # handle both formats: string (old) or dict with schema (new)
            if isinstance(topic_def, str):
                # old format, name only, generic schema
                topic_name = topic_def
                schema = [
                    {'name': 'timestamp', 'type': 'BIGINT'},
                    {'name': 'data',      'type': 'VARCHAR'},
                ]
            else:
                topic_name = topic_def['name']
                schema     = topic_def['schema']

            if topic_name in STATIC_TOPICS:
                continue

            self._ensure_kafka_topic(topic_name)
            self._register_ksql_stream(topic_name, schema)

        print(f"[ONLINE] Robot {serial} registered, topics: {[t if isinstance(t,str) else t['name'] for t in topics]}")

    def monitor_loop(self):
        while True:
            time.sleep(5)
            current_time = int(time.time() * 1000)
            with self.lock:
                for serial, data in self.robots.items():
                    if data['status'] == 'OFFLINE':
                        continue
                    if (current_time - data['last_ping']) / 1000.0 > TIMEOUT_SECONDS:
                        print(f"[ALERT] Robot {serial} OFFLINE!")
                        data['status'] = 'OFFLINE'

    def run(self):
        threading.Thread(target=self.monitor_loop, daemon=True).start()
        print("\n=== Robot Manager Active ===\n")

        for message in self.consumer:
            payload = message.value
            serial  = payload.get('serial_number')
            msg_type = payload.get('msg_type', 'PING')

            with self.lock:
                is_new = serial not in self.robots or self.robots[serial]['status'] == 'OFFLINE'

                if msg_type == 'REGISTRATION' or (is_new and 'topics' in payload):
                    self._handle_registration(payload)
                
                self.robots[serial] = {
                    'last_ping': payload.get('timestamp', int(time.time() * 1000)),
                    'status': 'ONLINE',
                    'topics': payload.get('topics', self.robots.get(serial, {}).get('topics', []))
                }

if __name__ == '__main__':
    try:
        Manager = RobotManager()
        Manager.run()
    except KeyboardInterrupt:
        print("\nShutting down...")