import json
import time
import sys
import random
import os
from kafka import KafkaProducer

# configuration
KAFKA_BROKER = "broker:29092"
TOPIC_NAME = "sensor_proximity"
CONFIG_FILE = "./sensor_config.json"

FREQUENCY_HZ = 0.000556

def setup_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print(f"[INFO] Connected to Kafka: {KAFKA_BROKER}")
        return producer
    except Exception as e:
        print(f"[CRITICAL] Could not connect to Kafka: {e}")
        sys.exit(1)

def main():
    if not os.path.exists(CONFIG_FILE):
        print(f"[ERROR] Missing file {CONFIG_FILE}. Generate it first!")
        sys.exit(1)

    with open(CONFIG_FILE, 'r') as f:
        sensors = json.load(f)
    
    print(f"[INFO] Loaded {len(sensors)} sensors.")
    
    for s in sensors:
        sid = int(s['sensor_id'])
        if sid % 2 != 0:
            s['base_humidity'] = 90.0
            s['role'] = "ALERT"
        else:
            s['base_humidity'] = 35.0
            s['role'] = "NORMAL"

        print(f" -> Sensor {sid}: Role={s['role']} (Hum ~{s['base_humidity']})")

    producer = setup_producer()
    
    print("="*50)
    print(f"STARTING REAL-TIME GENERATOR (Press Ctrl+C to stop)")
    print(f"Topic: {TOPIC_NAME}, Freq: {FREQUENCY_HZ} Hz")
    print("="*50)

    try:
        while True:
            
            loop_start = time.time()
            current_ts_ms = int(loop_start * 1000)

            for s in sensors:
                noise = random.uniform(-1.0, 1.0)
                humidity = round(s['base_humidity'] + noise, 1)

                payload = {
                    "timestamp": current_ts_ms,
                    #"sensor_id": str(s['sensor_id']),
                    "position_x": s['lon'], 
                    "position_y": s['lat'], 
                    "humidity": humidity,
                }
                
                producer.send(
                    TOPIC_NAME, 
                    key=str(s['sensor_id']).encode('utf-8'),
                    value=payload
                )
                
                print(f"[SEND] TS:{current_ts_ms} | ID:{s['sensor_id']} | Hum:{humidity} ({s['role']})")

            elapsed = time.time() - loop_start
            sleep_needed = (1.0 / FREQUENCY_HZ) - elapsed
            if sleep_needed > 0:
                time.sleep(sleep_needed)

    except KeyboardInterrupt:
        print("\n[INFO] Generator stopped.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()