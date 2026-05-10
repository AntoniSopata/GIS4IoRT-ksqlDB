#!/usr/bin/env python3

import json
import os
import time
import math
import logging
from kafka import KafkaConsumer, KafkaProducer
from itertools import combinations
from typing import Dict, Optional

COLLISION_DISTANCE_THRESHOLD_M = 6.0
MESSAGE_EXPIRATION_TIME_S = 60.0

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'broker:29092')
GPS_INPUT_TOPIC = 'ros_gps_fix'
REGISTRY_TOPIC = 'robot_selection'
COLLISION_CONTROL_TOPIC = 'collision_control'
COLLISION_OUTPUT_TOPIC = 'robot_collision_alerts'

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class CollisionDetector:
    def __init__(self):
        self.registered_robots = set()
        self.latest_positions: Dict[str, dict] = {}
        
        # Control state
        self.collision_monitoring_enabled = False
        self.active_config_name = None
        
        # Kafka connections
        self.gps_consumer: Optional[KafkaConsumer] = None
        self.registry_consumer: Optional[KafkaConsumer] = None
        self.control_consumer: Optional[KafkaConsumer] = None
        self.producer: Optional[KafkaProducer] = None
        
        # Statistics
        self.check_counter = 0
        self.stats = {
            'gps_messages_processed': 0,
            'registry_updates': 0,
            'collision_alerts_sent': 0,
            'positions_purged': 0,
            'control_updates': 0
        }

    def connect(self):
        """Initialize Kafka consumers and producer"""
        try:
            # GPS Consumer 
            self.gps_consumer = KafkaConsumer(
                GPS_INPUT_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                auto_offset_reset='latest',
                group_id='collision-detector-gps-v2',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                enable_auto_commit=True,
                max_poll_records=500
            )
            
            # Registry Consumer
            self.registry_consumer = KafkaConsumer(
                REGISTRY_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                auto_offset_reset='earliest',
                group_id='collision-detector-registry-v2',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                enable_auto_commit=True
            )
            
            # Control Consumer
            self.control_consumer = KafkaConsumer(
                COLLISION_CONTROL_TOPIC,
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                auto_offset_reset='earliest',
                group_id='collision-detector-control-v2',
                value_deserializer=lambda x: json.loads(x.decode('utf-8')),
                enable_auto_commit=True
            )
            
            # Producer
            self.producer = KafkaProducer(
                bootstrap_servers=[KAFKA_BOOTSTRAP_SERVERS],
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None
            )
            
            logger.info("=" * 70)
            logger.info("Kafka Connection Established")
            logger.info(f"  Bootstrap: {KAFKA_BOOTSTRAP_SERVERS}")
            logger.info(f"  GPS Input: {GPS_INPUT_TOPIC}")
            logger.info(f"  Registry: {REGISTRY_TOPIC}")
            logger.info(f"  Control: {COLLISION_CONTROL_TOPIC}")
            logger.info(f"  Alert Output: {COLLISION_OUTPUT_TOPIC}")
            logger.info("=" * 70)
            return True
            
        except Exception as e:
            logger.error(f"✗ Kafka Connection Failed: {e}")
            return False

    def process_control_message(self, message):
        """
        Process collision control messages
        Expected format: {"status": "ON/OFF", "config_name": "iter_007_COLLISION"}
        """
        try:
            data = message.value
            status = data.get('status', 'OFF')
            config_name = data.get('config_name', 'unknown')
            
            if status == 'ON':
                if not self.collision_monitoring_enabled:
                    self.collision_monitoring_enabled = True
                    self.active_config_name = config_name
                    self.stats['control_updates'] += 1
                    logger.warning(f"COLLISION MONITORING ACTIVATED | Config: {config_name}")
                    
            elif status == 'OFF':
                if self.collision_monitoring_enabled:
                    self.collision_monitoring_enabled = False
                    prev_config = self.active_config_name
                    self.active_config_name = None
                    self.stats['control_updates'] += 1
                    logger.warning(f"COLLISION MONITORING DEACTIVATED | Was: {prev_config}")
                    
        except Exception as e:
            logger.error(f"Error processing control message: {e}")

    def process_registry_message(self, message):
        """
        Only SELECTED robots are monitored for collisions
        """
        try:
            robot_id = message.key.decode('utf-8') if isinstance(message.key, bytes) else str(message.key)
            data = message.value
            status = data.get('status')
            
            if status == 'SELECTED':
                if robot_id not in self.registered_robots:
                    self.registered_robots.add(robot_id)
                    self.stats['registry_updates'] += 1
                    logger.info(f"Robot SELECTED: {robot_id}")
                    
            elif status == 'DESELECTED':
                if robot_id in self.registered_robots:
                    self.registered_robots.discard(robot_id)
                    if robot_id in self.latest_positions:
                        del self.latest_positions[robot_id]
                    self.stats['registry_updates'] += 1
                    logger.info(f"Robot DESELECTED: {robot_id}")
                    
        except Exception as e:
            logger.error(f"Error processing registry message: {e}")

    def update_robot_position(self, message):
        """
        Update latest position for a robot in the internal cache
        Returns True if a position was successfully updated
        """
        try:
            robot_id = message.key.decode('utf-8') if isinstance(message.key, bytes) else str(message.key)
            
            # Only process if robot is registered
            if robot_id not in self.registered_robots:
                return False
            
            data = message.value
            
            # Validate required fields
            if not all(k in data for k in ['timestamp', 'latitude', 'longitude']):
                return False
            
            self.latest_positions[robot_id] = {
                'lat': data['latitude'],
                'lon': data['longitude'],
                'altitude': data.get('altitude', 0.0),
                'timestamp': data['timestamp'],
                'received_at': time.time()
            }
            
            self.stats['gps_messages_processed'] += 1
            return True
            
        except Exception as e:
            logger.error(f"Error processing GPS message: {e}")
            return False

    def calculate_distance(self, lat1, lon1, lat2, lon2) -> float:
        """Calculate distance between two GPS coordinates using Haversine formula"""
        R = 6371000  # Earth radius
        
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        delta_phi = math.radians(lat2 - lat1)
        delta_lambda = math.radians(lon2 - lon1)
        
        a = (math.sin(delta_phi / 2) ** 2 +
             math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c

    def purge_expired_positions(self):
        """Remove GPS positions older than MESSAGE_EXPIRATION_TIME_S"""
        current_time = time.time()
        expired = [
            robot_id for robot_id, pos in self.latest_positions.items()
            if current_time - pos['received_at'] > MESSAGE_EXPIRATION_TIME_S
        ]
        
        for robot_id in expired:
            del self.latest_positions[robot_id]
            self.stats['positions_purged'] += 1

    def check_collisions(self):
        """
        Run collision detection on all robot pairs using their LATEST known state.
        Only runs if collision_monitoring_enabled is True
        """
        # Skip if monitoring is disabled
        if not self.collision_monitoring_enabled:
            return
        
        self.check_counter += 1
        
        # Purge expired positions before checking
        self.purge_expired_positions()
        
        # Get robots with fresh GPS data
        active_robots = [
            robot_id for robot_id in self.registered_robots
            if robot_id in self.latest_positions
        ]
        
        # Need at least 2 active robots to collide
        if len(active_robots) < 2:
            return
        
        alerts_sent = 0
        
        for robot1_id, robot2_id in combinations(active_robots, 2):
            pos1 = self.latest_positions[robot1_id]
            pos2 = self.latest_positions[robot2_id]
            
            distance = self.calculate_distance(
                pos1['lat'], pos1['lon'],
                pos2['lat'], pos2['lon']
            )
            
            if distance < COLLISION_DISTANCE_THRESHOLD_M:
                self.send_collision_alert(robot1_id, robot2_id, distance, pos1, pos2)
                alerts_sent += 1
        
        # Log periodic stats
        if self.check_counter % 100 == 0:
            logger.info(
                f"Check #{self.check_counter}: {len(active_robots)} active robots | "
                f"{self.stats['gps_messages_processed']} msgs processed | "
                f"Monitoring: {'ON' if self.collision_monitoring_enabled else 'OFF'}"
            )

    def send_collision_alert(self, robot1_id, robot2_id, distance, pos1, pos2):
        """Send collision alert to Kafka"""
        current_time_ms = int(time.time() * 1000)
        
        alert = {
            'robot1_id': robot1_id,
            'robot2_id': robot2_id,
            'distance_m': round(distance, 2),
            'threshold_m': COLLISION_DISTANCE_THRESHOLD_M,
            'timestamp': current_time_ms,
            'config_name': self.active_config_name,
            'robot1_position': {
                'latitude': pos1['lat'],
                'longitude': pos1['lon'],
                'altitude': pos1['altitude'],
                'gps_timestamp': pos1['timestamp']
            },
            'robot2_position': {
                'latitude': pos2['lat'],
                'longitude': pos2['lon'],
                'altitude': pos2['altitude'],
                'gps_timestamp': pos2['timestamp']
            },
            'severity': 'CRITICAL' if distance < COLLISION_DISTANCE_THRESHOLD_M / 2 else 'WARNING'
        }
        
        try:
            alert_key = f"{min(robot1_id, robot2_id)}_{max(robot1_id, robot2_id)}"
            
            self.producer.send(
                COLLISION_OUTPUT_TOPIC,
                key=alert_key,
                value=alert
            )
            
            self.stats['collision_alerts_sent'] += 1
            
            logger.warning(
                f"COLLISION DETECTED: {robot1_id} ↔ {robot2_id} | "
                f"Dist: {distance:.2f}m | Config: {self.active_config_name}"
            )
            
        except Exception as e:
            logger.error(f"Failed to send collision alert: {e}")

    def run(self):
        if not self.connect():
            logger.error("Failed to connect to Kafka. Exiting.")
            return
        
        logger.info("=" * 70)
        logger.info("COLLISION DETECTION SYSTEM ACTIVE (CONTROLLED MODE)")
        logger.info(f"   Distance Threshold: {COLLISION_DISTANCE_THRESHOLD_M} meters")
        logger.info(f"   Message Expiration: {MESSAGE_EXPIRATION_TIME_S} seconds")
        logger.info(f"   Control Topic: {COLLISION_CONTROL_TOPIC}")
        logger.info("   Status: WAITING FOR ACTIVATION")
        logger.info("=" * 70)
        
        try:
            while True:
                # Process Control Messages
                control_messages = self.control_consumer.poll(timeout_ms=10, max_records=50)
                for tp, messages in control_messages.items():
                    for message in messages:
                        self.process_control_message(message)
                
                # Process Registry Updates
                registry_messages = self.registry_consumer.poll(timeout_ms=10, max_records=50)
                for tp, messages in registry_messages.items():
                    for message in messages:
                        self.process_registry_message(message)
                
                # Process GPS Updates (only if monitoring is enabled)
                if self.collision_monitoring_enabled:
                    gps_batch = self.gps_consumer.poll(timeout_ms=50)
                    
                    data_received_in_this_tick = False
                    
                    if gps_batch:
                        for tp, messages in gps_batch.items():
                            for message in messages:
                                if self.update_robot_position(message):
                                    data_received_in_this_tick = True
                    
                    # Trigger Collision Check
                    if data_received_in_this_tick:
                        self.check_collisions()
                
                # Sleep if no activity
                if not control_messages and not registry_messages:
                    time.sleep(0.01)
                    
        except KeyboardInterrupt:
            logger.info("\nShutting down collision detector...")
        finally:
            if self.gps_consumer:
                self.gps_consumer.close()
            if self.registry_consumer:
                self.registry_consumer.close()
            if self.control_consumer:
                self.control_consumer.close()
            if self.producer:
                self.producer.close()
            logger.info("Shutdown complete")

def main():
    detector = CollisionDetector()
    detector.run()


if __name__ == "__main__":
    main()