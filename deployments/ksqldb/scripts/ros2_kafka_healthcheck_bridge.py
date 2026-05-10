#!/usr/bin/env python3

import rclpy
import time
import math
import json
import sys
from rclpy.node import Node
from nav_msgs.msg import Odometry
from sensor_msgs.msg import NavSatFix
from kafka import KafkaProducer
from functools import partial

class HybridKafkaBridge(Node):
    def __init__(self):
        super().__init__('hybrid_kafka_bridge')

        # Parameters
        # Only 101 in the active list for now
        self.declare_parameter('robot_list', ['101'])
        self.declare_parameter('kafka_bootstrap_servers', 'broker:29092')
        self.declare_parameter('kafka_odom_topic', 'ros_filtered_odom')
        self.declare_parameter('kafka_gps_topic', 'ros_gps_fix')
        self.declare_parameter('kafka_health_topic', 'robot_health')

        self.robot_list = self.get_parameter('robot_list').get_parameter_value().string_array_value
        self.kafka_servers = self.get_parameter('kafka_bootstrap_servers').get_parameter_value().string_value
        self.kafka_odom_topic = self.get_parameter('kafka_odom_topic').get_parameter_value().string_value
        self.kafka_gps_topic = self.get_parameter('kafka_gps_topic').get_parameter_value().string_value
        self.kafka_health_topic = self.get_parameter('kafka_health_topic').get_parameter_value().string_value

        # Map the new numeric IDs to the old ROS namespaces in your bag files
        self.ros_namespace_map = {'101': 'leader'}

        #self.ros_namespace_map = {
        #    '101': 'leader',
        #    '102': 'follower'
        #}

        self.get_logger().info("--- Initializing Hybrid ROS2-Kafka Bridge ---")
        self.get_logger().info(f'Connecting to Kafka: {self.kafka_servers}')

        # Kafka Producer
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=[self.kafka_servers],
                value_serializer=lambda x: json.dumps(x).encode('utf-8'),
                key_serializer=lambda x: str(x).encode('utf-8') if x else None,
                acks=1, 
                retries=3
            )
            self.get_logger().info("Successfully connected to Kafka")
        except Exception as e:
            self.get_logger().error(f"Failed to connect to Kafka: {e}")
            sys.exit(1)

        # Initial Registration & Immediate Ping
        self.register_robots()
        self.healthcheck_callback() 

        # Dynamic Subscriptions & Tracking
        self.subscriptions_list = []
        self.message_count = {}

        for robot_id in self.robot_list:
            self.message_count[f"{robot_id}_odom"] = 0
            self.message_count[f"{robot_id}_gps"] = 0

            # Translate to the old bag file namespace just for the ROS subscription
            ros_namespace = self.ros_namespace_map.get(robot_id, robot_id)

            odom_topic = f'/{ros_namespace}/localisation/filtered_odom'
            self.subscriptions_list.append(self.create_subscription(Odometry, odom_topic, partial(self.odom_callback, robot_id=robot_id), 10))
            
            gps_topic = f'/{ros_namespace}/gps/fix'
            self.subscriptions_list.append(self.create_subscription(NavSatFix, gps_topic, partial(self.gps_callback, robot_id=robot_id), 10))

        self.get_logger().info("Bridge is live. Waiting for ROS2 messages...")

        # 30 Second Healthcheck Timer
        self.health_timer = self.create_timer(30.0, self.healthcheck_callback)

    def register_robots(self):
        for robot_id in self.robot_list:
            payload = {
                'msg_type': 'REGISTRATION',
                'serial_number': robot_id,
                'topics': [
                    {
                        'name': self.kafka_odom_topic,
                        'schema': [
                            {'name': 'timestamp',   'type': 'BIGINT'},
                            {'name': 'position_x',  'type': 'DOUBLE'},
                            {'name': 'position_y',  'type': 'DOUBLE'},
                            {'name': 'position_z',  'type': 'DOUBLE'},
                            {'name': 'source_topic','type': 'VARCHAR'},
                        ]
                    },
                    {
                        'name': self.kafka_gps_topic,
                        'schema': [
                            {'name': 'timestamp', 'type': 'BIGINT'},
                            {'name': 'latitude',  'type': 'DOUBLE'},
                            {'name': 'longitude', 'type': 'DOUBLE'},
                            {'name': 'altitude',  'type': 'DOUBLE'},
                        ]
                    }
                ],
                'timestamp': int(time.time() * 1000)
            }
            self.producer.send('robot_registration', value=payload, key=robot_id)

    def healthcheck_callback(self):
        for robot_id in self.robot_list:
            payload = {
                'msg_type': 'PING',
                'serial_number': robot_id,
                'robot_name': robot_id,
                'timestamp': int(time.time() * 1000)
            }
            
            print(f"{robot_id} HEALTHCHECK:", payload)

            self.producer.send(self.kafka_health_topic, value=payload, key=robot_id)
            self.get_logger().info(f"Sent health ping for Robot {robot_id}")

    def odom_callback(self, msg, robot_id):
        try:
            payload = {
                'timestamp': int(time.time() * 1000),
                'position_x': msg.pose.pose.position.x,
                'position_y': msg.pose.pose.position.y,
                'position_z': msg.pose.pose.position.z,
                # Faking the source topic to match the new numerical ID standard
                'source_topic': f"/{robot_id}/localisation/filtered_odom"
            }
            print(f"{robot_id} ODOM:", payload)

            self.producer.send(self.kafka_odom_topic, value=payload, key=robot_id)
        except Exception as e:
            self.get_logger().error(f"Error processing odom: {e}")

    def gps_callback(self, msg, robot_id):
        if math.isnan(msg.latitude) or math.isnan(msg.longitude):
            return
        try:
            payload = {
                'timestamp': int(time.time() * 1000),
                'latitude': msg.latitude,
                'longitude': msg.longitude,
                'altitude': msg.altitude
            }
            print(f"{robot_id} GPS:", payload)

            self.producer.send(self.kafka_gps_topic, value=payload, key=robot_id)
        except Exception as e:
            self.get_logger().error(f"Error processing GPS: {e}")

    def shutdown(self):
        if hasattr(self, 'producer') and self.producer:
            self.producer.flush()
            self.producer.close()

def main(args=None):
    rclpy.init(args=args)
    node = HybridKafkaBridge()
    try:
        rclpy.spin(node)
    except KeyboardInterrupt:
        pass
    finally:
        node.shutdown()
        node.destroy_node()
        rclpy.shutdown()

if __name__ == '__main__':
    main()