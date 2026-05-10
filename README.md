# ksqlDB for GIS4IoRT Chist-Era Project

A real-time streaming pipeline built with Kafka, and ksqlDB. This system bridges ROS2 telemetry (odometry, GPS) with Kafka to perform real-time geofencing, collision detection, and proximity monitoring for robotics.

## Quick Start

Navigate to the deployment directory:
`cd deployments/ksqldb`

**1. In Terminal 1 (Start Infrastructure)**:
`docker compose up -d --build`

**2. System Configuration**:
Configure the system via the API (`http://localhost:8000`). You can use the FastAPI Swagger UI at `http://localhost:8000/docs` to:
* select robots and sensors,
* send zone definitions and humidity sensor settings,
* activate the desired queries.

**3. In Terminal 2 (Start Robot Bridge 101)**:
`docker compose exec -it ros2_bridge /bin/bash -c "source /opt/ros/humble/setup.bash && python3 /scripts/ros2_kafka_healthcheck_bridge.py"`

**4. In Terminal 3 (Start Robot Bridge 102)**:
`docker compose exec -it ros2_bridge /bin/bash -c "source /opt/ros/humble/setup.bash && python3 /scripts/ros2_kafka_healthcheck_bridge2.py"`

**5. In Terminal 4 (Start Sensor Generator)**:
`docker compose exec -it python-env python3 /scripts/sensor_realtime_generator.py`

**6. In Terminal 5 (Play ROSbag)**:
`docker compose exec -it ros2_bridge /bin/bash -c "source /opt/ros/humble/setup.bash && ros2 bag play /bags/rorbots_follower_leader_parcelle_1MONT_ros2"`

**7. View the WebSocket output in the GIS application, or open**:
`GIS4IoRT-ksqlDB/deployments/ksqldb/scripts/dashboard.html` in your browser.

**8. Cleanup (Terminal 1)**:
To kill everything and delete data run:
`docker compose down -v`
