SET 'auto.offset.reset' = 'earliest';

CREATE TABLE robot_registry (
  robot_id VARCHAR PRIMARY KEY,
  status VARCHAR
) WITH (
  KAFKA_TOPIC='robot_registration',
  VALUE_FORMAT='JSON',
  PARTITIONS=2
);

CREATE TABLE sensor_registry (
  sensor_id VARCHAR PRIMARY KEY,
  status VARCHAR
) WITH (
  KAFKA_TOPIC='sensor_registration',
  VALUE_FORMAT='JSON',
  PARTITIONS=2
);