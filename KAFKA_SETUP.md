# Kafka Setup Guide

This guide explains how to set up and use Kafka with Docker for the Real-Time Traffic Analytics project.

## Prerequisites

- Docker Desktop installed and running
- Docker Compose installed (usually comes with Docker Desktop)

## Quick Start

### 1. Start Kafka and Zookeeper

```bash
# Using the helper script
./scripts/kafka_setup.sh start

# Or using docker-compose directly
docker-compose up -d
```

This will start:
- **Zookeeper** on port `2181`
- **Kafka** on port `9092`

### 2. Verify Kafka is Running

```bash
# Check container status
./scripts/kafka_setup.sh status

# Or
docker-compose ps
```

You should see both `zookeeper` and `kafka` containers running.

### 3. Create a Topic

```bash
# Create the traffic-data topic
./scripts/kafka_setup.sh create-topic traffic-data

# Or manually
docker exec -it kafka kafka-topics --create \
  --topic traffic-data \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

### 4. Verify Topic was Created

```bash
# List all topics
./scripts/kafka_setup.sh list-topics

# Or manually
docker exec -it kafka kafka-topics --list \
  --bootstrap-server localhost:9092
```

## Helper Script Commands

The `scripts/kafka_setup.sh` script provides convenient commands:

| Command | Description |
|---------|-------------|
| `start` | Start Kafka and Zookeeper containers |
| `stop` | Stop Kafka and Zookeeper containers |
| `restart` | Restart Kafka and Zookeeper containers |
| `status` | Show container status |
| `logs` | Show Kafka logs (follow mode) |
| `create-topic <name>` | Create a new Kafka topic |
| `list-topics` | List all Kafka topics |
| `consumer <topic>` | Consume messages from a topic (for testing) |

## Testing Kafka Setup

### Test Producer (Manual)

You can test if Kafka is working by manually producing and consuming messages:

**Terminal 1 - Producer:**
```bash
docker exec -it kafka kafka-console-producer \
  --topic traffic-data \
  --bootstrap-server localhost:9092
```

Type some messages and press Enter after each message.

**Terminal 2 - Consumer:**
```bash
./scripts/kafka_setup.sh consumer traffic-data
```

You should see the messages you typed in Terminal 1 appear in Terminal 2.

## Using Kafka with the Project

### 1. Install Python Dependencies

```bash
source .venv/bin/activate
pip install -r requirements.txt
```

This installs `kafka-python` which is needed for the producer script.

### 2. Start Kafka (if not already running)

```bash
./scripts/kafka_setup.sh start
./scripts/kafka_setup.sh create-topic traffic-data
```

### 3. Run Kafka Producer

```bash
# Start the Kafka producer (reads from data/seeds/ and publishes to Kafka)
python src/kafka_producer.py
```

### 4. Run Spark Streaming Job (Kafka Mode)

```bash
# Run stream job configured for Kafka
python -m src.stream_job --config config/stream_kafka.yaml
```

## Stopping Kafka

When you're done:

```bash
# Stop Kafka and Zookeeper
./scripts/kafka_setup.sh stop

# Or
docker-compose down
```

To remove all data (topics, messages, etc.):

```bash
docker-compose down -v
```

## Troubleshooting

### Kafka won't start

1. **Check Docker is running:**
   ```bash
   docker ps
   ```

2. **Check ports are available:**
   - Port 2181 (Zookeeper)
   - Port 9092 (Kafka)
   
   If ports are in use, you can modify `docker-compose.yml` to use different ports.

3. **View logs:**
   ```bash
   ./scripts/kafka_setup.sh logs
   # Or
   docker-compose logs kafka
   ```

### Can't connect to Kafka from Python

- Make sure Kafka is running: `./scripts/kafka_setup.sh status`
- Verify the topic exists: `./scripts/kafka_setup.sh list-topics`
- Check the bootstrap server address: `localhost:9092`

### Topic already exists error

If you get "Topic already exists" when creating a topic, that's fine - it means the topic is already created. You can verify with `./scripts/kafka_setup.sh list-topics`.

## Configuration

Kafka configuration is in `docker-compose.yml`. Key settings:

- **Kafka Port**: `9092` (change if needed)
- **Zookeeper Port**: `2181` (change if needed)
- **Replication Factor**: `1` (single broker setup, fine for development)
- **Partitions**: `1` (can be increased for better parallelism)

For production, you'd want multiple brokers and higher replication factors, but for this project, a single broker is sufficient.

## Additional Resources

- [Kafka Documentation](https://kafka.apache.org/documentation/)
- [Docker Compose Documentation](https://docs.docker.com/compose/)
- [kafka-python Library](https://kafka-python.readthedocs.io/)

