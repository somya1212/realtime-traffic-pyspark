import time
import json
import csv
from pathlib import Path
from confluent_kafka import Producer

bootstrap_servers = "localhost:9092"
topic = "traffic-data"
seed_dir = Path("data/seeds")
interval = 2

config = {
    'bootstrap.servers': bootstrap_servers
}

try:
    producer = Producer(config)
except Exception as e:
    print(f"Failed to create producer: {e}")
    print("Make sure Kafka is running: ./scripts/kafka_setup.sh start")
    exit(1)

files = sorted(seed_dir.glob("*.csv"))
if not files:
    print("No CSV files found in data/seeds/")
    exit(1)

print(f"Found {len(files)} files. Starting producer...")
print(f"Publishing to topic: {topic}")

i = 0
msg_count = 0
try:
    while True:
        src_file = files[i % len(files)]
        
        with open(src_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                # Send each row as a JSON message
                message = json.dumps(row).encode('utf-8')
                producer.produce(topic, message, callback=lambda err, msg: None)
                producer.poll(0)
                msg_count += 1
                if msg_count % 10 == 0:
                    print(f"Sent {msg_count} messages...")
                time.sleep(interval)
        
        print(f"Finished sending {src_file.name} ({msg_count} total messages), cycling to next file...")
        i += 1
        
except KeyboardInterrupt:
    print("\nStopping producer...")
finally:
    producer.flush()
    print("Producer closed.")

