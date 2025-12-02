import json
from confluent_kafka import Consumer, KafkaError

bootstrap_servers = "localhost:9092"
topic = "traffic-data"

config = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'test-consumer',
    'auto.offset.reset': 'earliest'
}

try:
    consumer = Consumer(config)
    consumer.subscribe([topic])
    
    print(f"Consuming from topic: {topic}")
    print("Press Ctrl+C to stop\n")
    
    count = 0
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(f"Error: {msg.error()}")
                break
        
        count += 1
        value = json.loads(msg.value().decode('utf-8'))
        print(f"\nMessage {count}:")
        print(f"  Keys: {list(value.keys())}")
        print(f"  Sample values: {dict(list(value.items())[:3])}")
        if count >= 3:
            print("\nReceived 3 sample messages. Stopping...")
            break
            
except KeyboardInterrupt:
    print("\nStopped by user")
except Exception as e:
    print(f"Error: {e}")
    print("Make sure Kafka is running and topic exists")
finally:
    if 'consumer' in locals():
        consumer.close()

