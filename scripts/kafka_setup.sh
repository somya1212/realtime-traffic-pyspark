set -e

case "$1" in
  start)
    echo "Starting Kafka and Zookeeper..."
    docker-compose up -d
    echo "Waiting for Kafka to be ready..."
    sleep 10
    echo "Kafka is running!"
    echo "Zookeeper: localhost:2181"
    echo "Kafka: localhost:9092"
    ;;
  stop)
    echo "Stopping Kafka and Zookeeper..."
    docker-compose down
    echo "Kafka stopped."
    ;;
  restart)
    echo "Restarting Kafka and Zookeeper..."
    docker-compose restart
    ;;
  status)
    echo "Kafka containers status:"
    docker-compose ps
    ;;
  logs)
    docker-compose logs -f kafka
    ;;
  create-topic)
    if [ -z "$2" ]; then
      echo "Usage: $0 create-topic <topic-name>"
      exit 1
    fi
    echo "Creating topic: $2"
    docker exec -it kafka kafka-topics --create \
      --topic "$2" \
      --bootstrap-server localhost:9092 \
      --partitions 1 \
      --replication-factor 1
    echo "Topic '$2' created successfully!"
    ;;
  list-topics)
    echo "Listing topics:"
    docker exec -it kafka kafka-topics --list \
      --bootstrap-server localhost:9092
    ;;
  consumer)
    if [ -z "$2" ]; then
      echo "Usage: $0 consumer <topic-name>"
      exit 1
    fi
    echo "Consuming from topic: $2 (Ctrl+C to stop)"
    docker exec -it kafka kafka-console-consumer \
      --topic "$2" \
      --from-beginning \
      --bootstrap-server localhost:9092
    ;;
  *)
    echo "Usage: $0 {start|stop|restart|status|logs|create-topic|list-topics|consumer}"
    echo ""
    echo "Commands:"
    echo "  start              Start Kafka and Zookeeper containers"
    echo "  stop               Stop Kafka and Zookeeper containers"
    echo "  restart            Restart Kafka and Zookeeper containers"
    echo "  status             Show container status"
    echo "  logs               Show Kafka logs (follow mode)"
    echo "  create-topic NAME  Create a new Kafka topic"
    echo "  list-topics        List all Kafka topics"
    echo "  consumer TOPIC     Consume messages from a topic (for testing)"
    exit 1
    ;;
esac

