from confluent_kafka import Consumer, KafkaError

# Kafka Consumer Configuration
consumer = Consumer({
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'my-consumer-group',
    'auto.offset.reset': 'earliest'
})

# Subscribe to the Kafka topics
consumer.subscribe(['api1', 'api2'])

while True:
    message = consumer.poll(1.0)  # Adjust the timeout as needed

    if message is None:
        continue
    if message.error():
        if message.error().code() == KafkaError._PARTITION_EOF:
            print(f"Reached end of partition: {message.topic()} [{message.partition()}]")
        else:
            print(f"Error while consuming from topic {message.topic()}: {message.error()}")
    else:
        # Print the received message value
        print(f"Received message: {message.value().decode('utf-8')}")

# Close the Kafka consumer gracefully when done
consumer.close()