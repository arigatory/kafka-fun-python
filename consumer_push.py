from confluent_kafka import Consumer, KafkaError
import json


class Message:
    def __init__(self, content):
        self.content = content

    @classmethod
    def from_json(cls, json_str):
        data = json.loads(json_str)
        return cls(data["content"])


def create_consumer():
    return Consumer(
        {
            "bootstrap.servers": "localhost:9094,localhost:9095,localhost:9096",  # List of Kafka brokers
            "group.id": "push-group",  # Consumer group this consumer belongs to
            "auto.offset.reset": "earliest",
            # Start reading from the beginning of the topic if no committed offsets exist
            "enable.auto.commit": True,  # Enable automatic offset committing
            "fetch.min.bytes": 1024,  # Minimum amount of data the server should return for a fetch request
        }
    )


def push_consumer():
    consumer = create_consumer()
    consumer.subscribe(["my-first-topic"])

    try:
        while True:
            msgs = consumer.consume(
                num_messages=10, timeout=0.1
            )  # Fetch up to 10 messages with a 0.1s timeout
            for msg in msgs:
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        print("Reached end of partition")
                    else:
                        print(f"Error: {msg.error()}")
                else:
                    try:
                        message = Message.from_json(msg.value().decode("utf-8"))
                        print(f"Push Consumer: {message.content}")
                    except Exception as e:
                        print(f"Error in push consumer: {str(e)}")
    finally:
        consumer.close()


if __name__ == "__main__":
    push_consumer()
