from confluent_kafka import Producer
import json
import time


class Message:
    def __init__(self, content):
        self.content = content

    def to_json(self):
        return json.dumps({"content": self.content})


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def create_producer():
    return Producer(
        {
            "bootstrap.servers": "localhost:9094,localhost:9095,localhost:9096",
            "acks": "all",
            "retries": 5,
            "retry.backoff.ms": 1000,
        }
    )


def produce_messages():
    producer = create_producer()
    for i in range(10):
        message = Message(f"Message {i}")
        producer.produce(
            "my-first-topic",
            message.to_json().encode("utf-8"),
            callback=delivery_report,
        )
        producer.poll(0)
        time.sleep(1)
    producer.flush()


if __name__ == "__main__":
    produce_messages()
