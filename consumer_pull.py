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
            "bootstrap.servers": "localhost:9094,localhost:9095,localhost:9096",
            "group.id": "pull-group",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
            "fetch.min.bytes": 1024,
        }
    )


def pull_consumer():
    consumer = create_consumer()
    consumer.subscribe(["my-first-topic"])

    try:
        while True:
            msg = consumer.poll(1.0)
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
                    print(f"Pull Consumer: {message.content}")
                    consumer.commit(msg)
                except Exception as e:
                    print(f"Error in pull consumer: {str(e)}")
    finally:
        consumer.close()


if __name__ == "__main__":
    pull_consumer()
