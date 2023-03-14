import os
import sys
from kafka import KafkaProducer
from getpass import getpass
import random
import string

KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS')
CERT_LOCATION = os.environ.get('CERT_LOCATION')
KEY_LOCATION = os.environ.get('KEY_LOCATION')


def random_string(length: int) -> str:
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))

def send_random_messages(producer: KafkaProducer, topic: str, count: int):
    for c in range(count):
        text = "\n".join([f"_{c}_{i}_: {random_string(random.randrange(1, 50))}" for i in range(random.randrange(1, 20))])
        headers_tuple = [(f"header_{h}", bytes(f"_{c}_{random_string(10)}_", 'utf8')) for h in range(random.randrange(1, 10))]
        key = f"_{c}_{random_string(random.randint(1, 30))}_"
        print(f"Send: topic={topic}, value={text}, headers={headers_tuple}, key={key}")
        producer.send(topic=topic, value=bytes(text, 'utf8'), headers=headers_tuple, key=bytes(key, 'utf8'))
        producer.flush()


if __name__ == "__main__":
    print(KAFKA_BROKERS)
    print(CERT_LOCATION)
    print(KEY_LOCATION)
    password = getpass()
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS,
                             security_protocol='SSL',
                             ssl_check_hostname=True,
                             ssl_certfile=CERT_LOCATION,
                             ssl_keyfile=KEY_LOCATION,
                             ssl_password=password)
    send_random_messages(producer=producer, topic=sys.argv[1], count=int(sys.argv[2]))
