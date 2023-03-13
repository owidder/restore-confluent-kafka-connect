import os
import sys
from avro.io import DatumReader
from avro.datafile import DataFileReader
from kafka import KafkaProducer
from getpass import getpass


KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS')
CERT_LOCATION = os.environ.get('CERT_LOCATION')
KEY_LOCATION = os.environ.get('KEY_LOCATION')
RESTORE_TOPIC_PREFIX = os.environ.get('RESTORE_TOPIC_PREFIX') or 'copy_of_'


def send_to_kafka(producer: KafkaProducer, topic: str, value: bytes, headers: list, key: bytes, partition: int):
    headers_tuple = [(h['key'], bytes(h['value'], 'utf8')) for h in headers]
    print(f"Send: topic={topic}, value={value}, headers={headers_tuple}, key={key}, partition={partition}")
    producer.send(topic=topic, value=value, headers=headers_tuple, key=key, partition=partition)
    producer.flush()


def get_topic_name_partition_offset(file_name: str) -> tuple:
    parts = file_name.split('+')
    topic_name = parts[0]
    partition = int(parts[1])
    offset = int(parts[2].split('.')[0])

    return topic_name, partition, offset


def read_headers(file_path: str) -> list:
    headers = []
    with open(file_path, 'rb') as f:
        headers_reader = DataFileReader(f, DatumReader())
        for h in headers_reader:
            headers = headers + h

        headers_reader.close()

        return headers


def read_key(file_path: str) -> bytes:
    key = b''
    with open(file_path, 'rb') as f:
        key_reader = DataFileReader(f, DatumReader())
        for k in key_reader:
            key = key + bytes(k, 'utf8')

        key_reader.close()

        return key


def read_value(file_path: str) -> bytes:
    with open(file_path, 'rb') as f:
        value = f.read()
    return value


def read_and_process_files(rootpath: str, producer: KafkaProducer):
    current_topic_name = ""
    current_partition = -1
    current_offset = -1

    current_value, current_headers, current_key = b'', [], b''

    for root, dirs, files in os.walk(rootpath, topdown=False):
        files.sort()
        for name in files:
            file_path = os.path.join(root, name)
            topic_name, partition, offset = get_topic_name_partition_offset(name)
            if topic_name != current_topic_name or partition != current_partition or offset != current_offset:
                if len(current_topic_name) > 0:
                    send_to_kafka(producer=producer,
                                  topic=f"{RESTORE_TOPIC_PREFIX}{current_topic_name}",
                                  value=current_value,
                                  headers=current_headers,
                                  key=current_key,
                                  partition=current_partition)
                current_topic_name, current_partition, current_offset = topic_name, partition, offset
                current_value, current_headers, current_key = b'', [], b''

            if name.endswith('.bin'):
                current_value = read_value(file_path)

            if name.endswith('.headers.avro'):
                current_headers = read_headers(file_path)

            if name.endswith('.keys.avro'):
                current_key = read_key(file_path)


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
    read_and_process_files(rootpath=sys.argv[1], producer=producer)
