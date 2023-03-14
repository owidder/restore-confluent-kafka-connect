import base64
import os
import sys
import json
from avro.io import DatumReader
from avro.datafile import DataFileReader
from kafka import KafkaProducer
from getpass import getpass


KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS')
CERT_LOCATION = os.environ.get('CERT_LOCATION')
KEY_LOCATION = os.environ.get('KEY_LOCATION')
RESTORE_TOPIC_PREFIX = os.environ.get('RESTORE_TOPIC_PREFIX') or 'copy_of_'


def send_to_kafka(producer: KafkaProducer, topic: str, lines: list, headers: list, keys: list, partition: int):
    for line, header_list, key in zip(lines, headers, keys):
        #print(f"producer.send(topic={topic}, value={line}, headers={header_list}, key={key}, partition={partition})")
        producer.send(topic=topic, value=line, headers=header_list, key=key, partition=partition)
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
        for header_list in headers_reader:
            headers.append(header_list)

        headers_reader.close()

        return [[(h['key'], bytes(h['value'], 'utf8')) for h in header_list] for header_list in headers]


def read_keys(file_path: str) -> list:
    keys = []
    with open(file_path, 'rb') as f:
        key_reader = DataFileReader(f, DatumReader())
        for key in key_reader:
            keys.append(key)

        key_reader.close()

        return [bytes(k, 'utf8') for k in keys]


def read_lines(file_path: str) -> list:
    with open(file_path, 'rb') as f:
        raw_lines = f.readlines()

    lines = [base64.b64decode(json.loads(raw_line)) for raw_line in raw_lines]
    return lines


def read_and_process_files(rootpath: str, producer: KafkaProducer):
    current_topic_name = ""
    current_partition = -1
    current_offset = -1

    current_lines, current_headers, current_keys = [], [], []

    for root, dirs, files in os.walk(rootpath, topdown=False):
        files.sort()
        for name in files:
            file_path = os.path.join(root, name)
            topic_name, partition, offset = get_topic_name_partition_offset(name)
            if topic_name != current_topic_name or partition != current_partition or offset != current_offset:
                if len(current_topic_name) > 0:
                    send_to_kafka(producer=producer,
                                  topic=f"{RESTORE_TOPIC_PREFIX}{current_topic_name}",
                                  lines=current_lines,
                                  headers=current_headers,
                                  keys=current_keys,
                                  partition=current_partition)
                current_topic_name, current_partition, current_offset = topic_name, partition, offset
                current_lines, current_headers, current_keys = [], [], []

            if name.endswith('.json'):
                current_lines = read_lines(file_path)

            if name.endswith('.headers.avro'):
                current_headers = read_headers(file_path)

            if name.endswith('.keys.avro'):
                current_keys = read_keys(file_path)


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
    #read_and_process_files(rootpath=sys.argv[1], producer=None)
