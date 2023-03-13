import os
from avro.io import DatumReader
from avro.datafile import DataFileReader


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


def send_message(topic_name: str, partition: int, offset: int, value: bytes, headers: list, key: bytes):
    if len(topic_name) > 0:
        print(f"send to {topic_name}/{partition}/{offset}: {value}, {headers}, {key}")


def read_and_process_files(rootpath: str):
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
                send_message(topic_name=current_topic_name, partition=current_partition, offset=current_offset, value=current_value, headers=current_headers, key=current_key)
                current_topic_name, current_partition, current_offset = topic_name, partition, offset
                current_value, current_headers, current_key = b'', [], b''

            if name.endswith('.bin'):
                current_value = read_value(file_path)

            if name.endswith('.headers.avro'):
                current_headers = read_headers(file_path)

            if name.endswith('.keys.avro'):
                current_key = read_key(file_path)


if __name__ == "__main__":
    read_and_process_files("./s3")
