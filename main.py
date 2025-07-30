#!/usr/bin/env python3

import datetime
import io
import math
import random
import sys

import fastavro

# Example schema from https://avro.apache.org/docs/1.11.1/getting-started-python/
SCHEMA = {
    "namespace": "example.avro",
    "type": "record",
    "name": "User",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "favorite_number",  "type": ["int", "null"]},
        {"name": "favorite_color", "type": ["string", "null"]}
    ]
}

# These next three functions do low-level bit manipulation to encode numbers
# according to the Avro specification document. We need this to correctly
# encode the block length and the object size.
# https://avro.apache.org/docs/1.12.0/specification/#binary-encoding

def zigzag_encode(n: int) -> int:
    """ZigZag encodes a signed integer into an unsigned integer."""
    return (n << 1) ^ (n >> 63 if n < 0 else 0)

def varint_encode(value: int) -> bytes:
    """Encodes an unsigned integer using varint encoding (as per Protobuf)."""
    result = bytearray()
    while value > 0x7F:
        result.append((value & 0x7F) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)

def encode_zigzag_varint(n: int) -> bytes:
    """Encodes a signed integer using ZigZag and varint (Protobuf style)."""
    zigzagged = zigzag_encode(n)
    return varint_encode(zigzagged)



# Generates fake data
def generate_record():
    return {
        "name": random.choice([
            "Chris",
            "Divya",
            "Kevin",
            "Yulingfei",
            # This last one generates a variable-length string which helps
            # test the varint-zigzag encoding logic.
            "01234567890" * random.randint(1, 1000)
        ]),
        "favorite_number": random.randint(0, 1024),
        "favorite_color": random.choice(["red", "yellow", "orange"])
    }

def write(name):
    # By default, avro chooses a random marker but we need to force it to use
    # ours so that we can re-use the same marker every time we write. The marker
    # gets written as part of the header.
    marker = b'0123456789abcdef'
    schema = fastavro.parse_schema(SCHEMA)

    # Generate a bunch of records to write.
    records = [generate_record() for _ in range(128)]

    # We are going to write the data into two files using different methods.
    # The first one uses avro the intended way; writing all the records.
    # The second one simulates appending data at the end of the file.

    with open(f"{name}.real.avro", 'wb') as out:
        # We set the sync_marker to the marker defined above.
        # We set sync_interval to 1 because we will be appending 1 record
        # at a time. We want to force the encoder to write each record in
        # a separate block. Normally, this is very inefficient but you wouldn't
        # be able to do it any other way when trying to append.
        fastavro.writer(out, schema, records, sync_marker = marker, sync_interval = 1)

    with open(f"{name}.fake.avro", 'wb') as out:
        # Use the regular writer to write the schema. We pass an empty list so
        # we don't write any records.
        fastavro.writer(out, schema, [], sync_marker = marker)

        # This loop simulates doing the appends - we manually write bytes at
        # the end of the file. Each run of the loop is like appending another
        # time to the blob.
        for rec in records:
            buf = io.BytesIO()

            # fastavro has an API to write a record without the schema but it
            # can only write one record *and* it doesn't give you the block
            # metadata. We will use this but we need to constuct the metadata
            # by hand later.
            fastavro.schemaless_writer(buf, schema, rec)
            data = buf.getvalue()

            # The "Spec:" comments below are copied from the specification
            # document to help explain which part is which.

            # Spec: A long indicating the count of objects in this block
            # We know we're only writing one at a time so we encode `1`
            out.write(encode_zigzag_varint(1))

            # Spec: A long indicating the size in bytes of the serialized
            # objects in the current block, after any codec is applied
            out.write(encode_zigzag_varint(len(data)))

            # Spec: The serialized objects. If a codec is specified, this is
            # compressed by that codec.
            out.write(data)

            # Spec: The file’s 16-byte sync marker.
            out.write(marker)


def read(name):
    with open(name, 'rb') as strm:
        schema = fastavro.parse_schema(SCHEMA)
        reader = fastavro.reader(strm, schema)
        for rec in reader:
            print(rec)


def main(args):
    if len(args):
        read(args[0])
    else:
        write(f"{datetime.datetime.now().isoformat()}")

if __name__ == "__main__":
    main(sys.argv[1:])
