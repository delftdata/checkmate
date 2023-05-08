from enum import Enum, auto

import msgpack
import cloudpickle
import lz4.frame


class Serializer(Enum):
    CLOUDPICKLE = auto()
    MSGPACK = auto()


def msgpack_serialization(serializable_object: object) -> bytes:
    return msgpack.packb(serializable_object)


def msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgpack.unpackb(serialized_object, strict_map_key=False)


def compressed_msgpack_serialization(serializable_object: object) -> bytes:
    return lz4.frame.compress(msgpack.packb(serializable_object))


def compressed_msgpack_deserialization(serialized_object: bytes) -> dict:
    return msgpack.unpackb(lz4.frame.decompress(serialized_object), strict_map_key=False)


def cloudpickle_serialization(serializable_object: object) -> bytes:
    return cloudpickle.dumps(serializable_object)


def cloudpickle_deserialization(serialized_object: bytes) -> dict:
    return cloudpickle.loads(serialized_object)


def compressed_cloudpickle_serialization(serializable_object: object) -> bytes:
    return lz4.frame.compress(cloudpickle.dumps(serializable_object))


def compressed_cloudpickle_deserialization(serialized_object: bytes) -> dict:
    return cloudpickle.loads(lz4.frame.decompress(serialized_object))
