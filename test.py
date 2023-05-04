import sys
import msgpack

if __name__ == "__main__":
    args = sys.stdin.buffer.read()
    value = args
    print(msgpack.unpackb(value, use_list=False))