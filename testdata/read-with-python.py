"""Read MMDB files with the Python maxminddb package for the model test.

The input is a JSON manifest on stdin: a list of {"file": path, "queries":
[address, ...]}. For each file and each reader mode (the libmaxminddb C
extension and the pure-Python reader), the output has one JSON line:

    {"file": ..., "mode": ..., "lookups": [[address, prefix_len, value]],
     "networks": [[network, value]], "error": ...}

A value is a canonical string that the Go side builds the same way. Integers
lose their MMDB type, and float32 values become float64 values, because
Python decodes them that way.
"""

import json
import struct
import sys

import maxminddb


def canon(value):
    if value is None:
        return "nil"
    if isinstance(value, bool):
        return "bool(%s)" % ("true" if value else "false")
    if isinstance(value, int):
        return "int(%d)" % value
    if isinstance(value, float):
        return "f(%s)" % struct.pack(">d", value).hex()
    if isinstance(value, str):
        return "str(%s)" % value.encode("utf-8").hex()
    if isinstance(value, (bytes, bytearray)):
        return "bytes(%s)" % bytes(value).hex()
    if isinstance(value, dict):
        items = sorted((k.encode("utf-8"), v) for k, v in value.items())
        return "map{" + "".join("%s:%s," % (k.hex(), canon(v)) for k, v in items) + "}"
    if isinstance(value, list):
        return "list[" + "".join(canon(v) + "," for v in value) + "]"
    return "unknown(%s)" % type(value).__name__


MODES = {
    "c-extension": maxminddb.MODE_MMAP_EXT,
    "pure-python": maxminddb.MODE_MEMORY,
}


def read(entry, mode_name):
    out = {"file": entry["file"], "mode": mode_name, "lookups": [], "networks": []}
    try:
        reader = maxminddb.open_database(entry["file"], MODES[mode_name])
    except Exception as e:  # noqa: BLE001 - report every failure to Go
        out["error"] = "open: %r" % e
        return out
    with reader:
        for address in entry["queries"]:
            try:
                value, prefix_len = reader.get_with_prefix_len(address)
                out["lookups"].append([address, prefix_len, canon(value)])
            except Exception as e:  # noqa: BLE001
                out["lookups"].append([address, -1, "error: %r" % e])
        try:
            for network, value in reader:
                out["networks"].append([str(network), canon(value)])
        except Exception as e:  # noqa: BLE001
            out["error"] = "iterate: %r" % e
    return out


def main():
    for entry in json.load(sys.stdin):
        for mode_name in MODES:
            print(json.dumps(read(entry, mode_name)))


if __name__ == "__main__":
    main()
