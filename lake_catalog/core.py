"""规范JSON、内容摘要与时间戳的公共工具。"""
import hashlib
import json
import time

#: read_set中首次建表的明确absent标记，目录不接受隐式删表，也不接受缺省。
ABSENT = {"absent": True}


def canonical(obj):
    """规范JSON字节：键排序、紧凑分隔、拒绝非有限浮点。"""
    return json.dumps(obj, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf8")


def digest(obj):
    """对象的规范内容摘要（sha256十六进制）。"""
    return hashlib.sha256(canonical(obj)).hexdigest()


def file_digest(raw):
    return hashlib.sha256(raw).hexdigest()


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + ("%.3fZ" % (time.time() % 1))[1:]


def is_absent(marker):
    return isinstance(marker, dict) and marker.get("absent") is True and len(marker) == 1
