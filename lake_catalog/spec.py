"""候选发布规范：声明base_release、完整read_set与write_set，并给出内容摘要。

read_set的每个表要么是已注册快照ID，要么是明确absent标记（首次建表）；
write_set必须全部出现在read_set中；目录没有删表语义，未写表沿当前映射保留。
"""
from .core import ABSENT, digest, is_absent
from .errors import SpecError


def _check_table_name(name):
    if not isinstance(name, str) or not name:
        raise SpecError("表名必须是非空字符串: %r" % (name,))


class CandidateSpec:
    """已解析、已按注册快照解析内容摘要的候选规范。"""

    def __init__(self, operation_id, base_release, read_set, write_set):
        if not isinstance(operation_id, str) or not operation_id:
            raise SpecError("operation_id必须是非空字符串")
        if not isinstance(base_release, str) or not base_release:
            raise SpecError("base_release必须是非空字符串")
        if not isinstance(read_set, dict) or not read_set:
            raise SpecError("read_set必须是非空映射")
        if not isinstance(write_set, dict) or not write_set:
            raise SpecError("write_set必须是非空映射，目录没有空发布")
        self.operation_id = operation_id
        self.base_release = base_release
        self.read_set = dict(read_set)    # table -> snapshot_id 或 ABSENT
        self.write_set = dict(write_set)  # table -> snapshot_id
        for table, marker in self.read_set.items():
            _check_table_name(table)
            if is_absent(marker):
                continue
            if not isinstance(marker, str) or not marker:
                raise SpecError("read_set[%s]必须是快照ID或absent标记" % table)
        for table, snap in self.write_set.items():
            _check_table_name(table)
            if not isinstance(snap, str) or not snap or is_absent(snap):
                raise SpecError("write_set[%s]必须是快照ID，目录不允许隐式删表" % table)
            if table not in self.read_set:
                raise SpecError("写表%r必须出现在read_set" % table)

    @classmethod
    def from_manifest(cls, raw):
        return cls(raw["operation_id"], raw["base_release"],
                   raw["read_set"], raw["write_set"])

    def resolved(self, snapshot_digest):
        """把快照ID替换为内容摘要后的规范视图，用于摘要绑定与CAS。

        snapshot_digest: snapshot_id -> 内容摘要 的解析函数。
        """
        read = {}
        for table, marker in sorted(self.read_set.items()):
            read[table] = dict(ABSENT) if is_absent(marker) else snapshot_digest(marker)
        write = {table: snapshot_digest(snap)
                 for table, snap in sorted(self.write_set.items())}
        return {"base_release": self.base_release, "read_set": read,
                "write_set": write}

    def spec_digest(self, snapshot_digest):
        """操作规范摘要：含基准、完整读写集和内容身份。"""
        return digest(self.resolved(snapshot_digest))

    def as_dict(self):
        return {"operation_id": self.operation_id,
                "base_release": self.base_release,
                "read_set": self.read_set, "write_set": self.write_set}
