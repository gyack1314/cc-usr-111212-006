"""仓库文件布局、暂存与只读数据文件管理。

布局：
    <repo>/catalog.sqlite3   唯一可变的元数据文件
    <repo>/snapshots/<id>.json   不可变快照清单（规范JSON，只读）
    <repo>/data/<sha256[:2]>/<sha256>   内容寻址的不可变数据文件（只读）
    <repo>/staging/<operation_id>/<snapshot_id>/   候选暂存，失败留待清理

数据文件写入一次、随后chmod只读；目录不提供任何修改已有快照的接口。
"""
import json
import os
import shutil
from pathlib import Path

from .core import file_digest
from .errors import StagingError

READONLY = 0o444


def _fsync_dir(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def scan_jsonl(raw):
    """把JSONL字节解析为行对象列表；禁止空行，每行必须是JSON对象。"""
    text = raw.decode("utf8")
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()  # 唯一的末尾换行不算一行
    rows = []
    for line in lines:
        if not line.strip():
            raise StagingError("JSONL禁止空行")
        value = json.loads(line)
        if not isinstance(value, dict):
            raise StagingError("JSONL每行必须是JSON对象")
        rows.append(value)
    return rows


class FileRef:
    """一个已核验文件的身份证：内容摘要、字节数、行数。"""

    def __init__(self, sha256, bytes_, rows):
        self.sha256 = sha256
        self.bytes = bytes_
        self.rows = rows

    def as_dict(self):
        return {"sha256": self.sha256, "bytes": self.bytes, "rows": self.rows}

    def __repr__(self):
        return "FileRef(%s,%dB,%d行)" % (self.sha256[:12], self.bytes, self.rows)


class RepoStore:
    def __init__(self, root):
        self.root = Path(root)
        self.data_dir = self.root / "data"
        self.snapshots_dir = self.root / "snapshots"
        self.staging_dir = self.root / "staging"
        for d in (self.data_dir, self.snapshots_dir, self.staging_dir):
            d.mkdir(parents=True, exist_ok=True)

    # ---- 暂存 ----

    def stage_file(self, src, stage_dir):
        """把源文件复制进暂存目录并核验身份，返回FileRef与暂存路径。"""
        src = Path(src)
        raw = src.read_bytes()
        ref = FileRef(file_digest(raw), len(raw), len(scan_jsonl(raw)))
        stage_dir.mkdir(parents=True, exist_ok=True)
        dst = stage_dir / src.name
        dst.write_bytes(raw)
        return ref, dst

    def check_ref(self, ref, declared):
        """暂存实测身份必须等于清单声明身份。"""
        if (ref.sha256 != declared["sha256"] or ref.bytes != declared["bytes"]
                or ref.rows != declared["rows"]):
            raise StagingError(
                "文件身份不符: 实测%s != 声明%s" % (ref.as_dict(), declared))

    def verify_staged_file(self, path, declared):
        """发布前核对暂存文件：存在、字节数、摘要、行数。"""
        path = Path(path)
        if not path.is_file():
            raise StagingError("暂存文件缺失: %s" % path)
        raw = path.read_bytes()
        ref = FileRef(file_digest(raw), len(raw), len(scan_jsonl(raw)))
        self.check_ref(ref, declared)
        return ref

    # ---- 定稿（数据暂存完成的标志） ----

    def data_path(self, sha256):
        return self.data_dir / sha256[:2] / sha256

    def finalize_file(self, staged_path, ref):
        """把暂存文件原子搬进内容寻址存储并置只读；同内容幂等。"""
        final = self.data_path(ref.sha256)
        if final.exists():
            raw = final.read_bytes()
            if file_digest(raw) != ref.sha256 or len(raw) != ref.bytes:
                raise StagingError("内容寻址冲突: %s" % ref.sha256)
            return final
        final.parent.mkdir(parents=True, exist_ok=True)
        os.replace(staged_path, final)
        os.chmod(final, READONLY)
        _fsync_dir(final.parent)
        return final

    def write_manifest(self, snapshot_id, canonical_bytes):
        """快照清单写一次；同ID不同内容拒绝。"""
        path = self.snapshots_dir / (snapshot_id + ".json")
        if path.exists():
            if path.read_bytes() != canonical_bytes:
                raise StagingError("快照ID冲突: %s" % snapshot_id)
            return path
        path.write_bytes(canonical_bytes)
        os.chmod(path, READONLY)
        _fsync_dir(self.snapshots_dir)
        return path

    # ---- 读取 ----

    def read_rows(self, fileref_dict, verify=True, path=None):
        """按文件身份读取数据文件；默认核验摘要，句柄读已固化文件可关闭。

        path缺省为内容寻址存储位置，暂存文件可显式指定路径。"""
        path = Path(path) if path is not None else self.data_path(fileref_dict["sha256"])
        if not path.is_file():
            raise StagingError("数据文件缺失: %s" % path)
        raw = path.read_bytes()
        if verify and (len(raw) != fileref_dict["bytes"]
                       or file_digest(raw) != fileref_dict["sha256"]):
            raise StagingError("数据文件身份不符: %s" % path)
        rows = scan_jsonl(raw)
        if verify and len(rows) != fileref_dict["rows"]:
            raise StagingError("数据文件行数不符: %s" % path)
        return rows

    def staging_leftovers(self):
        """待清理暂存清单（本次不做GC，只报告）。"""
        out = []
        if self.staging_dir.is_dir():
            for p in sorted(self.staging_dir.rglob("*")):
                if p.is_file():
                    out.append(str(p.relative_to(self.root)))
        return out

    def unreferenced_data(self, referenced_sha256):
        """data/中未被任何release闭包引用的文件（失败候选的遗留）。"""
        out = []
        for p in sorted(self.data_dir.rglob("*")):
            if p.is_file() and p.name not in referenced_sha256:
                out.append(str(p.relative_to(self.root)))
        return out


def copy_tree_contents(src_dir, dst_dir):
    src_dir, dst_dir = Path(src_dir), Path(dst_dir)
    for p in src_dir.iterdir():
        if p.is_file():
            shutil.copy2(p, dst_dir / p.name)
