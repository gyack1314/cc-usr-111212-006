"""多表发布目录：SQLite元数据 + 不可变快照 + 事务CAS切换当前release。

协议要点：
- 当前release与全部历史release映射存于SQLite，同一事务内切换，数据文件只读；
- 候选声明base_release、完整read_set（含absent首次建表标记）与write_set；
- 验证在提交外进行（不串行化计算），记录所见表版本；提交在单个
  BEGIN IMMEDIATE 事务内重验read_set与当前映射一致（CAS），才合并write_set
  生成新release；全局release前进但读集未变不拒绝；
- 操作ID与规范摘要绑定：同ID同内容重试返回原release，同ID不同内容拒绝；
- 数据暂存定稿完成后才能提交元数据；提交后未回执的重试命中幂等记录，
  不会再造一代。
"""
import json
import sqlite3
import threading
from pathlib import Path

from .core import ABSENT, canonical, digest, is_absent, now_iso
from .errors import (CommitConflict, IdempotencyConflict, SpecError,
                     StagingError, UnknownRelease, UnknownSnapshot,
                     ValidationConflict, ValidationFailed)
from .spec import CandidateSpec
from .store import RepoStore

SCHEMA = """
PRAGMA journal_mode=WAL;
CREATE TABLE IF NOT EXISTS meta(
  key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS snapshots(
  id TEXT PRIMARY KEY, table_name TEXT NOT NULL,
  digest TEXT NOT NULL, manifest TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS releases(
  id TEXT PRIMARY KEY, parent TEXT, operation_id TEXT,
  spec_digest TEXT, mapping TEXT NOT NULL, created_at TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS current(
  id INTEGER PRIMARY KEY CHECK(id=1), release_id TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS operations(
  operation_id TEXT PRIMARY KEY, spec_digest TEXT NOT NULL,
  release_id TEXT NOT NULL, committed_at TEXT NOT NULL);
"""

HOOK_POINTS = ("after_validation", "before_commit", "after_commit_before_reply")


class Receipt:
    """提交回执。replayed=True表示同操作同内容的重试返回了原release。"""

    def __init__(self, operation_id, spec_digest, release_id, parent,
                 mapping, replayed, committed_at):
        self.operation_id = operation_id
        self.spec_digest = spec_digest
        self.release_id = release_id
        self.parent = parent
        self.mapping = mapping
        self.replayed = replayed
        self.committed_at = committed_at

    def as_dict(self):
        return {"operation_id": self.operation_id, "spec_digest": self.spec_digest,
                "release_id": self.release_id, "parent": self.parent,
                "mapping": self.mapping, "replayed": self.replayed,
                "committed_at": self.committed_at}


class StagedSnapshot:
    """已暂存待发布的新表快照：清单、内容摘要与暂存文件位置。"""

    def __init__(self, manifest, stage_paths):
        self.manifest = manifest
        self.stage_paths = stage_paths  # sha256 -> 暂存路径
        self.id = manifest["id"]
        self.table = manifest["table"]
        self.digest = digest(manifest)


class ValidationTicket:
    """验证结论：绑定规范摘要与验证时所见的当前表版本。

    提交时若当前映射中读集版本已变，本票据结论作废，必须重新验证。
    """

    def __init__(self, spec_digest, seen_versions, current_release,
                 target_mapping, checks, validated_at):
        self.spec_digest = spec_digest
        self.seen_versions = seen_versions
        self.current_release = current_release
        self.target_mapping = target_mapping
        self.checks = checks
        self.validated_at = validated_at

    def as_dict(self):
        return {"spec_digest": self.spec_digest, "seen_versions": self.seen_versions,
                "current_release": self.current_release,
                "target_mapping": self.target_mapping, "checks": self.checks,
                "validated_at": self.validated_at}


class Catalog:
    def __init__(self, root):
        self.root = Path(root)
        self.db_path = self.root / "catalog.sqlite3"
        self.store = RepoStore(self.root)
        self._hooks = {p: [] for p in HOOK_POINTS}
        self._audit = []
        self._audit_lock = threading.Lock()

    # ---- 生命周期 ----

    @classmethod
    def create(cls, root):
        root = Path(root)
        root.mkdir(parents=True, exist_ok=True)
        cat = cls(root)
        with cat._connect() as db:
            db.executescript(SCHEMA)
        return cat

    @classmethod
    def open(cls, root):
        cat = cls(root)
        if not cat.db_path.is_file():
            raise SpecError("目录不存在: %s" % cat.db_path)
        return cat

    def _connect(self):
        db = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
        db.row_factory = sqlite3.Row
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA foreign_keys=ON")
        return db

    def add_hook(self, point, fn):
        """注册可控调度/故障注入点：after_validation、before_commit、
        after_commit_before_reply。fn(ctx)抛异常即注入失败。"""
        if point not in self._hooks:
            raise SpecError("未知调度点: %s" % point)
        self._hooks[point].append(fn)

    def _run_hooks(self, point, ctx):
        for fn in self._hooks[point]:
            fn(ctx)

    def _log(self, entry):
        entry = dict(entry, at=now_iso())
        with self._audit_lock:
            self._audit.append(entry)

    def audit_log(self):
        with self._audit_lock:
            return list(self._audit)

    # ---- 引导 ----

    def bootstrap(self, manifest_path):
        """从冻结清单建立初始release；只导入初始映射引用的快照。"""
        from lake_data.io import load  # 复用已有读取工具的严格JSON解析
        manifest, source_digest = load(manifest_path)
        source_dir = Path(manifest_path).parent
        snapshots = {s["id"]: s for s in manifest["table_snapshots"]}
        initial = manifest["initial_release"]
        mapping = initial["tables"]
        if not isinstance(mapping, dict):
            raise SpecError("initial_release.tables必须是映射")
        constraints = manifest.get("constraints", [])
        for c in constraints:
            for endpoint in (c["from_table"], c["to_table"]):
                if endpoint not in mapping:
                    raise SpecError("约束端点表%r不在初始映射中" % endpoint)
        with self._connect() as db:
            if db.execute("SELECT 1 FROM current WHERE id=1").fetchone():
                raise SpecError("目录已引导")
        staged = {}
        for table, sid in sorted(mapping.items()):
            if sid not in snapshots:
                raise UnknownSnapshot("初始映射引用未知快照: %s" % sid)
            snap = snapshots[sid]
            if snap["table"] != table:
                raise SpecError("快照%s表名不符: %s" % (sid, table))
            staged[sid] = self.stage_snapshot("bootstrap", source_dir, snap)
        # 初始映射自身必须满足固定外键约束（读暂存文件）
        rows_by_table = {}
        for table, sid in mapping.items():
            rows_by_table[table] = self._rows_of_snapshot(
                staged[sid].manifest, staged[sid].stage_paths)
        self._check_fk(constraints, set(mapping), rows_by_table)
        for sid in sorted(staged):
            self._finalize(staged[sid])
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                for key, value in (("format", "1"),
                                   ("constraints", canonical(constraints).decode("utf8")),
                                   ("source_manifest_sha256", source_digest)):
                    db.execute("INSERT INTO meta(key,value) VALUES(?,?)", (key, value))
                db.execute(
                    "INSERT INTO releases(id,parent,operation_id,spec_digest,mapping,created_at)"
                    " VALUES(?,?,?,?,?,?)",
                    (initial["id"], None, "bootstrap", None,
                     canonical(mapping).decode("utf8"), now_iso()))
                db.execute("INSERT INTO current(id,release_id) VALUES(1,?)",
                           (initial["id"],))
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise
        self._log({"kind": "bootstrap", "release": initial["id"],
                   "mapping": mapping, "source_sha256": source_digest})
        return initial["id"]

    # ---- 快照暂存与定稿 ----

    def stage_snapshot(self, operation_id, source_dir, snapshot_def):
        """把新表文件复制进暂存区并核验清单声明的身份。"""
        sid = snapshot_def["id"]
        table = snapshot_def["table"]
        stage_dir = self.store.staging_dir / operation_id / sid
        files, total = [], 0
        for declared in snapshot_def["files"]:
            ref, _ = self.store.stage_file(Path(source_dir) / declared["path"], stage_dir)
            self.store.check_ref(ref, declared)
            files.append(ref.as_dict())
            total += ref.rows
        if total != snapshot_def["rows"]:
            raise StagingError("快照%s行数不符: 文件合计%d != 声明%d"
                               % (sid, total, snapshot_def["rows"]))
        files.sort(key=lambda f: f["sha256"])
        manifest = {"id": sid, "table": table, "rows": total, "files": files}
        # 暂存路径按源文件名定位，并逐文件复核身份
        stage_paths = {}
        for declared in snapshot_def["files"]:
            p = stage_dir / Path(declared["path"]).name
            ref = self.store.verify_staged_file(p, declared)
            stage_paths[ref.sha256] = str(p)
        return StagedSnapshot(manifest, stage_paths)

    def finalize(self, staged):
        """数据暂存定稿：文件入内容寻址只读存储，快照注册进目录。幂等。"""
        return self._finalize(staged)

    def _finalize(self, staged):
        for f in staged.manifest["files"]:
            staged_path = staged.stage_paths.get(f["sha256"])
            if staged_path is None:
                raise StagingError("暂存缺失: %s" % f["sha256"])
            ref = self.store.verify_staged_file(staged_path, f)
            self.store.finalize_file(staged_path, ref)
        manifest_bytes = canonical(staged.manifest)
        self.store.write_manifest(staged.id, manifest_bytes)
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                row = db.execute("SELECT digest FROM snapshots WHERE id=?",
                                 (staged.id,)).fetchone()
                if row is None:
                    db.execute(
                        "INSERT INTO snapshots(id,table_name,digest,manifest)"
                        " VALUES(?,?,?,?)",
                        (staged.id, staged.table, staged.digest,
                         manifest_bytes.decode("utf8")))
                elif row["digest"] != staged.digest:
                    raise StagingError("快照ID已登记不同内容: %s" % staged.id)
                db.execute("COMMIT")
            except BaseException:
                db.execute("ROLLBACK")
                raise
        return staged.digest

    # ---- 查询辅助 ----

    def _snapshot_digest_db(self, db, sid):
        row = db.execute("SELECT digest FROM snapshots WHERE id=?", (sid,)).fetchone()
        if row is None:
            raise UnknownSnapshot("未知快照: %s" % sid)
        return row["digest"]

    def snapshot_digest(self, sid):
        with self._connect() as db:
            return self._snapshot_digest_db(db, sid)

    def snapshot_manifest(self, sid):
        with self._connect() as db:
            row = db.execute("SELECT manifest FROM snapshots WHERE id=?",
                             (sid,)).fetchone()
        if row is None:
            raise UnknownSnapshot("未知快照: %s" % sid)
        return json.loads(row["manifest"])

    def current_release(self):
        """单条SQL读取当前release与其映射，读者只见完整旧代或完整新代。"""
        with self._connect() as db:
            row = db.execute(
                "SELECT r.id AS id, r.mapping AS mapping FROM current c"
                " JOIN releases r ON r.id=c.release_id WHERE c.id=1").fetchone()
        if row is None:
            raise UnknownRelease("目录未引导")
        return row["id"], json.loads(row["mapping"])

    def release_info(self, release_id):
        with self._connect() as db:
            row = db.execute(
                "SELECT id,parent,operation_id,spec_digest,mapping,created_at"
                " FROM releases WHERE id=?", (release_id,)).fetchone()
        if row is None:
            raise UnknownRelease("未知release: %s" % release_id)
        return {"id": row["id"], "parent": row["parent"],
                "operation_id": row["operation_id"],
                "spec_digest": row["spec_digest"],
                "mapping": json.loads(row["mapping"]),
                "created_at": row["created_at"]}

    def releases(self):
        with self._connect() as db:
            rows = db.execute(
                "SELECT id,parent,operation_id,mapping,created_at FROM releases"
                " ORDER BY created_at, id").fetchall()
        return [{"id": r["id"], "parent": r["parent"],
                 "operation_id": r["operation_id"],
                 "mapping": json.loads(r["mapping"]),
                 "created_at": r["created_at"]} for r in rows]

    def constraints(self):
        with self._connect() as db:
            row = db.execute("SELECT value FROM meta WHERE key='constraints'").fetchone()
        return json.loads(row["value"]) if row else []

    def compare_read_set(self, resolved_read_set, mapping=None):
        """读集（已解析为内容摘要）与当前映射逐表比较，返回比较证据。"""
        if mapping is None:
            _, mapping = self.current_release()
        out = []
        for table, expected in sorted(resolved_read_set.items()):
            current_sid = mapping.get(table)
            current = ("absent" if current_sid is None
                       else self.snapshot_digest(current_sid))
            want = "absent" if is_absent(expected) else expected
            out.append({"table": table, "expected": want, "current": current,
                        "match": want == current})
        return out

    # ---- 验证 ----

    def validate(self, spec, staged_map):
        """发布前核对：规范自检、读集对当时当前映射、新表文件/行数、固定外键。

        不持有任何写锁，多个生产者可并发验证；结论以票据形式记录所见版本。
        """
        if not isinstance(spec, CandidateSpec):
            raise SpecError("需要CandidateSpec")
        staged_map = dict(staged_map or {})
        constraints = self.constraints()
        with self._connect() as db:
            base = db.execute("SELECT mapping FROM releases WHERE id=?",
                              (spec.base_release,)).fetchone()
            if base is None:
                raise UnknownRelease("未知base_release: %s" % spec.base_release)
            base_mapping = json.loads(base["mapping"])
            # read_set必须自洽于声明的基准release
            for table, marker in sorted(spec.read_set.items()):
                if is_absent(marker):
                    if table in base_mapping:
                        raise SpecError("read_set标absent但基准含表%r" % table)
                elif base_mapping.get(table) != marker:
                    raise SpecError("read_set[%s]=%r与基准%s不一致"
                                    % (table, marker, spec.base_release))
            resolver = self._resolver(db, staged_map)
            resolved = spec.resolved(resolver)
            spec_digest = digest(resolved)
            # 固定外键：from或to被写即触发，两个端点都必须列入read_set
            triggered = self._triggered(constraints, spec.write_set)
            for c in triggered:
                for endpoint in (c["from_table"], c["to_table"]):
                    if endpoint not in spec.read_set:
                        raise SpecError(
                            "约束%s.%s->%s.%s的依赖表%r必须列入read_set"
                            % (c["from_table"], c["from_field"], c["to_table"],
                               c["to_field"], endpoint))
            # 写表快照的表名必须与映射键一致
            for table, sid in sorted(spec.write_set.items()):
                if self._snapshot_table(db, staged_map, sid) != table:
                    raise SpecError("write_set[%s]的快照%s表名不符" % (table, sid))
        # 读集对当时当前映射（验证针对write_set合并进当时当前映射的版本）
        current_id, current_mapping = self.current_release()
        comparison = self.compare_read_set(resolved["read_set"], current_mapping)
        if not all(c["match"] for c in comparison):
            self._log({"kind": "validate", "operation_id": spec.operation_id,
                       "spec_digest": spec_digest, "ok": False,
                       "comparison": comparison})
            raise ValidationConflict(
                "验证时读集已与当前映射不一致: %s"
                % json.dumps([c for c in comparison if not c["match"]],
                             ensure_ascii=False))
        # 新表所有引用文件、行数核对（针对暂存内容）
        file_checks = []
        for table, sid in sorted(spec.write_set.items()):
            staged = staged_map.get(sid)
            if staged is None:
                raise StagingError("写表快照%s未暂存" % sid)
            total = 0
            for f in staged.manifest["files"]:
                ref = self.store.verify_staged_file(staged.stage_paths[f["sha256"]], f)
                file_checks.append({"snapshot": sid, "sha256": ref.sha256,
                                    "bytes": ref.bytes, "rows": ref.rows, "ok": True})
                total += ref.rows
            if total != staged.manifest["rows"]:
                raise ValidationFailed("快照%s行数合计不符" % sid)
        # 固定外键核对：目标映射 = 当前映射合并write_set
        target_mapping = dict(current_mapping)
        target_mapping.update(spec.write_set)
        rows_by_table = {}
        for c in triggered:
            for endpoint in (c["from_table"], c["to_table"]):
                if endpoint in rows_by_table:
                    continue
                if endpoint not in target_mapping:
                    raise SpecError("约束端点表%r在目标映射中不存在" % endpoint)
                sid = target_mapping[endpoint]
                if sid in staged_map:
                    staged = staged_map[sid]
                    rows_by_table[endpoint] = self._rows_of_snapshot(
                        staged.manifest, staged.stage_paths)
                else:
                    rows_by_table[endpoint] = self._rows_of_snapshot(
                        self.snapshot_manifest(sid))
        fk_checks = self._check_fk(constraints, set(spec.write_set), rows_by_table)
        ticket = ValidationTicket(
            spec_digest=spec_digest,
            seen_versions={t: ("absent" if is_absent(v) else v)
                           for t, v in sorted(resolved["read_set"].items())},
            current_release=current_id,
            target_mapping=target_mapping,
            checks={"files": file_checks, "constraints": fk_checks,
                    "read_set_comparison": comparison},
            validated_at=now_iso())
        self._log({"kind": "validate", "operation_id": spec.operation_id,
                   "spec_digest": spec_digest, "ok": True,
                   "current_release": current_id, "comparison": comparison})
        self._run_hooks("after_validation",
                        {"spec": spec, "ticket": ticket, "catalog": self})
        return ticket

    def _resolver(self, db, staged_map):
        def resolve(sid):
            staged = staged_map.get(sid)
            if staged is not None:
                return staged.digest
            return self._snapshot_digest_db(db, sid)
        return resolve

    def _snapshot_table(self, db, staged_map, sid):
        staged = staged_map.get(sid)
        if staged is not None:
            return staged.table
        row = db.execute("SELECT table_name FROM snapshots WHERE id=?",
                         (sid,)).fetchone()
        if row is None:
            raise UnknownSnapshot("未知快照: %s" % sid)
        return row["table_name"]

    @staticmethod
    def _triggered(constraints, write_set):
        out = []
        for c in constraints:
            if c["from_table"] in write_set or c["to_table"] in write_set:
                out.append(c)
        return out

    def _rows_of_snapshot(self, manifest, stage_paths=None):
        rows = []
        for f in manifest["files"]:
            path = stage_paths.get(f["sha256"]) if stage_paths else None
            rows.extend(self.store.read_rows(f, verify=True, path=path))
        if len(rows) != manifest["rows"]:
            raise ValidationFailed("快照%s行数不符" % manifest["id"])
        return rows

    def _check_fk(self, constraints, write_tables, rows_by_table):
        """固定外键：非null来源值必须落在目标主键集合内，目标主键唯一。"""
        results = []
        for c in constraints:
            if c["from_table"] not in write_tables and c["to_table"] not in write_tables:
                continue
            frm = rows_by_table[c["from_table"]]
            to = rows_by_table[c["to_table"]]
            keys = set()
            for row in to:
                key = row.get(c["to_field"])
                if key is None:
                    raise ValidationFailed(
                        "目标主键%s.%s含null" % (c["to_table"], c["to_field"]))
                token = canonical(key)
                if token in keys:
                    raise ValidationFailed(
                        "目标主键%s.%s重复: %r" % (c["to_table"], c["to_field"], key))
                keys.add(token)
            checked = skipped = 0
            for row in frm:
                value = row.get(c["from_field"])
                if value is None:
                    skipped += 1  # null来源按SQL外键跳过
                    continue
                if canonical(value) not in keys:
                    raise ValidationFailed(
                        "外键悬空: %s.%s=%r不在%s.%s主键集合"
                        % (c["from_table"], c["from_field"], value,
                           c["to_table"], c["to_field"]))
                checked += 1
            results.append({"constraint": c, "checked": checked,
                            "null_skipped": skipped, "ok": True})
        return results

    # ---- 提交 ----

    def commit(self, spec, ticket=None):
        """事务CAS提交：幂等检查→暂存完成检查→读集重验→合并写集切换当前。"""
        with self._connect() as db:
            db.execute("BEGIN IMMEDIATE")
            try:
                # 读集快照必须已登记；写表快照必须已暂存定稿，才能提交元数据
                resolved_read = {}
                for table, marker in sorted(spec.read_set.items()):
                    resolved_read[table] = (dict(ABSENT) if is_absent(marker)
                                            else self._snapshot_digest_db(db, marker))
                resolved_write = {}
                for table, sid in sorted(spec.write_set.items()):
                    row = db.execute("SELECT digest FROM snapshots WHERE id=?",
                                     (sid,)).fetchone()
                    if row is None:
                        raise StagingError(
                            "写表快照%s未暂存定稿，不能提交元数据" % sid)
                    resolved_write[table] = row["digest"]
                resolved = {"base_release": spec.base_release,
                            "read_set": resolved_read, "write_set": resolved_write}
                spec_digest = digest(resolved)
                # 1) 操作ID幂等：同ID同内容直接返回原release（提交后失联的重试
                #    走这里，不会再造一代）；同ID不同内容拒绝。
                op = db.execute(
                    "SELECT spec_digest,release_id,committed_at FROM operations"
                    " WHERE operation_id=?", (spec.operation_id,)).fetchone()
                if op is not None:
                    if op["spec_digest"] != spec_digest:
                        self._log({"kind": "idempotency_reject",
                                   "operation_id": spec.operation_id,
                                   "spec_digest": spec_digest,
                                   "recorded_digest": op["spec_digest"]})
                        raise IdempotencyConflict(
                            "操作%s已绑定不同内容摘要" % spec.operation_id)
                    info = self.release_info(op["release_id"])
                    self._log({"kind": "replay",
                               "operation_id": spec.operation_id,
                               "release_id": op["release_id"]})
                    db.execute("COMMIT")
                    return Receipt(spec.operation_id, spec_digest, op["release_id"],
                                   info["parent"], info["mapping"], True,
                                   op["committed_at"])
                # 2) 首次提交必须持有效验证票据
                if ticket is None or ticket.spec_digest != spec_digest:
                    raise SpecError("缺少与候选规范匹配的验证票据")
                # 3) 读集CAS：与当前映射逐表核对，全局release前进不必然拒绝
                current_id, current_mapping = self.current_release()
                comparison = self.compare_read_set(resolved["read_set"],
                                                   current_mapping)
                mismatched = [c for c in comparison if not c["match"]]
                if mismatched:
                    self._log({"kind": "conflict",
                               "operation_id": spec.operation_id,
                               "spec_digest": spec_digest,
                               "current_release": current_id,
                               "comparison": comparison})
                    raise CommitConflict(
                        "读集与当前映射不一致: %s"
                        % json.dumps(mismatched, ensure_ascii=False),
                        comparison=comparison)
                self._run_hooks("before_commit",
                                {"spec": spec, "current": current_id,
                                 "catalog": self})
                # 4) 合并写集生成新release并切换当前指针（同事务）
                new_mapping = dict(current_mapping)
                new_mapping.update(spec.write_set)
                release_id = "rel_" + digest({
                    "parent": current_id, "mapping": new_mapping,
                    "operation_id": spec.operation_id,
                    "spec_digest": spec_digest})[:16]
                committed_at = now_iso()
                db.execute(
                    "INSERT INTO releases(id,parent,operation_id,spec_digest,"
                    "mapping,created_at) VALUES(?,?,?,?,?,?)",
                    (release_id, current_id, spec.operation_id, spec_digest,
                     canonical(new_mapping).decode("utf8"), committed_at))
                db.execute(
                    "INSERT INTO operations(operation_id,spec_digest,release_id,"
                    "committed_at) VALUES(?,?,?,?)",
                    (spec.operation_id, spec_digest, release_id, committed_at))
                db.execute("UPDATE current SET release_id=? WHERE id=1",
                           (release_id,))
                db.execute("COMMIT")
            except BaseException:
                try:
                    db.execute("ROLLBACK")
                except sqlite3.Error:
                    pass
                raise
        receipt = Receipt(spec.operation_id, spec_digest, release_id, current_id,
                          new_mapping, False, committed_at)
        self._log({"kind": "commit", "operation_id": spec.operation_id,
                   "spec_digest": spec_digest, "release_id": release_id,
                   "parent": current_id, "mapping": new_mapping,
                   "comparison": comparison})
        # 提交已落库、回执未发出：此处注入失败即"提交后失联"
        self._run_hooks("after_commit_before_reply",
                        {"spec": spec, "receipt": receipt, "catalog": self})
        return receipt

    def recover(self, operation_id):
        """提交结果未知时的恢复：已提交则返回原回执，否则返回None。"""
        with self._connect() as db:
            op = db.execute(
                "SELECT spec_digest,release_id,committed_at FROM operations"
                " WHERE operation_id=?", (operation_id,)).fetchone()
        if op is None:
            return None
        info = self.release_info(op["release_id"])
        return Receipt(operation_id, op["spec_digest"], op["release_id"],
                       info["parent"], info["mapping"], True, op["committed_at"])

    # ---- 读者 ----

    def open_handle(self, release_id=None):
        from .reader import ReleaseHandle
        if release_id is None:
            release_id, mapping = self.current_release()
        else:
            mapping = self.release_info(release_id)["mapping"]
        return ReleaseHandle(self, release_id, mapping)

    def closure(self, release_id):
        """release的引用闭包：表映射→快照清单→文件身份，供证据保存。"""
        info = self.release_info(release_id)
        snapshots = {}
        for table, sid in sorted(info["mapping"].items()):
            manifest = self.snapshot_manifest(sid)
            snapshots[sid] = {"digest": self.snapshot_digest(sid),
                              "table": manifest["table"], "rows": manifest["rows"],
                              "files": manifest["files"]}
        return {"release": info["id"], "parent": info["parent"],
                "operation_id": info["operation_id"],
                "mapping": info["mapping"], "snapshots": snapshots}
