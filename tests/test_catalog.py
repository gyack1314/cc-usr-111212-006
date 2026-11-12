"""多表发布目录协议测试：读集CAS、约束核对、幂等、暂存、读者句柄。"""
import hashlib
import json
import threading
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from lake_catalog import (CandidateSpec, Catalog, CommitConflict,
                          IdempotencyConflict, SpecError, StagingError,
                          UnknownSnapshot, ValidationConflict, ValidationFailed)
from lake_catalog.report import daily_report, recompute_from_closure

DATA = Path(__file__).resolve().parent.parent / "data"
NORMAL = DATA / "normal.json"
CONFLICT = DATA / "read-dependency-conflict.json"
RETRY = DATA / "same-operation-retry.json"


def load_manifest(path):
    return json.loads(Path(path).read_text(encoding="utf8"))


def make_snapshot_def(source_dir, sid, table, rows):
    """在源目录造一个JSONL快照并给出清单声明。"""
    raw = b"".join((json.dumps(r, ensure_ascii=False) + "\n").encode("utf8")
                   for r in rows)
    name = "%s.jsonl" % sid
    (Path(source_dir) / name).write_bytes(raw)
    return {"id": sid, "table": table, "rows": len(rows),
            "files": [{"path": name, "bytes": len(raw),
                       "sha256": hashlib.sha256(raw).hexdigest(),
                       "rows": len(rows)}]}


class RepoCase(unittest.TestCase):
    def setUp(self):
        self.tmp = TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.repo = Path(self.tmp.name) / "repo"
        # 每个测试独立的源目录：复制冻结资料，新造快照也放这里
        self.incoming = Path(self.tmp.name) / "incoming"
        self.incoming.mkdir()
        for p in DATA.glob("*.jsonl"):
            (self.incoming / p.name).write_bytes(p.read_bytes())
        self.manifest = load_manifest(NORMAL)
        self.snaps = {s["id"]: s for s in self.manifest["table_snapshots"]}
        self.cat = Catalog.create(self.repo)
        self.cat.bootstrap(NORMAL)

    def candidate(self, raw):
        return CandidateSpec.from_manifest(raw)

    def prepare(self, spec):
        staged = {}
        for table, sid in sorted(spec.write_set.items()):
            staged[sid] = self.cat.stage_snapshot(
                spec.operation_id, self.incoming, self.snaps[sid])
        ticket = self.cat.validate(spec, staged)
        return spec, ticket, staged

    def run_candidate(self, raw):
        spec, ticket, staged = self.prepare(self.candidate(raw))
        for sid in sorted(staged):
            self.cat.finalize(staged[sid])
        return self.cat.commit(spec, ticket)

    def add_snapshot(self, sid, table, rows):
        snap = make_snapshot_def(self.incoming, sid, table, rows)
        self.snaps[sid] = snap
        return snap


class BootstrapTests(RepoCase):
    def test_bootstrap_initial_release(self):
        current_id, mapping = self.cat.current_release()
        self.assertEqual(current_id, "r0")
        self.assertEqual(mapping, {"devices": "dv1", "inspections": "iv1",
                                   "trades": "tv1"})
        closure = self.cat.closure("r0")
        self.assertEqual(set(closure["snapshots"]), {"dv1", "iv1", "tv1"})
        with self.cat.open_handle() as h:
            self.assertEqual(h.rows("devices"), [{"id": "d1", "team": "north"}])

    def test_bootstrap_rejects_invalid_config(self):
        cat = Catalog.create(Path(self.tmp.name) / "repo2")
        with self.assertRaises(SpecError):
            cat.bootstrap(DATA / "invalid-config.json")

    def test_data_files_readonly(self):
        import stat
        for p in self.cat.store.data_dir.rglob("*"):
            if p.is_file():
                self.assertFalse(stat.S_IMODE(p.stat().st_mode) & 0o222, p)


class CommitProtocolTests(RepoCase):
    def test_disjoint_candidates_both_commit(self):
        c1, c2 = self.manifest["candidates"]
        r1 = self.run_candidate(c1)
        r2 = self.run_candidate(c2)
        self.assertNotEqual(r1.release_id, r2.release_id)
        self.assertEqual(r2.parent, r1.release_id)  # 全局release已前进
        self.assertEqual(r2.mapping, {"devices": "dv2", "inspections": "iv1",
                                      "trades": "tv2"})
        # 未写表沿当前映射保留，目录不允许隐式删表
        self.assertEqual(set(r2.mapping), {"devices", "inspections", "trades"})

    def test_read_dependency_conflict_at_commit(self):
        manifest = load_manifest(CONFLICT)
        c1, c2 = manifest["candidates"]
        # 受害者先在r0上验证（所见devices=dv1）
        spec, ticket, staged = self.prepare(self.candidate(c2))
        for sid in sorted(staged):
            self.cat.finalize(staged[sid])
        # 另一个提交改动了被读表devices
        self.run_candidate(c1)
        # 写表trades读集仍匹配，但被读表devices已变：CAS必须拒绝
        with self.assertRaises(CommitConflict) as ctx:
            self.cat.commit(spec, ticket)
        by_table = {c["table"]: c for c in ctx.exception.comparison}
        self.assertTrue(by_table["trades"]["match"])
        self.assertFalse(by_table["devices"]["match"])

    def test_validation_conflict_when_already_moved(self):
        manifest = load_manifest(CONFLICT)
        c1, c2 = manifest["candidates"]
        self.run_candidate(c1)
        with self.assertRaises(ValidationConflict):
            self.prepare(self.candidate(c2))

    def test_stale_ticket_then_revalidate(self):
        manifest = load_manifest(CONFLICT)
        c1, c2 = manifest["candidates"]
        spec, ticket, staged = self.prepare(self.candidate(c2))
        for sid in sorted(staged):
            self.cat.finalize(staged[sid])
        r1 = self.run_candidate(c1)
        with self.assertRaises(CommitConflict):
            self.cat.commit(spec, ticket)
        # 重基到新基准（读集改为当前版本）后重新验证，再提交成功
        rebased = dict(c2)
        rebased["base_release"] = r1.release_id
        rebased["read_set"] = dict(c2["read_set"], devices="dv2")
        receipt = self.run_candidate(rebased)
        self.assertEqual(receipt.mapping["trades"], "tv2")

    def test_write_table_must_be_in_read_set(self):
        with self.assertRaises(SpecError):
            CandidateSpec("op", "r0", {"inspections": "iv1"},
                          {"devices": "dv2"})

    def test_first_create_requires_absent_and_works(self):
        self.add_snapshot("al1", "alerts", [{"id": "a1", "device": "d1"}])
        # 不写absent标记：写表必须出现在read_set
        with self.assertRaises(SpecError):
            CandidateSpec("op", "r0", {}, {"alerts": "al1"})
        # absent标记建首表
        spec = CandidateSpec("op-a", "r0", {"alerts": {"absent": True}},
                             {"alerts": "al1"})
        _, ticket, staged = self.prepare(spec)
        for sid in sorted(staged):
            self.cat.finalize(staged[sid])
        receipt = self.cat.commit(spec, ticket)
        self.assertEqual(receipt.mapping["alerts"], "al1")
        # absent标记但表已存在：验证即拒绝
        spec2 = CandidateSpec("op-b", receipt.release_id,
                              {"alerts": {"absent": True}}, {"alerts": "al1"})
        with self.assertRaises(SpecError):
            self.cat.validate(spec2, staged)

    def test_absent_cas_conflict_between_two_creators(self):
        self.add_snapshot("ax1", "alerts", [{"id": "a1"}])
        self.add_snapshot("ax2", "alerts", [{"id": "a2"}])
        spec1, ticket1, staged1 = self.prepare(CandidateSpec(
            "op-1", "r0", {"alerts": {"absent": True}}, {"alerts": "ax1"}))
        spec2, ticket2, staged2 = self.prepare(CandidateSpec(
            "op-2", "r0", {"alerts": {"absent": True}}, {"alerts": "ax2"}))
        for staged in (staged1, staged2):
            for sid in sorted(staged):
                self.cat.finalize(staged[sid])
        self.cat.commit(spec1, ticket1)
        with self.assertRaises(CommitConflict):
            self.cat.commit(spec2, ticket2)

    def test_empty_write_set_rejected(self):
        with self.assertRaises(SpecError):
            CandidateSpec("op", "r0", {"devices": "dv1"}, {})


class ConstraintTests(RepoCase):
    def test_constraint_endpoint_must_be_in_read_set(self):
        self.add_snapshot("iv2", "inspections", [{"id": "i2", "device": "d1"}])
        # 写inspections触发外键，devices必须列入read_set
        spec = CandidateSpec("op", "r0", {"inspections": "iv1"},
                             {"inspections": "iv2"})
        with self.assertRaises(SpecError):
            self.prepare(spec)

    def test_fk_dangling_rejected(self):
        self.add_snapshot("iv2", "inspections", [{"id": "i2", "device": "ghost"}])
        spec = CandidateSpec("op", "r0",
                             {"inspections": "iv1", "devices": "dv1"},
                             {"inspections": "iv2"})
        with self.assertRaises(ValidationFailed):
            self.prepare(spec)

    def test_fk_triggered_by_target_table_write(self):
        # 写devices（to表）也要触发外键核对：dv3删掉d1会让iv1悬空
        self.add_snapshot("dv3", "devices", [{"id": "d9", "team": "west"}])
        spec = CandidateSpec("op", "r0",
                             {"devices": "dv1", "inspections": "iv1"},
                             {"devices": "dv3"})
        with self.assertRaises(ValidationFailed):
            self.prepare(spec)

    def test_null_fk_source_skipped(self):
        self.add_snapshot("iv2", "inspections",
                          [{"id": "i2", "device": None},
                           {"id": "i3", "device": "d1"}])
        spec = CandidateSpec("op", "r0",
                             {"inspections": "iv1", "devices": "dv1"},
                             {"inspections": "iv2"})
        _, ticket, _ = self.prepare(spec)
        fk = ticket.checks["constraints"][0]
        self.assertEqual(fk["null_skipped"], 1)
        self.assertEqual(fk["checked"], 1)

    def test_target_pk_must_be_unique(self):
        self.add_snapshot("dv3", "devices",
                          [{"id": "d1", "team": "a"}, {"id": "d1", "team": "b"}])
        spec = CandidateSpec("op", "r0",
                             {"devices": "dv1", "inspections": "iv1"},
                             {"devices": "dv3"})
        with self.assertRaises(ValidationFailed):
            self.prepare(spec)


class StagingTests(RepoCase):
    def test_declared_identity_mismatch(self):
        bad = dict(self.snaps["dv2"])
        bad["files"] = [dict(self.snaps["dv2"]["files"][0], sha256="0" * 64)]
        with self.assertRaises(StagingError):
            self.cat.stage_snapshot("op", self.incoming, bad)

    def test_blank_line_rejected(self):
        raw = b'{"id":"d1"}\n\n{"id":"d2"}\n'
        (self.incoming / "bad.jsonl").write_bytes(raw)
        snap = {"id": "bad1", "table": "devices", "rows": 2,
                "files": [{"path": "bad.jsonl", "bytes": len(raw),
                           "sha256": hashlib.sha256(raw).hexdigest(),
                           "rows": 2}]}
        with self.assertRaises(StagingError):
            self.cat.stage_snapshot("op", self.incoming, snap)

    def test_commit_requires_finalized_staging(self):
        c1 = self.manifest["candidates"][0]
        spec, ticket, staged = self.prepare(self.candidate(c1))
        # 不定稿直接提交元数据：拒绝
        with self.assertRaises(StagingError):
            self.cat.commit(spec, ticket)

    def test_snapshot_id_content_conflict(self):
        dv1b = self.add_snapshot("dv1", "devices", [{"id": "d7", "team": "x"}])
        staged = self.cat.stage_snapshot("op", self.incoming, dv1b)
        with self.assertRaises(StagingError):
            self.cat.finalize(staged)  # dv1已在引导时登记不同内容

    def test_mid_failure_keeps_current_and_leaves_garbage(self):
        c1 = self.manifest["candidates"][0]
        spec, ticket, staged = self.prepare(self.candidate(c1))
        for sid in sorted(staged):
            self.cat.finalize(staged[sid])

        def boom(ctx):
            raise RuntimeError("注入失败")

        self.cat.add_hook("before_commit", boom)
        with self.assertRaises(RuntimeError):
            self.cat.commit(spec, ticket)
        current_id, mapping = self.cat.current_release()
        self.assertEqual((current_id, mapping["devices"]), ("r0", "dv1"))
        self.assertEqual(len(self.cat.releases()), 1)
        referenced = {f["sha256"]
                      for s in self.cat.closure("r0")["snapshots"].values()
                      for f in s["files"]}
        self.assertTrue(self.cat.store.unreferenced_data(referenced))


class IdempotencyTests(RepoCase):
    def test_same_operation_retry_returns_original(self):
        manifest = load_manifest(RETRY)
        c1, c2, c1_retry = manifest["candidates"]
        r1 = self.run_candidate(c1)
        count = len(self.cat.releases())
        replay = self.cat.commit(self.candidate(c1_retry))
        self.assertTrue(replay.replayed)
        self.assertEqual(replay.release_id, r1.release_id)
        self.assertEqual(len(self.cat.releases()), count)  # 未再造一代
        # current前进后重试仍返回原release
        r2 = self.run_candidate(c2)
        replay2 = self.cat.commit(self.candidate(c1_retry))
        self.assertEqual(replay2.release_id, r1.release_id)
        self.assertNotEqual(replay2.release_id, r2.release_id)

    def test_same_operation_different_content_rejected(self):
        c1 = self.manifest["candidates"][0]
        self.run_candidate(c1)
        altered = CandidateSpec("c1", "r0",
                                {"devices": "dv1", "inspections": "iv1"},
                                {"devices": "dv1"})
        with self.assertRaises(IdempotencyConflict):
            self.cat.commit(altered)

    def test_commit_after_lost_reply(self):
        c1 = self.manifest["candidates"][0]
        spec, ticket, staged = self.prepare(self.candidate(c1))
        for sid in sorted(staged):
            self.cat.finalize(staged[sid])
        state = {"armed": True}

        def lose_reply(ctx):
            if state["armed"]:
                state["armed"] = False
                raise RuntimeError("提交后回执前失联")

        self.cat.add_hook("after_commit_before_reply", lose_reply)
        with self.assertRaises(RuntimeError):
            self.cat.commit(spec, ticket)
        recovered = self.cat.recover("c1")
        self.assertIsNotNone(recovered)
        count = len(self.cat.releases())
        replay = self.cat.commit(spec)
        self.assertTrue(replay.replayed)
        self.assertEqual(replay.release_id, recovered.release_id)
        self.assertEqual(len(self.cat.releases()), count)


class ReaderTests(RepoCase):
    def test_handle_pinned_to_one_release(self):
        c1, c2 = self.manifest["candidates"]
        old = self.cat.open_handle()
        r1 = self.run_candidate(c1)
        r2 = self.run_candidate(c2)
        # 老句柄仍读完整旧代，不为每张表重新看current
        self.assertEqual(old.release_id, "r0")
        self.assertEqual(old.rows("devices")[0]["team"], "north")
        self.assertEqual(daily_report(old),
                         recompute_from_closure(self.cat, "r0"))
        with self.cat.open_handle() as h:
            self.assertEqual(h.release_id, r2.release_id)
            report = daily_report(h)
            self.assertEqual(report["trade_energy"], 120)
            self.assertEqual(report["teams"], ["south"])
            self.assertEqual(report,
                             recompute_from_closure(self.cat, r2.release_id))
        # 关闭句柄不删除任何文件
        old.close()
        for rel in self.cat.releases():
            for snap in self.cat.closure(rel["id"])["snapshots"].values():
                for f in snap["files"]:
                    self.assertTrue(self.cat.store.data_path(f["sha256"]).is_file())
        with self.assertRaises(Exception):
            old.rows("devices")

    def test_concurrent_disjoint_producers_and_readers(self):
        c1, c2 = self.manifest["candidates"]
        barrier = threading.Barrier(2)

        def sync(ctx):
            barrier.wait(timeout=30)

        self.cat.add_hook("after_validation", sync)
        receipts, errors = {}, {}

        def work(candidate):
            try:
                receipts[candidate["operation_id"]] = self.run_candidate(candidate)
            except BaseException as e:  # noqa: BLE001
                errors[candidate["operation_id"]] = e

        threads = [threading.Thread(target=work, args=(c1,)),
                   threading.Thread(target=work, args=(c2,))]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertFalse(errors)
        self.assertEqual(len(receipts), 2)
        _, mapping = self.cat.current_release()
        self.assertEqual(mapping, {"devices": "dv2", "inspections": "iv1",
                                   "trades": "tv2"})


if __name__ == "__main__":
    unittest.main()
