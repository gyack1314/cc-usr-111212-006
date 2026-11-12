"""可控并发调度演示：验证多表发布目录的版本协议，并保存证据。

场景：
A 两份写不同表且读集不交叉的候选并发验证、先后提交都成功（不串行化计算）；
B 读依赖冲突：写集不冲突但读集被另一个提交改动，提交CAS拒绝；
C 验证之后另一个提交改了依赖，旧验证结论作废，重基重验后成功；
D 提交后失联：同操作同内容重试返回原release，同ID不同内容拒绝；
E 读者任一时刻只拿到完整旧代或完整新代，保电日报三表总数可复算；
F 候选中途失败只留待清理暂存，不改变当前可读版本。

证据写入 <out>/：publish-evidence.jsonl、read-set-comparison.json、
reference-closure.json、reader-observations.jsonl、timeline.jsonl。
"""
import argparse
import json
import shutil
import threading
import time
from pathlib import Path

from lake_data.io import load

from .catalog import Catalog
from .core import now_iso
from .errors import CommitConflict, IdempotencyConflict
from .report import daily_report, recompute_from_closure
from .spec import CandidateSpec


class InjectedFault(Exception):
    """调度点注入的故障。"""


class Timeline:
    def __init__(self):
        self._events = []
        self._lock = threading.Lock()

    def record(self, thread, event, **extra):
        with self._lock:
            self._events.append({"t": time.perf_counter(), "thread": thread,
                                 "event": event, **extra})

    def events(self):
        with self._lock:
            return list(self._events)


class Prepared:
    def __init__(self, spec, ticket, staged):
        self.spec, self.ticket, self.staged = spec, ticket, staged


class Producer:
    """候选生产者：暂存→验证→定稿→提交。验证与暂存不持目录写锁。"""

    def __init__(self, catalog, source_dir, snapshots_by_id, timeline, name):
        self.cat = catalog
        self.source_dir = source_dir
        self.snapshots = snapshots_by_id
        self.timeline = timeline
        self.name = name

    def prepare(self, candidate):
        spec = CandidateSpec.from_manifest(candidate)
        self.timeline.record(self.name, "stage_begin", op=spec.operation_id)
        staged = {}
        for table, sid in sorted(spec.write_set.items()):
            staged[sid] = self.cat.stage_snapshot(
                spec.operation_id, self.source_dir, self.snapshots[sid])
        self.timeline.record(self.name, "validate_begin", op=spec.operation_id)
        ticket = self.cat.validate(spec, staged)
        self.timeline.record(self.name, "validate_end", op=spec.operation_id)
        return Prepared(spec, ticket, staged)

    def finalize(self, prep):
        for sid in sorted(prep.staged):
            self.cat.finalize(prep.staged[sid])
        self.timeline.record(self.name, "staged_finalized", op=prep.spec.operation_id)

    def commit(self, prep):
        self.timeline.record(self.name, "commit_begin", op=prep.spec.operation_id)
        receipt = self.cat.commit(prep.spec, prep.ticket)
        self.timeline.record(self.name, "commit_end", op=prep.spec.operation_id,
                             release=receipt.release_id, replayed=receipt.replayed)
        return receipt

    def run(self, candidate):
        prep = self.prepare(candidate)
        self.finalize(prep)
        return self.commit(prep)


def _load_material(path):
    manifest, source_digest = load(path)
    return manifest, source_digest, Path(path).parent


def _fresh_repo(repos_dir, name):
    path = Path(repos_dir) / name
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True)
    return path


def _scenario_result(name, cat, notes):
    return {"scenario": name, "releases": cat.releases(),
            "audit": cat.audit_log(), "notes": notes}


def scenario_a(material, repos_dir, timeline):
    """A: 两个生产者写不同表、读集不交叉，并发验证后先后提交都成功。"""
    manifest, _, src = _load_material(material)
    cat = Catalog.create(_fresh_repo(repos_dir, "a-disjoint"))
    cat.bootstrap(material)
    barrier = threading.Barrier(2)

    def sync(point):
        def hook(ctx):
            timeline.record("hook", point, op=ctx["spec"].operation_id)
            if point == "after_validation":
                barrier.wait(timeout=30)  # 两个候选都完成验证才放行提交
        return hook

    for point in ("after_validation", "before_commit", "after_commit_before_reply"):
        cat.add_hook(point, sync(point))
    snaps = {s["id"]: s for s in manifest["table_snapshots"]}
    c1, c2 = manifest["candidates"]
    results, errors = {}, {}

    def work(name, candidate):
        try:
            results[candidate["operation_id"]] = Producer(
                cat, src, snaps, timeline, name).run(candidate)
        except BaseException as e:  # noqa: BLE001 - 演示需要记录任何结果
            errors[candidate["operation_id"]] = e

    threads = [threading.Thread(target=work, args=("producer-devices", c1),
                                name="producer-devices"),
               threading.Thread(target=work, args=("producer-trades", c2),
                                name="producer-trades")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, {k: repr(v) for k, v in errors.items()}
    r1, r2 = results["c1"], results["c2"]
    assert not r1.replayed and not r2.replayed
    # 后提交者的父代是另一候选的release，而它的base_release仍是r0：
    # 全局release前进但读集未变，不一律拒绝。
    chain = {r1.release_id, r2.release_id}
    assert len(chain) == 2
    _, mapping = cat.current_release()
    assert mapping == {"devices": "dv2", "inspections": "iv1", "trades": "tv2"}
    notes = ["两个候选base_release都是r0，第二个提交时全局release已前进，"
             "但读集未变，CAS通过", "after_validation屏障证明两段验证并发重叠"]
    return _scenario_result("A-读集不交叉先后成功", cat, notes)


def scenario_b(material, repos_dir, timeline):
    """B: 读依赖冲突——写集不冲突，但被读表已变，提交CAS拒绝。"""
    manifest, _, src = _load_material(material)
    cat = Catalog.create(_fresh_repo(repos_dir, "b-read-conflict"))
    cat.bootstrap(material)
    snaps = {s["id"]: s for s in manifest["table_snapshots"]}
    c1, c2 = manifest["candidates"]  # c2的read_set含devices:dv1
    victim_validated = threading.Event()
    winner_committed = threading.Event()

    def hold_victim(ctx):
        if ctx["spec"].operation_id == c2["operation_id"]:
            victim_validated.set()
            assert winner_committed.wait(timeout=30)

    cat.add_hook("after_validation", hold_victim)
    outcome = {}

    def victim():
        producer = Producer(cat, src, snaps, timeline, "victim-c2")
        prep = producer.prepare(c2)
        producer.finalize(prep)
        try:
            producer.commit(prep)
            outcome["error"] = AssertionError("读依赖冲突未被拒绝")
        except CommitConflict as e:
            outcome["conflict"] = e

    t = threading.Thread(target=victim, name="victim-c2")
    t.start()
    assert victim_validated.wait(timeout=30)
    winner = Producer(cat, src, snaps, timeline, "winner-c1")
    winner_receipt = winner.run(c1)  # 先提交devices: dv1->dv2
    winner_committed.set()
    t.join()
    assert "conflict" in outcome, outcome.get("error")
    comparison = outcome["conflict"].comparison
    by_table = {c["table"]: c for c in comparison}
    # 写表trades读集仍匹配，被读表devices不匹配：只检查写表会漏掉它
    assert by_table["trades"]["match"] and not by_table["devices"]["match"]
    _, mapping = cat.current_release()
    assert mapping["devices"] == "dv2" and mapping["trades"] == "tv1"
    notes = ["c2写trades（读集仍匹配），但读集里的devices已被c1改为dv2，CAS拒绝",
             "冲突比较显示trades匹配、devices不匹配，证明检查覆盖被读表"]
    result = _scenario_result("B-读依赖冲突被拒", cat, notes)
    result["winner_release"] = winner_receipt.release_id
    return result


def scenario_c(material, repos_dir, timeline):
    """C: 验证之后另一个提交改了依赖，旧结论作废；重基重验后成功。"""
    manifest, _, src = _load_material(material)
    cat = Catalog.create(_fresh_repo(repos_dir, "c-stale-ticket"))
    cat.bootstrap(material)
    snaps = {s["id"]: s for s in manifest["table_snapshots"]}
    c1, c2 = manifest["candidates"]
    victim_validated = threading.Event()
    winner_committed = threading.Event()

    def hold_victim(ctx):
        if ctx["spec"].operation_id == c2["operation_id"]:
            victim_validated.set()
            assert winner_committed.wait(timeout=30)

    cat.add_hook("after_validation", hold_victim)
    outcome = {}
    winner = {}

    def victim():
        producer = Producer(cat, src, snaps, timeline, "victim-c2")
        prep = producer.prepare(c2)  # 验证时所见为r0
        producer.finalize(prep)
        try:
            producer.commit(prep)
        except CommitConflict as e:
            outcome["stale_conflict"] = e
            # 重基到新基准：devices读集更新为当前版本dv2，重新验证后再提交
            rebased = dict(c2)
            rebased["base_release"] = winner["release_id"]
            rebased["read_set"] = dict(c2["read_set"], devices="dv2")
            prep2 = producer.prepare(rebased)
            producer.finalize(prep2)
            outcome["rebased_receipt"] = producer.commit(prep2)

    t = threading.Thread(target=victim, name="victim-c2")
    t.start()
    assert victim_validated.wait(timeout=30)
    winner["release_id"] = Producer(
        cat, src, snaps, timeline, "winner-c1").run(c1).release_id
    winner_committed.set()
    t.join()
    assert "stale_conflict" in outcome
    receipt = outcome["rebased_receipt"]
    assert not receipt.replayed
    _, mapping = cat.current_release()
    assert mapping == {"devices": "dv2", "inspections": "iv1", "trades": "tv2"}
    notes = ["c2在r0上验证，c1抢先提交后c2的旧票据结论作废（CAS拒绝）",
             "c2重基到新基准并把devices读集改为dv2，重新验证后提交成功"]
    return _scenario_result("C-验证后冲突与重基", cat, notes)


def scenario_d(material, repos_dir, timeline):
    """D: 提交后失联——同操作同内容重试返回原release，同ID不同内容拒绝。"""
    manifest, _, src = _load_material(material)
    cat = Catalog.create(_fresh_repo(repos_dir, "d-lost-reply"))
    cat.bootstrap(material)
    snaps = {s["id"]: s for s in manifest["table_snapshots"]}
    c1, c2, c1_retry = manifest["candidates"]
    assert c1 == c1_retry  # 同操作同内容的重试
    fault = {"armed": True}

    def lose_reply(ctx):
        if ctx["spec"].operation_id == "c1" and fault["armed"]:
            fault["armed"] = False
            timeline.record("hook", "reply_lost", op="c1")
            raise InjectedFault("提交后、回执前失联")

    cat.add_hook("after_commit_before_reply", lose_reply)
    producer = Producer(cat, src, snaps, timeline, "producer-c1")
    prep = producer.prepare(c1)
    producer.finalize(prep)
    lost = None
    try:
        producer.commit(prep)
    except InjectedFault:
        lost = True
    assert lost, "应注入提交后失联"
    # 恢复：操作已提交，重试返回原release，不能再造一代
    recovered = cat.recover("c1")
    assert recovered is not None
    before = len(cat.releases())
    replay = cat.commit(prep.spec)  # 客户端重试（甚至无需重新验证）
    assert replay.replayed and replay.release_id == recovered.release_id
    assert len(cat.releases()) == before
    # 另一候选前进current
    r2 = Producer(cat, src, snaps, timeline, "producer-c2").run(c2)
    # 同操作同内容在current前进后重试，仍返回原release
    replay2 = cat.commit(CandidateSpec.from_manifest(c1_retry))
    assert replay2.replayed and replay2.release_id == recovered.release_id
    assert replay2.release_id != r2.release_id
    # 同ID不同内容拒绝
    altered = CandidateSpec("c1", "r0",
                            {"devices": "dv1", "inspections": "iv1"},
                            {"devices": "dv1"})
    rejected = None
    try:
        cat.commit(altered)
    except IdempotencyConflict:
        rejected = True
    assert rejected
    notes = ["c1提交后回执前失联，恢复与重试都返回原release且release总数不变",
             "current前进后同操作同内容重试仍返回原release",
             "同操作ID不同内容摘要被拒绝"]
    result = _scenario_result("D-提交后失联重试", cat, notes)
    result["recovered_release"] = recovered.release_id
    return result


def scenario_e(material, repos_dir, timeline):
    """E: 读者任一时刻只拿到完整旧代或完整新代；日报总数可复算。"""
    manifest, _, src = _load_material(material)
    cat = Catalog.create(_fresh_repo(repos_dir, "e-reader"))
    cat.bootstrap(material)
    snaps = {s["id"]: s for s in manifest["table_snapshots"]}
    c1, c2 = manifest["candidates"]
    observations = []
    stop = threading.Event()

    def polling_reader():
        while not stop.is_set():
            with cat.open_handle() as h:
                report = daily_report(h)
                recomputed = recompute_from_closure(cat, h.release_id)
                observations.append({
                    "at": now_iso(), "release": h.release_id,
                    "report": report, "recomputed": recomputed,
                    "consistent": report == recomputed})
            time.sleep(0.002)

    poller = threading.Thread(target=polling_reader, name="polling-reader")
    # 老句柄在提交前取得
    old_handle = cat.open_handle()
    old_release = old_handle.release_id
    poller.start()
    r1 = Producer(cat, src, snaps, timeline, "producer-c1").run(c1)
    time.sleep(0.03)  # 给轮询读者留出观察中间代的窗口
    r2 = Producer(cat, src, snaps, timeline, "producer-c2").run(c2)
    time.sleep(0.03)
    stop.set()
    poller.join()
    # 老句柄仍读完整旧代，不为每张表重新看current
    old_report = daily_report(old_handle)
    assert old_report == recompute_from_closure(cat, old_release)
    assert old_report["release"] == old_release
    # 新句柄拿完整新代
    with cat.open_handle() as h:
        new_report = daily_report(h)
        assert h.release_id == r2.release_id
        assert new_report == recompute_from_closure(cat, r2.release_id)
        assert new_report["trade_energy"] == 120  # tv2
    # 关闭句柄不删除任何文件
    closure_files = {f["sha256"] for rel in cat.releases()
                     for s in cat.closure(rel["id"])["snapshots"].values()
                     for f in s["files"]}
    old_handle.close()
    for sha in closure_files:
        assert cat.store.data_path(sha).is_file()
    assert observations and all(o["consistent"] for o in observations)
    seen = {o["release"] for o in observations}
    notes = ["轮询读者%d次观察全部自洽（只见完整某一代）" % len(observations),
             "观察到的release: %s" % sorted(seen),
             "提交前取得的老句柄仍读完整旧代，关闭句柄不删除任何文件"]
    result = _scenario_result("E-读者只见完整一代", cat, notes)
    result["observations"] = observations
    result["old_handle_report"] = old_report
    result["new_handle_report"] = new_report
    return result


def scenario_f(material, repos_dir, timeline):
    """F: 候选中途失败只留待清理暂存，不改变当前可读版本。"""
    manifest, _, src = _load_material(material)
    cat = Catalog.create(_fresh_repo(repos_dir, "f-mid-failure"))
    cat.bootstrap(material)
    snaps = {s["id"]: s for s in manifest["table_snapshots"]}
    c1, _ = manifest["candidates"]
    fault = {"armed": True}

    def fail_before_commit(ctx):
        if fault["armed"]:
            fault["armed"] = False
            raise InjectedFault("提交前注入失败")

    cat.add_hook("before_commit", fail_before_commit)
    producer = Producer(cat, src, snaps, timeline, "producer-c1")
    prep = producer.prepare(c1)
    producer.finalize(prep)
    failed = None
    try:
        producer.commit(prep)
    except InjectedFault:
        failed = True
    assert failed
    current_id, mapping = cat.current_release()
    assert current_id == "r0" and mapping["devices"] == "dv1"
    assert [r["id"] for r in cat.releases()] == ["r0"]
    # 失败候选的遗留：已定稿但未被任何release引用的数据文件（待清理，本次不GC）
    referenced = {f["sha256"] for r in cat.releases()
                  for s in cat.closure(r["id"])["snapshots"].values()
                  for f in s["files"]}
    garbage = cat.store.unreferenced_data(referenced)
    assert garbage, "应存在失败候选遗留的未引用数据文件"
    # 故障解除后同一候选可重新提交成功
    receipt = producer.commit(prep)
    assert not receipt.replayed
    _, mapping = cat.current_release()
    assert mapping["devices"] == "dv2"
    notes = ["before_commit注入失败：当前可读版本保持r0，无新release",
             "遗留未引用数据文件%d个（待清理暂存，本次不做GC）" % len(garbage),
             "故障解除后同候选重新提交成功"]
    result = _scenario_result("F-中途失败不留痕于当前版本", cat, notes)
    result["garbage_files"] = garbage
    result["staging_leftovers"] = cat.store.staging_leftovers()
    return result


def run_all(materials, out_dir):
    out_dir = Path(out_dir)
    repos_dir = out_dir / "repos"
    if repos_dir.exists():
        shutil.rmtree(repos_dir)
    repos_dir.mkdir(parents=True, exist_ok=True)
    out_dir.mkdir(parents=True, exist_ok=True)
    timeline = Timeline()
    scenarios = [
        scenario_a(materials["normal"], repos_dir, timeline),
        scenario_b(materials["read-dependency-conflict"], repos_dir, timeline),
        scenario_c(materials["read-dependency-conflict"], repos_dir, timeline),
        scenario_d(materials["same-operation-retry"], repos_dir, timeline),
        scenario_e(materials["normal"], repos_dir, timeline),
        scenario_f(materials["normal"], repos_dir, timeline),
    ]
    # 证据一：发布证据（每个场景的审计流水）
    with open(out_dir / "publish-evidence.jsonl", "w", encoding="utf8") as f:
        for sc in scenarios:
            for entry in sc["audit"]:
                f.write(json.dumps({"scenario": sc["scenario"], **entry},
                                   ensure_ascii=False) + "\n")
    # 证据二：读集比较（验证与提交CAS的逐表对照）
    comparisons = {}
    for sc in scenarios:
        rows = []
        for entry in sc["audit"]:
            if "comparison" in entry:
                rows.append({"kind": entry["kind"],
                             "operation_id": entry.get("operation_id"),
                             "current_release": entry.get("current_release"),
                             "comparison": entry["comparison"]})
        comparisons[sc["scenario"]] = rows
    (out_dir / "read-set-comparison.json").write_text(
        json.dumps(comparisons, ensure_ascii=False, indent=2), encoding="utf8")
    # 证据三：引用闭包（每个场景每个release的表→快照→文件身份）
    closures = {}
    for sc in scenarios:
        cat_dir = repos_dir / _repo_dirname(sc["scenario"])
        cat = Catalog.open(cat_dir)
        closures[sc["scenario"]] = {r["id"]: cat.closure(r["id"])
                                    for r in cat.releases()}
    (out_dir / "reference-closure.json").write_text(
        json.dumps(closures, ensure_ascii=False, indent=2), encoding="utf8")
    # 证据四：读者观察
    with open(out_dir / "reader-observations.jsonl", "w", encoding="utf8") as f:
        for sc in scenarios:
            for obs in sc.get("observations", []):
                f.write(json.dumps({"scenario": sc["scenario"], **obs},
                                   ensure_ascii=False) + "\n")
    # 证据五：并发时间线（验证阶段重叠，证明未串行化计算）
    events = timeline.events()
    if events:
        t0 = events[0]["t"]
        with open(out_dir / "timeline.jsonl", "w", encoding="utf8") as f:
            for e in events:
                f.write(json.dumps({"t_ms": round((e["t"] - t0) * 1000, 3),
                                    **{k: v for k, v in e.items() if k != "t"}},
                                   ensure_ascii=False) + "\n")
    return scenarios


def _repo_dirname(scenario_name):
    return {"A-读集不交叉先后成功": "a-disjoint",
            "B-读依赖冲突被拒": "b-read-conflict",
            "C-验证后冲突与重基": "c-stale-ticket",
            "D-提交后失联重试": "d-lost-reply",
            "E-读者只见完整一代": "e-reader",
            "F-中途失败不留痕于当前版本": "f-mid-failure"}[scenario_name]


def main(argv=None):
    parser = argparse.ArgumentParser(description="多表发布目录并发演示")
    parser.add_argument("--materials", default="data", help="冻结资料目录")
    parser.add_argument("--out", default="out/evidence", help="证据输出目录")
    args = parser.parse_args(argv)
    materials = {name: str(Path(args.materials) / ("%s.json" % name))
                 for name in ("normal", "read-dependency-conflict",
                              "same-operation-retry")}
    scenarios = run_all(materials, args.out)
    print("证据目录: %s" % args.out)
    for sc in scenarios:
        releases = " -> ".join(r["id"] for r in sc["releases"])
        print("[%s] release链: %s" % (sc["scenario"], releases))
        for note in sc["notes"]:
            print("    - %s" % note)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
