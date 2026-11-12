"""保电日报：在同一release句柄上读取设备、巡检、交易三表并给出可复算总数。

历史上的故障是指针式发布只换掉了设备、巡检两张表就退出，读者拿到的
三表来自不同代，总数无法复算。这里日报只接受一个release句柄，三表必然
来自同一代，且可用该release的引用闭包独立复算核对。
"""


def daily_report(handle):
    devices = handle.rows("devices")
    inspections = handle.rows("inspections")
    trades = handle.rows("trades")
    device_ids = {d["id"] for d in devices}
    return {
        "release": handle.release_id,
        "devices": len(devices),
        "teams": sorted({d.get("team") for d in devices}),
        "inspections": len(inspections),
        "orphan_inspections": sorted(
            i["id"] for i in inspections if i.get("device") not in device_ids),
        "trades": len(trades),
        "trade_energy": sum(t.get("energy", 0) for t in trades),
    }


def recompute_from_closure(catalog, release_id):
    """不看current，只用release的引用闭包独立复算日报总数。"""
    closure = catalog.closure(release_id)
    tables = {}
    for table, sid in closure["mapping"].items():
        rows = []
        for f in closure["snapshots"][sid]["files"]:
            rows.extend(catalog.store.read_rows(f, verify=True))
        tables[table] = rows
    device_ids = {d["id"] for d in tables["devices"]}
    return {
        "release": release_id,
        "devices": len(tables["devices"]),
        "teams": sorted({d.get("team") for d in tables["devices"]}),
        "inspections": len(tables["inspections"]),
        "orphan_inspections": sorted(
            i["id"] for i in tables["inspections"]
            if i.get("device") not in device_ids),
        "trades": len(tables["trades"]),
        "trade_energy": sum(t.get("energy", 0) for t in tables["trades"]),
    }
