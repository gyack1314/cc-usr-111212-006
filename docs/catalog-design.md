# 多表发布目录设计

保电日报同时读设备、巡检、交易三张表。指针式发布只来得及换掉前两张表就退出，
读者拿到的三表来自不同代，总数无法复算。本目录把一组已生成的不可变表快照作为
同一仓库版本原子切换：读者任一时刻只能拿到完整旧代或完整新代。

## 布局

```
<repo>/catalog.sqlite3        唯一可变文件：当前release、不可变release映射、
                              快照登记、操作幂等记录
<repo>/snapshots/<id>.json    不可变快照清单（规范JSON，定稿后chmod只读）
<repo>/data/<sha[:2]>/<sha>   内容寻址数据文件（写一次，chmod只读）
<repo>/staging/<op>/<sid>/    候选暂存；中途失败只留这里，待清理，本次不做GC
```

release的完整表映射是不可变记录，当前指针与release插入在同一SQLite事务切换，
读者用单条SQL（current JOIN releases）取得句柄，不会看到半切换状态。

## 候选协议

候选声明：`operation_id`、`base_release`、完整`read_set`、`write_set`。

- `read_set`含所有实际依赖表（含约束依赖）的快照身份；首次建表填
  `{"absent": true}`；写表必须出现在read_set；目录没有删表语义，未写表沿
  当前映射保留。
- 比较时把快照ID解析为规范元数据摘要（含全部资产摘要），逐表与当前映射核对。
- 验证（不持写锁，可并发）：规范自检→read_set对当时当前映射→新表全部引用
  文件的字节/摘要/行数（JSONL实际计数，禁止空行）→固定外键。外键仅在from或
  to表被写时触发，两个端点都必须在read_set；非null来源值须落在目标主键集合
  内，目标主键唯一，null来源按SQL外键跳过。验证票据记录所见表版本。
- 提交（单个BEGIN IMMEDIATE事务）：操作ID幂等检查→写表快照已定稿检查→
  read_set对当前映射CAS→合并write_set生成新release并切换current。
  全局release前进但读集未变不拒绝；只检查写表而漏被读表也不允许——CAS
  覆盖完整read_set。验证之后另一个提交改了依赖，旧票据结论作废，须重基重验。
- 操作ID与规范摘要（基准+完整读写集+内容身份）绑定：同ID同内容重试返回原
  release，即使current后来又变；同ID不同内容拒绝。数据暂存定稿完成后才能
  提交元数据；提交后未回执的重试命中幂等记录，不会再造一代。

## 调度点与故障注入

三个可控调度点（`Catalog.add_hook`）：`after_validation`（验证后、定稿/提交前）、
`before_commit`（事务内CAS后、写入前，异常即回滚）、`after_commit_before_reply`
（提交已落库、回执未发，注入失败即"提交后失联"）。演示用它们制造确定性交错。

## 演示与证据

```sh
python -m lake_catalog.demo --materials data --out out/evidence
```

六个场景：A读集不交叉两候选并发验证先后成功；B读依赖冲突被拒（写集匹配但被读
表已变）；C验证后冲突、重基重验成功；D提交后失联重试；E读者只见完整一代，
保电日报三表总数可用引用闭包复算；F中途失败只留待清理暂存。

证据写入`out/evidence/`：`publish-evidence.jsonl`（验证/提交/冲突/重放流水）、
`read-set-comparison.json`（逐表读集比较）、`reference-closure.json`
（每release的表→快照→文件身份闭包）、`reader-observations.jsonl`、
`timeline.jsonl`（验证阶段时间重叠，证明未串行化计算）。

计算不串行化：暂存、验证、外键核对都在写锁外并发进行，只有提交事务内的CAS
被串行；已有快照与release只增不改。
