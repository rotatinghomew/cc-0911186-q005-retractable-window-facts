# 设备遥测窗口聚合中的迟到更正与撤回归因

设备遥测事件的到达/发生双时间、窗口事实与撤回状态基线。

仓库内的实现使用 Python 3.11 标准库和 SQLite，数据只写入命令行指定的本地数据库及导出目录。`lakeapp.storage` 保存原始接收、业务状态和版本清单；`lakeapp.contract` 负责输入约定；`lakeapp.ingest` 接收来源批次；`lakeapp.derive` 生成当前基线支持的事实；`lakeapp.dws` 按事件时间生成 15 分钟窗口汇总并处理迟到更正与撤回；`lakeapp.publish` 读取并写出可追溯版本；`lakeapp.query` 提供运维复核视图；`lakeapp.cli` 提供本地操作入口。

## 运行

在 Python 3.11 环境中执行 `python3 -m unittest discover -s tests -v` 运行测试，执行 `python3 -m compileall -q lakeapp tests` 检查构建和语法。示例数据是本地合成记录，不依赖外部服务或联网。

## 现有契约

输入批次包含来源名、批次标识、游标和记录数组。原始记录以 JSON 保存，重复批次不会再次产生事实；业务键相同但内容不同的记录会保留冲突信息。事实查询按业务日期和版本清单读取，清单中的文件摘要与行数用于发现导出内容被替换。数据库初始化由运行入口完成，已有数据不会因重新打开而丢失。

## 目录职责

`lakeapp/storage.py` 负责事务和表结构，`lakeapp/contract.py` 负责记录约束，`lakeapp/ingest.py` 负责接收和来源进度，`lakeapp/derive.py` 负责从原始记录生成事实，`lakeapp/dws.py` 负责窗口聚合、迟到更正、撤回留痕与快照状态，`lakeapp/publish.py` 负责版本清单与回读，`lakeapp/query.py` 负责复核查询，`lakeapp/cli.py` 负责把这些业务操作组合给本地使用者。测试数据覆盖正常接收、重复批次和版本读取。

## DWS 窗口更正语义

窗口按事件发生时间 `occurred_at` 归属 15 分钟桶，汇总同时保留事件时间范围（`event_min/event_max`）与到达时间范围（`arrival_min/arrival_max`），网关重连补送的旧读数不会按到达日期计入错误窗口。

- 更正范围：默认 15 分钟，即批次到达时间不晚于“窗口起点 + 更正分钟数”时，迟到读数与撤回才能改写窗口，范围可在合并时通过 `correction_minutes` 调整。
- 超出更正范围、撤回目标不存在或窗口归属冲突的事件进入 `unmerged_events` 待归并区，附原因与到达依据，不静默改写已发布历史。
- 撤回是软删除：明细行保留并标记 `retracted`，事件不会表现成从未发生；已发布窗口被改写时聚合版本递增，并在 `window_revisions` 记录修订缘由、前后版本与受影响读数键，旧清单版本持续可读。
- 下游告警快照登记在 `downstream_snapshots`：引用窗口一旦修订，关联快照变为“待修订”，重新发布后变“已过时”，运维确认重算后可标记“已修订”。
- 复核入口 `window_review`（CLI `review`）按来源、设备、窗口返回当前汇总、读数明细、修订缘由、未归并事件和快照处理状态。

CLI 窗口操作：`window-merge`（接收并并入窗口）、`window-publish`（发布新版本清单）、`snapshot-register`/`snapshot-resolve`、`unmerged`、`review`。`window-merge` 的输入载荷与 `receive` 相同，另支持可选 `received_at` 与 `correction_minutes`。
