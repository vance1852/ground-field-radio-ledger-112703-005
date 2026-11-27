# 低功耗设备接收窗口与射频占用协调

回放目录使用本地合成设备和事件。约定见 [设备资料](protocol.md)，其中说明实体、事件以及如何解释相应的现场观测。

需要 Python 3.11 标准库。运行 `python3 bench.py` 查看设备与观测数量，运行 `python3 -m unittest discover -s tests -v` 核验资料关联和本地 HTTP 往返。用 `python3 bench.py --listen 8765` 启动回放台，终止进程即可关停。可用 `--scene` 指定同格式的另一份回放。

## 链路协调 / 联调工具（`linkbench.py`）

在只读回放台之外，`linkbench.py` 把“设备物理帧 — 业务请求 — 网关发射三阶段 —
设备反馈”关联成一条可追溯链，并按设备合同做确定性调度。它只使用标准库。

```bash
python3 linkbench.py                 # 中文诊断报告
python3 linkbench.py --json          # 完整机器可读报告
python3 linkbench.py --id U3         # 只追某一笔（含逐步轨迹）
python3 bench.py --report            # 同样的报告挂在 bench 入口
```

更换区域 / 机型 / 上传节奏重复使用：

```bash
python3 linkbench.py --scene regionA.ndjson \
        --devices regionA-devices.json --policy regionA-policy.json \
        --cadence 20000 --json
```

### 这份资料核对出的结论（也是内置验收测试）

逻辑当前时刻取全部物理事件时刻的最大值 `now=5060`（`deadline` 不是时钟）。

| 请求 | 业务级别 | 结论 | 选中的无线资源 / 耗时 |
|---|---|---|---|
| `U1` | urgent 自检 | **设备已经收到但后来没回应**（`device_silent_after_heard`），建议补采 | `tower-W`/f1 `[3028,3138)`，实占 **110ms**（参数估计 117ms，以 `tx_air` 为准）；`join=7` 的迟到 heard 只归旧帧 |
| `M1` | normal 抄表 | **基站没发出去**（`not_transmitted`），截止已过，补采 | 当时可行槽 `tower-E`/f1 `[3070,3131)`（避开 protect `[3030,3070)`，180+61=241≤250），但无任何 queue/begin/air |
| `M2` | normal | 已排程，继续等待 | 最终 `tower-W`/f1 `[6050,6119)` 69ms（219/250）。先按最早点占 `tower-E`（180+69=**249**/250），被 `U3` 腾挪后改走 W |
| `U2` | urgent | **占空预算不够**（`duty_budget`），本周期补采 | 仅 W 收见；W/f1 真实占用 40+110=150，+101=**251>250**；E 则 180+101=281，均不可行 |
| `U3` | urgent | 已排程，继续等待 | 只有 E 覆盖 f2：`tower-E`/f2 `[6060,6161)` 101ms；跨频段占用同一 half-duplex 收发机，因此腾挪了 M2 |

资源账本按 **网关（发射资源）× 频段** 各自独立的滚动窗
（`rolling_ms=10000`、`budget_ms=250`）核算区间重合毫秒，不随 join/重连清零，
也不把 E、W 两个网关合并成一个总数。

### 业务侧结论字段

每个 `/jobs` 条目给出：

- `stage`：`classified → scheduled/queued → began → on_air → heard →
  succeeded`（另有 `rejected/expired`），严格区分 **网关收到申请
  (`tx_queue`)、开始发射 (`tx_begin`)、设备确认 (`heard`)** 三个阶段；
- `reason_code` / `reason_label` / `delay_bucket`：四类迟回
  `no_window`（排不进窗口）、`duty_budget`（占空预算不够）、
  `gateway_not_transmitted`（基站没发出去）、
  `device_no_later_response`（设备收到但后来没回应）；
- `recommendation`：`wait*`（继续等待）、`recollect`（补采）、
  `redispatch`（仅在网关拒绝或设备明确报告未生效后）；
- `selected`：选中的网关、group、频段、窗口序号、起讫与确实消耗时长；
- `windows` / `feasibility` / `trace`：两次接收窗（截止点截断）、
  每个“窗口 × 网关”的可行性与失败原因、逐步调度轨迹。

### Python API

```python
from linkbench import analyze, LinkBench, diagnose, plan_all

report = analyze()                       # 读仓库默认三份资料
report = analyze(scene="x.ndjson", devices_path="d.json",
                 policy_path="p.json")
print(diagnose(job_id="U2")["reason_code"])

bench = LinkBench.from_files()           # 有状态联调台
bench.submit_command({"device": "meter", "id": "C1", "action": "sample",
                      "bytes": 8, "class": "normal", "deadline": 7100})
bench.ingest({"seq": 21, "device": "tower-W", "kind": "tx_begin", ...})
bench.advance(to=8000)                   # 推进时间，扫掉错过窗的预约
print(bench.report()["verdicts"])
```

`plan_all()` 给出不按当前时刻裁窗的“全窗口理想排程”（含 `M1→E@3070`），
用于回答“当时最早应怎么走”。

### HTTP 业务接口（`bench.py --listen`）

既有 `/devices`、`/events`、`POST /commands`（始终只回
`receipt="transport-only"`，不改记录）保持不变。新增：

| 方法 / 路径 | 作用 |
|---|---|
| `GET /report` | 完整关联 + 资源 + 预约 + 诊断 |
| `GET /jobs`、`GET /jobs?id=U1` | 全部 / 单笔业务状态 |
| `GET /trace?id=U3` | 一笔请求的完整追踪 |
| `GET /schedule`、`GET /resources` | 当前预约；网关×频段占空与 protect |
| `POST /requests` | 网管投递取样请求，立即返回排程决定与 `/jobs` 视图 |
| `POST /ingest` | 追加一 / 多条合成射频报告，推进办理 |
| `POST /tick` `{"to": 8000}` | 推进虚拟时钟并重新协调 |
| `POST /execute` `{"id":"C1"}` | 可选：把某预约演成 `tx_queue→begin→air→heard` 合成报告，闭合联调环（仅内存） |
| `POST /redispatch` `{"id": "..."}` | 拒绝 / 明确未生效后重新派发 |

调度遵循的优先规则：`urgent` 先于 `normal`，同级按截止点（EDF）与受理顺序；
高优可腾挪**尚未生效**的预约，但已 `tx_begin/tx_air` 的占用不可撤成零；
“每三次机会至少保住最早到期普通待办一次”的配额由 `quota_pick` 落实
（先安排普通请求不得导致任何高优越过硬截止）。同一笔排程效果未确认时，
重复收见同一物理帧不会产生第二次派发。

> 模块别名 `sim.py`、`coordinator.py`、`scheduler.py` 仅为常见导入名做
> 再导出，实现都在 `linkbench.py`。

