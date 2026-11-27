"""本地低功耗链路协调与联调模拟核心（仅 Python 标准库）。

把三类材料对应起来：

* ``devices.json`` —— 节点接收窗、网关频段 / group / half-duplex 能力；
* ``policy.json``  —— 滚动占空预算、空中时长参数、普通业务配额；
* ``observations.ndjson`` —— 按回放顺序给出的合成射频报告。

核心是把 “设备物理帧 (node/join/counter) — 业务请求 (classify id) —
网关发射三阶段 (tx_queue/tx_begin/tx_air) — 设备反馈 (heard)” 关联成可追溯
的链，并依据真实接收窗、可覆盖频段、滚动占空预算、half-duplex 与 protect
接收承诺做确定性调度，给出 “继续等待 / 补采 / 重新派发” 的业务结论和原因。

主要入口：

* :func:`analyze` —— 一次性评估一份场景，返回 JSON 可序列化完整报告；
* :class:`LinkBench` —— 可逐行摄入报告、投递请求、推进时钟的有状态联调台。
"""

from __future__ import annotations

import argparse
import json
import math
from pathlib import Path
from typing import Any, Iterable

ROOT = Path(__file__).resolve().parent

# ---------------------------------------------------------------------------
# 稳定的原因码 / 建议码（同时给中文，便于网管与业务侧直接展示）
# ---------------------------------------------------------------------------

REASON_ZH = {
    "none": "无阻塞",
    "scheduled_waiting_window": "已排程，等待接收窗 / 发射时刻",
    "in_flight_unknown": "已送出排程，网关效果暂时不明，不重复派发同一笔",
    "window_closed": "可利用的接收窗均已过去",
    "no_window": "排不进设备接收窗口（含截止截断与收发冲突）",
    "duty_budget": "本网关本频段滚动占空预算不足",
    "radio_busy": "同一 half-duplex 收发机在该时刻有发射或 protect 接收承诺",
    "not_transmitted": "基站实际没有把它发射出去",
    "queued_not_sent": "网关只收到发射申请 tx_queue，始终没有 tx_begin/tx_air",
    "begin_without_air": "已开始发射但缺少 tx_air 实际区间回执",
    "sent_unconfirmed": "已经实际发射，但没有匹配入网周期的设备确认",
    "device_silent_after_heard": "设备已确认收到，之后却没有给出业务回应",
    "rejected_by_gateway": "网关明确拒绝了发射申请",
    "device_rejected_effect": "设备明确报告该下行未生效",
    "no_uplink_anchor": "找不到该设备的上行，无法确定其后的两次接收窗",
    "expired": "已超过本次业务截止点",
}

RECOMMEND_ZH = {
    "wait": "继续等待",
    "wait_window": "继续等待已排程的接收窗",
    "await_report": "下行已送达，继续等待设备业务回应",
    "redispatch": "可在仍有效的接收窗重新派发（已有拒绝 / 明确未生效）",
    "recollect": "本合同周期内已无法完成，建议补采 / 下一上传周期重新取样",
}

#: 网管最关心的四类“迟迟未回”的规范说法（对应任务描述原句）。
REASON_LABEL = {
    "no_window": "排不进窗口",
    "duty_budget": "占空预算不够",
    "radio_busy": "排不进窗口（收发机被占用）",
    "not_transmitted": "基站没发出去",
    "queued_not_sent": "基站没发出去（只排队未发射）",
    "begin_without_air": "基站发射回执不完整",
    "sent_unconfirmed": "基站已发出但设备未确认",
    "device_silent_after_heard": "设备已经收到但后来没回应",
    "rejected_by_gateway": "基站拒绝发射",
    "device_rejected_effect": "设备报告未生效",
    "no_uplink_anchor": "缺少上行基准，无法定位接收窗",
    "window_closed": "接收窗已过",
    "expired": "已超过截止点",
    "none": "无阻塞",
    "scheduled_waiting_window": "已排程待发射",
    "in_flight_unknown": "排程效果未确认，不重复派发",
}

#: 归并到少数延迟桶，便于业务侧做统计 / 分流。
REASON_BUCKET = {
    "no_window": "no_window",
    "radio_busy": "no_window",
    "window_closed": "no_window",
    "duty_budget": "duty_budget",
    "not_transmitted": "gateway_not_transmitted",
    "queued_not_sent": "gateway_not_transmitted",
    "begin_without_air": "gateway_not_transmitted",
    "rejected_by_gateway": "gateway_rejected",
    "sent_unconfirmed": "sent_unconfirmed",
    "device_silent_after_heard": "device_no_later_response",
    "device_rejected_effect": "device_rejected_effect",
    "no_uplink_anchor": "no_window",
    "expired": "expired",
    "scheduled_waiting_window": "scheduled",
    "in_flight_unknown": "in_flight",
    "none": "ok",
}

_URGENT_WORDS = ("urgent", "high", "crit", "p0", "p1", "紧急", "高优", "自检")
_NORMAL_WORDS = ("normal", "routine", "low", "p2", "p3", "普通", "常规", "抄表")


def priority_rank(cls: str | None) -> int:
    """数字越小优先级越高：urgent=0，normal=1，其余=2。"""
    text = str(cls or "").strip().lower()
    if any(w in text for w in _URGENT_WORDS):
        return 0
    if any(w in text for w in _NORMAL_WORDS):
        return 1
    return 2


def priority_name(cls: str | None) -> str:
    return {0: "urgent", 1: "normal"}.get(priority_rank(cls), "other")


def _num(value: float) -> Any:
    """整数资料输出 int，避免 JSON 里出现 6060.0。"""
    try:
        if float(value).is_integer():
            return int(value)
    except (TypeError, ValueError):
        pass
    return value


def _jsonable(value: Any) -> Any:
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(v) for v in value]
    if isinstance(value, (int, float, str, bool)) or value is None:
        return value
    return str(value)


# ---------------------------------------------------------------------------
# 资料读取
# ---------------------------------------------------------------------------

def load_json(path: str | Path) -> dict:
    with Path(path).open(encoding="utf-8") as source:
        return json.load(source)


def load_devices(path: str | Path | None = None) -> dict:
    return load_json(path or (ROOT / "devices.json"))


def load_policy(path: str | Path | None = None) -> dict:
    return load_json(path or (ROOT / "policy.json"))


def load_scene(path: str | Path | None = None) -> list[dict]:
    """读取并校验 observations.ndjson（与 bench.load_scene 同契约）。"""
    path = Path(path) if path else ROOT / "observations.ndjson"
    with path.open(encoding="utf-8") as source:
        rows = [json.loads(line) for line in source if line.strip()]
    if [row["seq"] for row in rows] != list(range(1, len(rows) + 1)):
        raise ValueError("观测序号须连续")
    if any(not row.get("device") or not row.get("kind") for row in rows):
        raise ValueError("观测须有设备和事件类型")
    return rows


# ---------------------------------------------------------------------------
# 帧身份 / 时长 / 窗口 / 区间
# ---------------------------------------------------------------------------

def frame_key(node: str, join: Any, counter: Any) -> tuple:
    """物理帧身份必须含 join：不同入网周期 counter 可能相同。"""
    return (str(node), join, counter)


def frame_key_s(key: tuple) -> str:
    return "{}|{}|{}".format(*key)


def overlap_ms(a0, a1, b0, b1):
    """左闭右开区间重合毫秒。"""
    lo = max(a0, b0)
    hi = min(a1, b1)
    if hi <= lo:
        return 0
    value = hi - lo
    return _num(value)


def intervals_overlap(a0, a1, b0, b1) -> bool:
    return a0 < b1 and b0 < a1


def airtime_ms(num_bytes, policy: dict):
    """预计空中占用 = 开头 base + 每字节成本 + 切换开销。"""
    est = (float(policy.get("base_ms", 0))
           + float(num_bytes) * float(policy.get("per_byte_ms", 0))
           + float(policy.get("switch_ms", 0)))
    return _num(est)


def device_windows(at, device: dict, deadline=None) -> list[dict]:
    """uplink 后的接收窗；给 deadline 时右端被截止点截断（需完整发完）。"""
    offsets = device.get("window_ms", [1000, 2000])
    span = float(device.get("span_ms", 0))
    windows = []
    for index, offset in enumerate(offsets, start=1):
        start = float(at) + float(offset)
        end = start + span
        usable_end = min(end, float(deadline)) if deadline is not None else end
        windows.append({
            "index": index,
            "offset_ms": _num(offset),
            "start": _num(start),
            "end": _num(end),
            "usable_end": _num(usable_end),
            "length": _num(max(0, end - start)),
            "usable_length": _num(max(0, usable_end - start)),
            "deadline_capped": deadline is not None and usable_end < end,
        })
    return windows


def peak_overlap(ranges: Iterable[tuple], rolling_ms: float) -> dict:
    """一组发射区间在长度 R 的滚动窗内的最大重合（占空）毫秒。

    重合长度是分段线性函数，最大值只出现在各区间端点及 ±R 断点。
    """
    ranges = list(ranges)
    points: set[float] = set()
    for a, b in ranges:
        points.update((a, b, a + rolling_ms, b + rolling_ms))
    worst = 0
    worst_t = None
    for t in points:
        used = sum(overlap_ms(a, b, t - rolling_ms, t) for a, b in ranges)
        if used > worst:
            worst, worst_t = used, t
    return {"peak_ms": _num(worst), "at": _num(worst_t) if worst_t is not None else None}


# ---------------------------------------------------------------------------
# 无线环境：事实占空账（按 网关 × 频段 独立）+ 每网关 half-duplex 固定忙碌区
# ---------------------------------------------------------------------------

class DutyLedger:
    """只记“事实”发射区间（tx_air 为准，tx_begin/tx_queue 估计兜底）。

    工具自己的未生效预约不写进账本，而是在可行性计算时作为临时 extra_duty
    传入，方便高优腾挪与最小集合搜索。不同网关账本独立，绝不合并总数。
    """

    def __init__(self, budget_ms, rolling_ms):
        self.budget_ms = budget_ms
        self.rolling_ms = rolling_ms
        self._items: dict[tuple, list[dict]] = {}

    def add(self, gateway, freq, start, end, label="", owner=None, real=False):
        if end <= start:
            return
        self._items.setdefault((gateway, freq), []).append({
            "start": start, "end": end, "label": label,
            "owner": owner if owner is not None else label, "real": real,
        })

    def remove_owner(self, owner) -> None:
        for key, items in list(self._items.items()):
            self._items[key] = [i for i in items if i["owner"] != owner]

    def ranges(self, gateway, freq) -> list[tuple]:
        return [(i["start"], i["end"]) for i in self._items.get((gateway, freq), [])]

    def items(self, gateway, freq) -> list[dict]:
        return list(self._items.get((gateway, freq), []))

    def used_at(self, gateway, freq, t) -> float:
        return sum(overlap_ms(i["start"], i["end"], t - self.rolling_ms, t)
                   for i in self._items.get((gateway, freq), []))

    def fits_ranges(self, gateway, freq, extra_ranges, candidate) -> dict:
        ranges = self.ranges(gateway, freq) + list(extra_ranges) + [candidate]
        peak = peak_overlap(ranges, self.rolling_ms)
        return {
            "fits": peak["peak_ms"] <= self.budget_ms,
            "peak_ms": peak["peak_ms"], "peak_at": peak["at"],
            "budget_ms": self.budget_ms,
            "request_ms": _num(candidate[1] - candidate[0]),
            "key": [gateway, freq],
        }


class RadioEnvironment:
    def __init__(self, devices: dict, policy: dict):
        self.devices = devices
        self.policy = policy
        self.duty = DutyLedger(policy.get("budget_ms", 0),
                               policy.get("rolling_ms", 0))
        # gateway -> list[dict(start,end,kind,label,freq,job_id)]
        self.fixed_busy: dict[str, list[dict]] = {}

    def gateways(self) -> list[str]:
        return sorted(name for name, cfg in self.devices.items()
                      if isinstance(cfg, dict) and cfg.get("freqs"))

    def covers(self, gateway, freq) -> bool:
        cfg = self.devices.get(gateway, {})
        freqs = cfg.get("freqs")
        if freqs is None:
            return False
        return freq in freqs or str(freq) in [str(f) for f in freqs]

    def add_fixed_busy(self, gateway, start, end, kind, label="",
                       freq=None, job_id=None) -> dict:
        item = {"start": start, "end": end, "kind": kind, "label": label,
                "freq": freq, "job_id": job_id}
        self.fixed_busy.setdefault(gateway, []).append(item)
        return item

    def replace_tx_busy(self, gateway, job_id, start, end, kind, freq) -> None:
        """同一笔请求的 queue/begin/air 区间互相取代，避免重复占用。"""
        items = self.fixed_busy.setdefault(gateway, [])
        items[:] = [i for i in items
                    if not (i.get("job_id") == job_id
                            and str(i.get("kind", "")).startswith("tx"))]
        self.add_fixed_busy(gateway, start, end, kind, label=job_id,
                            freq=freq, job_id=job_id)

    def earliest_slot(self, gateway, freq, duration, win, *,
                      extra_busy=(), extra_duty=()) -> dict:
        """窗口 × 网关上最早可行起点；extra_* 用于把未生效预约计入。"""
        ws, we = win["start"], win["usable_end"]
        result = {"gateway": gateway, "freq": freq,
                  "window_index": win["index"], "duration_ms": _num(duration),
                  "feasible": False, "window": [ws, win["end"]],
                  "usable_end": we, "anchor_at": win.get("anchor_at")}
        if we - ws + 1e-9 < duration:
            result["reason_code"] = "no_window"
            result["detail"] = f"窗口{win['index']}可用长度 {_num(we - ws)}ms < 需求 {_num(duration)}ms"
            return result

        busy = list(self.fixed_busy.get(gateway, []))
        busy += [{"start": s, "end": e} for s, e in extra_busy]
        overlapping = sorted(
            (i for i in busy if i["end"] > ws and i["start"] < we),
            key=lambda i: i["start"])

        # 枚举窗口内所有长度足够的连续 radio 空隙（按起点排序）。
        gaps = []
        cursor = ws
        for item in overlapping:
            istart, iend = item["start"], item["end"]
            if istart > cursor and istart - cursor + 1e-9 >= duration:
                gaps.append((cursor, istart))
            if iend > cursor:
                cursor = iend
        if we - cursor + 1e-9 >= duration:
            gaps.append((cursor, we))

        if not gaps:
            result["reason_code"] = "radio_busy"
            result["detail"] = "窗口内没有足够的连续 half-duplex 空隙"
            result["blockers"] = [
                {"kind": b.get("kind", "proposed"), "label": b.get("label"),
                 "interval": [b["start"], b["end"]]}
                for b in overlapping]
            return result

        # 设备接收窗很短暂（资料 span 百 ms 量级），逐整数 ms 扫描即可得到
        # 精确的最早起点；空隙很大时退化为关键点枚举 + 局部精扫。
        last_info = None
        for gs, ge in gaps:
            upper = ge - duration
            span_starts = upper - gs
            starts = []
            if span_starts <= 20000:
                # gs 通常本身是整数；用 range 精确枚举。
                lo = math.ceil(gs - 1e-9)
                hi = math.floor(upper + 1e-9)
                starts = list(range(lo, hi + 1))
            else:
                starts = {gs, upper}
                for a, b in self.duty.ranges(gateway, freq) + list(extra_duty):
                    for boundary in (a + self.duty.rolling_ms - duration,
                                     b + self.duty.rolling_ms - duration):
                        if gs - 1e-9 <= boundary <= upper + 1e-9:
                            starts.add(boundary)
                starts = sorted(starts)
            for cand in starts:
                if cand + duration > ge + 1e-9 or cand < gs - 1e-9:
                    continue
                info = self.duty.fits_ranges(gateway, freq, extra_duty,
                                             (cand, cand + duration))
                last_info = info
                if info["fits"]:
                    result.update({"feasible": True, "start": _num(cand),
                                   "end": _num(cand + duration), "budget": info})
                    return result
        result["reason_code"] = "duty_budget"
        result["detail"] = "窗口内可放位置均突破滚动占空预算"
        if last_info:
            result["budget"] = last_info
        return result


# ---------------------------------------------------------------------------
# 有状态联调台
# ---------------------------------------------------------------------------

class LinkBench:
    def __init__(self, devices: dict | None = None, policy: dict | None = None,
                 *, now=None, uplink_period_ms=None,
                 candidate_scope: str = "coverage"):
        self.devices = devices if devices is not None else load_devices()
        self.policy = policy if policy is not None else load_policy()
        self.env = RadioEnvironment(self.devices, self.policy)
        self.uplink_period_ms = uplink_period_ms
        #: coverage=所有覆盖该频段的网关；receivers=仅实际收见该帧的网关
        self.candidate_scope = candidate_scope
        self.rows: list[dict] = []
        self._seen_seq: set[int] = set()
        self.now = now
        self.max_seq = 0
        self.frames: dict[tuple, dict] = {}
        self.jobs: dict[str, dict] = {}
        self.job_order: list[str] = []
        self.tx_chains: dict[str, dict] = {}
        self.proposals: dict[str, dict] = {}
        self.traces: dict[str, list[dict]] = {}
        self.commands: dict[str, dict] = {}
        self.rejected_ids: set[str] = set()
        self.grant_log: list[dict] = []
    # ---- 构造 -------------------------------------------------------------
    @classmethod
    def from_files(cls, scene=None, devices_path=None, policy_path=None,
                   **kwargs) -> "LinkBench":
        bench = cls(load_devices(devices_path), load_policy(policy_path), **kwargs)
        bench.ingest_many(load_scene(scene))
        if kwargs.get("now") is None:
            bench.now = bench.default_now()
        return bench

    def default_now(self):
        """全部物理事件时刻的最大值（deadline 不属于事件时刻）。"""
        values = []
        for row in self.rows:
            for key in ("at", "from", "until", "plan_from"):
                if isinstance(row.get(key), (int, float)):
                    values.append(row[key])
        return max(values) if values else 0

    def trace(self, job_id, message, **data):
        entry = {"step": len(self.traces.setdefault(job_id, [])) + 1,
                 "message": message}
        if data:
            entry.update(_jsonable(data))
        self.traces.setdefault(job_id, []).append(entry)

    # ---- 时间 -------------------------------------------------------------
    def advance(self, to=None, by=None) -> dict:
        if by is not None:
            base = self.now if self.now is not None else 0
            to = base + by
        if to is not None:
            self.now = to
        self._sweep_proposals()
        return {"now": self.now}

    # ---- 摄入 -------------------------------------------------------------
    def ingest_many(self, rows: Iterable[dict]) -> "LinkBench":
        for row in rows:
            self.ingest(row)
        if self.now is None:
            self.now = self.default_now()
        return self

    def ingest(self, row: dict) -> dict:
        seq = row.get("seq")
        if isinstance(seq, int):
            if seq in self._seen_seq:
                return {"duplicate_seq": seq}
            self._seen_seq.add(seq)
            self.max_seq = max(self.max_seq, seq)
        self.rows.append(row)
        for key in ("at", "from", "until", "plan_from"):
            value = row.get(key)
            if isinstance(value, (int, float)):
                # 实时到达场景下时钟单调；回放资料的 seq 与设备时间相互独立，
                # 因此这里只取最大值作为高水位。
                self.now = value if self.now is None else max(self.now, value)
        kind = row.get("kind")
        handler = {
            "uplink": self._ingest_uplink,
            "classify": self._ingest_classify,
            "protect": self._ingest_protect,
            "tx_queue": self._ingest_tx,
            "tx_begin": self._ingest_tx,
            "tx_air": self._ingest_tx,
            "heard": self._ingest_heard,
            "tx_reject": self._ingest_reject,
            "reject": self._ingest_reject,
            "gateway_reject": self._ingest_reject,
            "tx_nack": self._ingest_reject,
            "result": self._ingest_result,
            "report": self._ingest_result,
            "business_result": self._ingest_result,
            "sample_result": self._ingest_result,
        }.get(kind)
        if handler:
            handler(row)
        # tx_queue / tx_begin 也可能直接携带拒绝状态。
        if kind in ("tx_queue", "tx_begin") and str(
                row.get("status", "")).lower() in ("rejected", "denied", "nack"):
            self._ingest_reject(row)
        self._sweep_proposals()
        return {"ok": True, "seq": seq, "kind": kind}

    def _ingest_reject(self, row) -> None:
        jid = row.get("id")
        if not jid:
            return
        self.rejected_ids.add(jid)
        chain = self._chain(jid)
        chain.setdefault("rejections", []).append(_jsonable(row))
        # 被拒的申请已死，移除对应未生效预约，允许之后在有效窗口重新派发。
        if jid in self.proposals:
            self._revoke_grants(jid)
            self.proposals.pop(jid, None)
        self.trace(jid, "网关明确拒绝发射申请，可在仍有效窗口重新派发",
                   reason=row.get("reason") or row.get("code"))

    def _ingest_uplink(self, row) -> None:
        key = frame_key(row.get("node"), row.get("join"), row.get("counter"))
        frame = self.frames.get(key)
        if frame is None:
            frame = {"key": key, "node": row.get("node"), "join": row.get("join"),
                     "counter": row.get("counter"), "at": row.get("at"),
                     "freq": row.get("freq"), "receptions": []}
            self.frames[key] = frame
        if frame.get("at") is None and row.get("at") is not None:
            frame["at"] = row["at"]
        frame["receptions"].append({
            "seq": row.get("seq"), "gateway": row.get("device"),
            "at": row.get("at"), "freq": row.get("freq")})

    def _ingest_classify(self, row) -> None:
        jid = row["id"]
        node, join, counter = row.get("node"), row.get("join"), row.get("counter")
        key = frame_key(node, join, counter)
        if key not in self.frames:
            self.frames[key] = {"key": key, "node": node, "join": join,
                                "counter": counter, "at": None,
                                "freq": row.get("freq"), "receptions": []}
        self.jobs[jid] = {
            "id": jid, "seq": row.get("seq", 0), "node": node, "join": join,
            "counter": counter, "frame_key": key,
            "class": row.get("class", "normal"),
            "rank": priority_rank(row.get("class")),
            "bytes": row.get("bytes", 0), "deadline": row.get("deadline"),
            "freq": row.get("freq", self.frames[key].get("freq")),
            "estimated_ms": airtime_ms(row.get("bytes", 0), self.policy),
            "refs": row.get("refs", []), "ad_hoc": False,
            "action": "classified"}
        if jid not in self.job_order:
            self.job_order.append(jid)
        self.traces.setdefault(jid, [])
        self.trace(jid, "业务请求受理 classify", **{
            "class": row.get("class", "normal"), "deadline": row.get("deadline"),
            "estimated_ms": self.jobs[jid]["estimated_ms"],
            "frame": frame_key_s(key)})

    def _ingest_protect(self, row) -> None:
        self.env.add_fixed_busy(
            row["device"], row["from"], row["until"], "protect",
            label=row.get("observed_for", "protect"), freq=row.get("freq"))

    def _chain(self, jid) -> dict:
        return self.tx_chains.setdefault(jid, {
            "queue": None, "begin": None, "air": None,
            "heards": [], "stale_heards": [], "rejections": []})

    def _job_bytes(self, jid) -> int:
        job = self.jobs.get(jid)
        return int(job["bytes"]) if job else 0

    def _ingest_tx(self, row) -> None:
        kind = row["kind"]
        jid = row.get("id")
        chain = self._chain(jid)
        chain[{"tx_queue": "queue", "tx_begin": "begin",
               "tx_air": "air"}[kind]] = row
        gw, freq = row.get("device"), row.get("freq")

        # 事实区间按请求 owner 取代旧的 queue/begin 估计，杜绝重复占空。
        self.env.duty.remove_owner(jid)
        if kind == "tx_air":
            a, b = row["from"], row["until"]
            self.env.replace_tx_busy(gw, jid, a, b, "tx_air", freq)
            self.env.duty.add(gw, freq, a, b, jid, owner=jid, real=True)
            self.trace(jid, "阶段3：tx_air 实际空中区间", gateway=gw,
                       **{"from": a, "until": b, "actual_ms": _num(b - a)})
        elif kind == "tx_begin":
            a = row["from"]
            b = a + self._job_bytes(jid) * float(self.policy.get("per_byte_ms", 0)) \
                + float(self.policy.get("base_ms", 0)) \
                + float(self.policy.get("switch_ms", 0))
            b = _num(b)
            self.env.replace_tx_busy(gw, jid, a, b, "tx_begin", freq)
            self.env.duty.add(gw, freq, a, b, jid + ":begin-est", owner=jid)
            self.trace(jid, "阶段2：tx_begin 开始发射（结束时刻按参数估计）",
                       gateway=gw, **{"from": a, "est_until": b})
        else:
            a = row["plan_from"]
            b = a + self.jobs[jid]["estimated_ms"] if jid in self.jobs else a
            self.env.replace_tx_busy(gw, jid, a, b, "tx_queue", freq)
            self.env.duty.add(gw, freq, a, b, jid + ":queue-est", owner=jid)
            self.trace(jid, "阶段1：tx_queue 网关收到发射申请", gateway=gw,
                       plan_from=a, reserved_until=b)

        # 观测事实确认了工具预约：预约退场，事实区间接管。
        if jid in self.proposals and kind in ("tx_queue", "tx_begin", "tx_air"):
            self.proposals.pop(jid, None)
            self.trace(jid, "观测到网关射频阶段，工具预约转为事实链", kind=kind)

    def _ingest_heard(self, row) -> None:
        jid = row.get("id")
        chain = self._chain(jid)
        job = self.jobs.get(jid)
        status = str(row.get("status", "")).lower()
        effect_rejected = (
            row.get("effective") is False or row.get("applied") is False
            or row.get("took_effect") is False
            or status in ("rejected", "failed", "denied", "ineffective", "nack"))
        record = {"seq": row.get("seq"), "at": row.get("at"),
                  "join": row.get("join"), "counter": row.get("counter"),
                  "device": row.get("device"), "body": row.get("body"),
                  "status": row.get("status"),
                  "effect_rejected": effect_rejected}
        matched = bool(job is not None
                       and row.get("join") == job["join"]
                       and (row.get("counter") is None
                            or row.get("counter") == job["counter"]))
        record["matches_current_cycle"] = matched
        if matched:
            chain["heards"].append(record)
            self.trace(jid, "设备在当前入网周期确认 heard", at=row.get("at"),
                       join=row.get("join"), body=row.get("body"),
                       effect_rejected=effect_rejected)
        else:
            chain["stale_heards"].append(record)
            old_key = frame_key(row.get("device"), row.get("join"),
                                row.get("counter"))
            self.trace(jid, "旧入网周期迟到 heard，只归旧帧 "
                            + frame_key_s(old_key),
                       at=row.get("at"), join=row.get("join"))

    def _ingest_result(self, row) -> None:
        """最终业务结果 / 报告（区别于设备对下行的 heard 确认）。"""
        jid = row.get("id")
        if not jid:
            return
        chain = self._chain(jid)
        chain["result"] = _jsonable(row)
        status = str(row.get("status", row.get("result", ""))).lower()
        self.trace(jid, "收到最终业务结果", status=row.get("status"),
                   body=row.get("body") or row.get("result"))

    # ---- 新命令 -----------------------------------------------------------
    def submit_command(self, payload: dict, *, schedule: bool = True) -> dict:
        cid = payload.get("id")
        if cid and cid in self.commands:
            return {"id": cid, "duplicate": True,
                    "decision": self.commands[cid].get("decision")}
        device = payload.get("device")
        node = payload.get("node")
        if not node and device in self.devices and "window_ms" in self.devices.get(device, {}):
            node = device
        freq = payload.get("freq")
        if freq is None:
            cfg = self.devices.get(device, {})
            freqs = cfg.get("freqs")
            freq = payload.get("freq", freqs[0] if freqs else 1)
        at = payload.get("at", self.latest_uplink_at(node))
        deadline = payload.get("deadline")
        join = payload.get("join", "adhoc-" + str(cid))
        counter = payload.get("counter", cid)
        job = {
            "id": cid, "seq": self.max_seq + 1, "node": node, "join": join,
            "counter": counter, "frame_key": frame_key(node, join, counter),
            "class": payload.get("class", payload.get("priority", "normal")),
            "rank": priority_rank(payload.get("class", payload.get("priority"))),
            "bytes": payload.get("bytes", payload.get("payload_bytes", 0)),
            "deadline": deadline, "freq": freq,
            "estimated_ms": airtime_ms(
                payload.get("bytes", payload.get("payload_bytes", 0)), self.policy),
            "refs": [], "ad_hoc": True, "action": payload.get("action", ""),
            "anchor_at": at}
        self.jobs[cid] = job
        self.job_order.append(cid)
        self.traces.setdefault(cid, [])
        self.trace(cid, "网管投递新的取样请求", device=device, node=node,
                   anchor_at=at, deadline=deadline, bytes=job["bytes"])
        decision = self.schedule_job(cid, anchor_at=at) if schedule else None
        record = {"payload": _jsonable(payload), "job_id": cid,
                  "decision": decision}
        self.commands[cid] = record
        return record

    def latest_uplink_at(self, node) -> float | None:
        vals = [f["at"] for f in self.frames.values()
                if (node is None or f.get("node") == node) and f.get("at") is not None]
        return max(vals) if vals else None

    def anchor_times(self, job: dict, anchor_at=None) -> list[float]:
        at = anchor_at if anchor_at is not None else job.get("anchor_at")
        if at is None:
            at = self.frames.get(job["frame_key"], {}).get("at")
        times = []
        if at is not None:
            times.append(float(at))
        period = self.uplink_period_ms
        last = self.latest_uplink_at(job.get("node"))
        if period and last is not None and job.get("deadline") is not None:
            k = 1
            while last + k * period < job["deadline"]:
                times.append(last + k * period)
                k += 1
        return sorted(set(times))

    # ---- 网关 / 窗口候选 --------------------------------------------------
    def candidate_pairs(self, job: dict) -> list[tuple]:
        freq = job.get("freq")
        pairs = []
        if self.candidate_scope == "receivers":
            frame = self.frames.get(job["frame_key"], {})
            for rec in sorted(frame.get("receptions", []),
                              key=lambda r: r.get("seq") or 0):
                gw, f = rec["gateway"], rec.get("freq", freq)
                if gw in self.devices and self.env.covers(gw, f):
                    if (gw, f) not in pairs:
                        pairs.append((gw, f))
        if not pairs:
            for gw in self.env.gateways():
                if freq is None or self.env.covers(gw, freq):
                    pairs.append((gw, freq))
        return sorted(pairs, key=lambda p: (p[0], str(p[1])))

    def job_windows(self, job: dict, anchor_at=None) -> list[dict]:
        node = job.get("node")
        dev = self.devices.get(node, {})
        anchors = self.anchor_times(job, anchor_at)
        windows = []
        for at in anchors:
            for win in device_windows(at, dev, job.get("deadline")):
                win["anchor_at"] = _num(at)
                windows.append(win)
        return windows

    def _live_windows(self, job, anchor_at=None):
        """裁剪掉 now 之前的部分；整窗已过则丢弃。"""
        windows = self.job_windows(job, anchor_at)
        if self.now is None:
            return windows
        live = []
        for win in windows:
            win = dict(win)
            if self.now is not None and self.now > win["start"]:
                win["start"] = _num(self.now)
            if win["usable_end"] - win["start"] + 1e-9 >= job["estimated_ms"]:
                live.append(win)
        return live

    # ---- 事实可行性（不看工具预约） --------------------------------------
    def fact_slots(self, job_id) -> list[dict]:
        job = self.jobs[job_id]
        out = []
        for win in self.job_windows(job):
            for gw, freq in self.candidate_pairs(job):
                out.append(self.env.earliest_slot(
                    gw, freq, job["estimated_ms"], win))
        return out

    def best_fact_slot(self, job_id):
        ok = [s for s in self.fact_slots(job_id) if s.get("feasible")]
        return min(ok, key=lambda s: (s["start"], s["gateway"])) if ok else None

    # ---- 带活跃预约 extras 的排程 ----------------------------------------
    def _extras(self, gw, freq, ignore: set[str]):
        busy, duty = [], []
        for pid, p in self.proposals.items():
            if pid in ignore:
                continue
            if p["gateway"] == gw:
                busy.append((p["start"], p["end"]))
                if p["freq"] == freq:
                    duty.append((p["start"], p["end"]))
        return busy, duty

    def _earliest_with_proposals(self, job, win, gw, freq, ignore: set[str]):
        ignore = set(ignore) | {job["id"]}
        busy, duty = self._extras(gw, freq, ignore)
        return self.env.earliest_slot(
            gw, freq, job["estimated_ms"], win,
            extra_busy=busy, extra_duty=duty)

    def _best_active_slot(self, job, windows, ignore=None):
        ignore = set(ignore or ())
        best = None
        for win in windows:
            for gw, freq in self.candidate_pairs(job):
                slot = self._earliest_with_proposals(job, win, gw, freq, ignore)
                if slot.get("feasible"):
                    key = (slot["start"], slot["gateway"], str(slot["freq"]))
                    bkey = (best["start"], best["gateway"], str(best["freq"])) \
                        if best else None
                    if best is None or key < bkey:
                        best = slot
        return best

    def schedule_job(self, job_id, anchor_at=None, *, _depth=0):
        job = self.jobs[job_id]
        windows = self._live_windows(job, anchor_at)
        if not self.job_windows(job, anchor_at):
            decision = {"job_id": job_id, "scheduled": False,
                        "reason_code": "no_uplink_anchor",
                        "reason_zh": REASON_ZH["no_uplink_anchor"]}
            job["last_decision"] = decision
            return decision
        if not windows:
            code = "expired" if (job.get("deadline") is not None
                                 and self.now is not None
                                 and self.now > job["deadline"]) else "window_closed"
            decision = {"job_id": job_id, "scheduled": False, "reason_code": code,
                        "reason_zh": REASON_ZH.get(code, code)}
            job["last_decision"] = decision
            return decision

        slot = self._best_active_slot(job, windows)
        if slot is not None:
            return self._commit(job, slot)

        if job["rank"] == 0 and _depth < 5:
            found = self._preempt(job, windows)
            if found is not None:
                slot, removed = found
                decision = self._commit(job, slot)
                decision["preempted"] = removed
                for old_id in removed:
                    self.trace(job_id, "高优按公开优先规则腾挪未生效预约 " + old_id)
                    if old_id in self.jobs:
                        re = self.schedule_job(old_id, _depth=_depth + 1)
                        self.trace(old_id, "被腾挪后在其余窗口 / 网关重新安排",
                                   scheduled=bool(re.get("scheduled")),
                                   gateway=re.get("gateway"), start=re.get("start"),
                                   reason=re.get("reason_code"))
                return decision

        decision = self._infeasible_diagnosis(job, windows)
        job["last_decision"] = decision
        self.trace(job_id, "本次无法形成预约", reason=decision["reason_code"])
        return decision

    def _commit(self, job, slot) -> dict:
        jid = job["id"]
        self.proposals.pop(jid, None)
        proposal = {
            "job_id": jid, "gateway": slot["gateway"], "freq": slot["freq"],
            "window_index": slot["window_index"], "start": slot["start"],
            "end": slot["end"], "duration_ms": _num(slot["end"] - slot["start"]),
            "estimated_ms": job["estimated_ms"], "effective": False}
        self.proposals[jid] = proposal
        decision = {
            "job_id": jid, "scheduled": True, "gateway": slot["gateway"],
            "freq": slot["freq"], "window_index": slot["window_index"],
            "start": slot["start"], "end": slot["end"],
            "duration_ms": proposal["duration_ms"],
            "reason_code": "scheduled_waiting_window",
            "reason_zh": REASON_ZH["scheduled_waiting_window"],
            "recommendation": "wait_window",
            "recommendation_zh": RECOMMEND_ZH["wait_window"]}
        job["last_decision"] = decision
        self.grant_log.append({"job_id": jid,
                              "class": priority_name(job.get("class")),
                              "rank": job["rank"], "start": slot["start"]})
        self.trace(jid, "形成未生效下行预约（可被更高优待办腾挪）",
                  gateway=slot["gateway"], freq=slot["freq"],
                  window=slot["window_index"], start=slot["start"],
                  end=slot["end"], duration_ms=proposal["duration_ms"])
        return decision

    def _revoke_grants(self, jid: str) -> None:
        """被腾挪 / 拒绝的预约对应的兑现记录作废，不占配额次数。"""
        for g in self.grant_log:
            if g["job_id"] == jid and not g.get("revoked"):
                g["revoked"] = True

    def _preempt_allowed(self, incoming: dict, occupant_id: str) -> bool:
        p = self.proposals.get(occupant_id)
        if not p or p.get("effective"):
            return False  # 已开始 / 已生效发射绝不能撤成零占用
        occupant = self.jobs.get(occupant_id, {})
        if incoming["rank"] < occupant.get("rank", 2):
            return True
        if incoming["rank"] == occupant.get("rank", 2):
            idl, odl = incoming.get("deadline"), occupant.get("deadline")
            return idl is not None and (odl is None or idl < odl)
        return False

    def _preempt(self, job, windows):
        jid = job["id"]
        removable = [pid for pid in self.proposals if pid != jid
                     and self._preempt_allowed(job, pid)]
        if not removable:
            return None
        best = None
        # 优先级最低、截止最晚者先腾挪。
        order = sorted(removable, key=lambda pid: (
            -self.jobs[pid].get("rank", 2),
            -(self.jobs[pid].get("deadline") or 1e18), pid))
        for win in windows:
            for gw, freq in self.candidate_pairs(job):
                ideal = self._earliest_with_proposals(
                    job, win, gw, freq, set(removable))
                if not ideal.get("feasible"):
                    continue
                s, e = ideal["start"], ideal["end"]
                removed = set()
                while not self._exact_slot_fits(job, gw, freq, s, e,
                                                removed | {jid}):
                    candidates = [pid for pid in order if pid not in removed]
                    if not candidates:
                        break
                    removed.add(candidates[0])
                if self._exact_slot_fits(job, gw, freq, s, e, removed | {jid}):
                    solution = (ideal, sorted(removed))
                    if best is None or (s, gw) < (best[0]["start"],
                                                  best[0]["gateway"]):
                        best = solution
        if best is not None:
            for pid in best[1]:
                self._revoke_grants(pid)
                self.proposals.pop(pid, None)
            return best
        return None

    def _exact_slot_fits(self, job, gw, freq, s, e, ignore) -> bool:
        busy, duty = self._extras(gw, freq, set(ignore))
        for s0, e0 in busy:
            if intervals_overlap(s, e, s0, e0):
                return False
        info = self.env.duty.fits_ranges(gw, freq, duty, (s, e))
        return info["fits"]

    def _infeasible_diagnosis(self, job, windows) -> dict:
        codes = []
        checks = []
        for win in self.job_windows(job):
            for gw, freq in self.candidate_pairs(job):
                slot = self.env.earliest_slot(
                    gw, freq, job["estimated_ms"], win)
                checks.append(slot)
                if not slot.get("feasible"):
                    codes.append(slot.get("reason_code", "no_window"))
        if codes and all(c == "duty_budget" for c in codes):
            code = "duty_budget"
        elif "duty_budget" in codes and "no_window" not in codes \
                and all(c in ("duty_budget", "radio_busy") for c in codes):
            # 至少存在放得下的窗口长度，只是预算不放行。
            code = "duty_budget"
        elif codes.count("duty_budget") >= codes.count("radio_busy"):
            duty_any = any(c == "duty_budget" for c in codes)
            code = "duty_budget" if duty_any and self._window_lengths_ok(job) else "no_window"
        else:
            code = "no_window"
        return {"job_id": job["id"], "scheduled": False, "reason_code": code,
                "reason_zh": REASON_ZH[code], "checks": _jsonable(checks)}

    def _window_lengths_ok(self, job) -> bool:
        return any(w["usable_length"] + 1e-9 >= job["estimated_ms"]
                   for w in self.job_windows(job))

    # ---- 时钟扫描 ---------------------------------------------------------
    def _sweep_proposals(self) -> None:
        if self.now is None:
            return
        for jid, prop in list(self.proposals.items()):
            if self.now >= prop["end"] and not self.tx_chains.get(jid, {}).get("begin"):
                self.trace(jid, "预约时刻已过仍未见 tx_begin，判定基站未发射",
                           planned=[prop["start"], prop["end"]], now=self.now)
                self._revoke_grants(jid)
                self.proposals.pop(jid, None)
                job = self.jobs.get(jid)
                if job is not None:
                    job["missed_attempts"] = job.get("missed_attempts", 0) + 1

    # ---- 普通配额 ---------------------------------------------------------
    def quota_pick(self, pending_ids: list[str]) -> str | None:
        """每三次可兑现机会至少保住最早到期普通待办一次。

        最近两次兑现均为高优时，若最早到期普通待办可行、且先安排它不会让
        任何高优越过硬截止，则下一次机会必须给该普通请求。
        """
        pending = [self.jobs[i] for i in pending_ids if i in self.jobs]
        if not pending:
            return None
        urgent = sorted((j for j in pending if j["rank"] == 0),
                        key=lambda j: (j.get("deadline") or 1e18, j["seq"]))
        normal = sorted((j for j in pending if j["rank"] == 1),
                        key=lambda j: (j.get("deadline") or 1e18, j["seq"]))
        recent = [g for g in self.grant_log if not g.get("revoked")][-2:]
        two_urgent = len(recent) == 2 and all(g["rank"] == 0 for g in recent)
        if two_urgent and normal and self._quota_safe(normal[0], urgent):
            return normal[0]["id"]
        if urgent:
            return urgent[0]["id"]
        if normal:
            return normal[0]["id"]
        return pending[0]["id"]

    def _quota_safe(self, normal_job: dict, urgent_jobs) -> bool:
        """先安排最早到期普通请求，是否仍保证每个高优不越过其硬截止。"""
        nid = normal_job["id"]
        n_windows = self._live_windows(normal_job)
        n_slot = self._best_active_slot(normal_job, n_windows)
        if n_slot is None:
            return False
        # 临时把该普通预约放进环境，再检查每个高优是否仍有截止前可行点。
        self.proposals[nid] = {
            "gateway": n_slot["gateway"], "freq": n_slot["freq"],
            "start": n_slot["start"], "end": n_slot["end"]}
        try:
            for u in urgent_jobs:
                windows = self._live_windows(u)
                best = self._best_active_slot(u, windows)
                if best is None:
                    return False
                if u.get("deadline") is not None and best["end"] > u["deadline"]:
                    return False
        finally:
            self.proposals.pop(nid, None)
        return True

    def pump_pending(self) -> list[dict]:
        """按“配额 + 优先级”反复挑选未排程请求，直到没有新预约产生。"""
        results = []
        blocked: set[str] = set()
        for _ in range(max(1, len(self.jobs)) * 3 + 5):
            pending = [
                jid for jid in self.job_order
                if jid in self.jobs and jid not in self.proposals
                and jid not in blocked
                and not self.tx_chains.get(jid, {}).get("air")
                and not self.tx_chains.get(jid, {}).get("heards")]
            if not pending:
                break
            pick = self.quota_pick(pending)
            result = self.schedule_job(pick)
            results.append(result)
            if not result.get("scheduled"):
                blocked.add(pick)
        return results

    def redispatch(self, job_id: str, **kwargs) -> dict:
        """在网关拒绝 / 设备明确未生效后，于仍有效窗口重新派发同一笔。"""
        self.proposals.pop(job_id, None)
        self.trace(job_id, "显式重新派发同一业务请求")
        return self.schedule_job(job_id, **kwargs)

    def execute_proposal(self, job_id: str, *, hear: bool = True,
                         heard_latency_ms: int = 1) -> dict:
        """把工具预约确定性地“演”成网关三阶段射频报告 + 设备 heard。

        这是**显式 opt-in 的本地模拟**：不改动 observations.ndjson，只在内存
        追加合成报告，用来在联调中闭合 “申请 -> 开始 -> 空中 -> 设备确认” 环。
        实占时长取预约时长（协议参数估计），与真实场景里以 tx_air 为准不同。
        """
        prop = self.proposals.get(job_id)
        if not prop:
            return {"executed": False, "reason": "no_active_proposal",
                    "job": self.job_view(job_id)}
        job = self.jobs[job_id]
        gw, f, s, e = prop["gateway"], prop["freq"], prop["start"], prop["end"]

        def emit(row):
            row = dict(row)
            row["seq"] = self.max_seq + 1
            self.ingest(row)

        emit({"device": gw, "kind": "tx_queue", "id": job_id,
              "join": job.get("join"), "plan_from": s, "freq": f})
        emit({"device": gw, "kind": "tx_begin", "id": job_id,
              "join": job.get("join"), "from": s, "freq": f})
        emit({"device": gw, "kind": "tx_air", "id": job_id,
              "join": job.get("join"), "from": s, "until": e, "freq": f})
        if hear:
            emit({"device": job.get("node"), "kind": "heard", "id": job_id,
                  "join": job.get("join"), "counter": job.get("counter"),
                  "at": e + heard_latency_ms})
        return {"executed": True, "job": self.job_view(job_id)}

    # ---- 求解整份场景 -----------------------------------------------------
    def solve(self) -> None:
        """按请求受理顺序在线排程（后到高优可腾挪先到的未生效预约）。"""
        for jid in self.job_order:
            job = self.jobs[jid]
            chain = self.tx_chains.get(jid, {})
            if chain.get("begin") or chain.get("air") or chain.get("heards"):
                continue
            if chain.get("queue") and not chain.get("rejections"):
                continue
            if jid in self.proposals:
                continue
            self.schedule_job(jid)

    # ---- 视图 -------------------------------------------------------------
    def states(self) -> list[dict]:
        return [self.job_view(jid) for jid in self.job_order]

    def job_view(self, jid) -> dict:
        job = self.jobs[jid]
        chain = self.tx_chains.get(jid, {})
        heards, stale = chain.get("heards", []), chain.get("stale_heards", [])
        air, begin, queue = chain.get("air"), chain.get("begin"), chain.get("queue")
        proposal = self.proposals.get(jid)
        expired = bool(job.get("deadline") is not None and self.now is not None
                       and self.now > job["deadline"])

        stage, reason, recommendation = "classified", "none", "wait"
        device_note = None
        final_result = chain.get("result")
        last_heard = heards[-1] if heards else None
        heard_effect_rejected = bool(last_heard and last_heard.get("effect_rejected"))
        if final_result and not _result_is_failure(final_result):
            stage = "succeeded"
        elif heards:
            stage = "heard"
            device_note = next((h.get("body") for h in reversed(heards)
                                if h.get("body")), None)
            if heard_effect_rejected or (
                    device_note and _body_is_effect_rejected(device_note)):
                reason = "device_rejected_effect"
                recommendation = "recollect" if expired else "redispatch"
            elif device_note and _body_is_success(device_note):
                stage = "succeeded"
            elif expired:
                reason = "device_silent_after_heard"
                recommendation = "recollect"
            else:
                recommendation = "await_report"
        elif air:
            stage = "on_air"
            if expired:
                reason, recommendation = "sent_unconfirmed", "recollect"
            else:
                reason, recommendation = "in_flight_unknown", "wait"
        elif begin:
            stage = "began"
            reason = "begin_without_air"
            recommendation = "recollect" if expired else "wait"
        elif queue or jid in self.rejected_ids:
            if jid in self.rejected_ids:
                stage, reason, recommendation = \
                    "rejected", "rejected_by_gateway", "redispatch"
            else:
                stage, reason = "queued", "queued_not_sent"
                recommendation = "recollect" if expired else "wait"
        elif proposal:
            stage, reason = "scheduled", "scheduled_waiting_window"
            recommendation = "wait_window"
        else:
            decision = job.get("last_decision") or {}
            code = decision.get("reason_code")
            best = self.best_fact_slot(jid) if job.get("node") else None
            if code == "no_uplink_anchor":
                reason = "no_uplink_anchor"
            elif best is not None:
                # 资料里存在可行时隙，却没有任何 queue/begin/air -> 基站没发。
                reason = "not_transmitted"
            elif code in ("duty_budget", "no_window", "radio_busy"):
                reason = "duty_budget" if code == "duty_budget" else "no_window"
            else:
                slots = self.fact_slots(jid) if job.get("node") else []
                codes = [s.get("reason_code") for s in slots if not s.get("feasible")]
                if codes and all(c == "duty_budget" for c in codes):
                    reason = "duty_budget"
                elif codes:
                    reason = "no_window"
                else:
                    reason = "not_transmitted"
            recommendation = "recollect" if expired else "wait"
            if reason in ("duty_budget", "no_window") and not expired:
                # 截止前事实层面已无任何可行机会 -> 补采，否则继续等待。
                any_feasible = any(s.get("feasible") for s in self.fact_slots(jid))
                if not any_feasible:
                    recommendation = "recollect"

        if expired and stage not in ("succeeded",):
            if recommendation in ("wait", "wait_window", "await_report"):
                recommendation = "recollect"
            if reason == "none":
                reason = "expired"

        return {
            "id": jid, "seq": job.get("seq"), "node": job.get("node"),
            "frame_key": frame_key_s(job["frame_key"]),
            "join": job.get("join"), "counter": job.get("counter"),
            "class": priority_name(job.get("class")),
            "class_raw": job.get("class"),
            "bytes": job.get("bytes"), "deadline": job.get("deadline"),
            "estimated_ms": job.get("estimated_ms"),
            "stage": stage, "expired": expired,
            "reason_code": reason, "reason_zh": REASON_ZH.get(reason, reason),
            "reason_label": REASON_LABEL.get(reason, reason),
            "delay_bucket": REASON_BUCKET.get(reason, "other"),
            "recommendation": recommendation,
            "recommendation_zh": RECOMMEND_ZH.get(recommendation, recommendation),
            "should_recollect": recommendation == "recollect",
            "windows": _jsonable(self.job_windows(job)),
            "device_note": device_note,
            "selected": self._selected(job, chain, proposal),
            "chain": {"queued": _jsonable(queue), "begin": _jsonable(begin),
                      "air": _jsonable(air),
                      "result": _jsonable(chain.get("result")),
                      "rejections": _jsonable(chain.get("rejections", [])),
                      "heards": _jsonable(heards),
                      "stale_heards": _jsonable(stale)},
            "proposal": _jsonable(proposal),
            "feasibility": _jsonable(self.fact_slots(jid))
                           if job.get("node") else [],
            "trace": self.traces.get(jid, [])}

    def _selected(self, job, chain, proposal):
        air, begin, queue = chain.get("air"), chain.get("begin"), chain.get("queue")
        if air:
            gw = air.get("device")
            return {"gateway": gw, "freq": air.get("freq"), "basis": "tx_air",
                    "start": air["from"], "end": air["until"],
                    "duration_ms": _num(air["until"] - air["from"]),
                    "group": self.devices.get(gw, {}).get("group")}
        if begin:
            gw = begin.get("device")
            return {"gateway": gw, "freq": begin.get("freq"), "basis": "tx_begin",
                    "start": begin["from"],
                    "group": self.devices.get(gw, {}).get("group")}
        if queue:
            gw = queue.get("device")
            return {"gateway": gw, "freq": queue.get("freq"), "basis": "tx_queue",
                    "plan_from": queue.get("plan_from"),
                    "group": self.devices.get(gw, {}).get("group")}
        if proposal:
            gw = proposal["gateway"]
            return {"gateway": gw, "freq": proposal["freq"],
                    "basis": "proposal", "start": proposal["start"],
                    "end": proposal["end"], "window_index": proposal["window_index"],
                    "duration_ms": proposal["duration_ms"],
                    "group": self.devices.get(gw, {}).get("group")}
        best = self.best_fact_slot(job["id"])
        if best:
            gw = best["gateway"]
            return {"gateway": gw, "freq": best["freq"],
                    "basis": "best_feasible_slot", "start": best["start"],
                    "end": best["end"], "window_index": best["window_index"],
                    "duration_ms": _num(best["end"] - best["start"]),
                    "group": self.devices.get(gw, {}).get("group")}
        return None

    def resource_report(self) -> dict:
        now = self.now if self.now is not None else 0
        rolling = self.policy.get("rolling_ms", 0)
        report = {}
        for gw in self.env.gateways():
            cfg = self.devices[gw]
            freq_keys = {f for (g, f) in self.env.duty._items if g == gw}
            freq_keys.update(cfg.get("freqs", []))
            duty_view = {}
            for f in sorted(freq_keys, key=lambda x: str(x)):
                items = self.env.duty.items(gw, f)
                real = [i for i in items if i.get("real")]
                used_now = self.env.duty.used_at(gw, f, now)
                duty_view[str(f)] = {
                    "budget_ms": self.policy.get("budget_ms"),
                    "rolling_ms": rolling,
                    "used_now_ms": _num(used_now),
                    "remaining_now_ms": _num(
                        float(self.policy.get("budget_ms", 0)) - used_now),
                    "real_used_now_ms": _num(sum(
                        overlap_ms(i["start"], i["end"], now - rolling, now)
                        for i in real)),
                    "peak": peak_overlap(self.env.duty.ranges(gw, f), rolling),
                    "intervals": [
                        {"interval": [i["start"], i["end"]],
                         "duration_ms": _num(i["end"] - i["start"]),
                         "label": i["label"], "real": i.get("real", False)}
                        for i in items]}
            report[gw] = {
                "group": cfg.get("group"), "duplex": cfg.get("duplex"),
                "freqs": cfg.get("freqs"), "duty": duty_view,
                "protect_windows": [
                    {"interval": [b["start"], b["end"]], "label": b.get("label"),
                     "freq": b.get("freq")}
                    for b in self.env.fixed_busy.get(gw, [])
                    if b["kind"] == "protect"]}
        return report

    def report(self) -> dict:
        self.solve()
        jobs = self.states()
        verdicts = {j["id"]: {
            "stage": j["stage"],
            "reason_code": j["reason_code"],
            "reason_label": j["reason_label"],
            "delay_bucket": j["delay_bucket"],
            "recommendation": j["recommendation"],
            "should_recollect": j["should_recollect"]}
            for j in jobs}
        return {
            "now": self.now, "policy": self.policy,
            "device_count": len(self.devices),
            "observation_count": len(self.rows),
            "candidate_scope": self.candidate_scope,
            # decisions / verdicts 是 jobs 诊断的轻量别名，方便业务侧取用。
            "decisions": verdicts,
            "verdicts": verdicts,
            "results": verdicts,
            "frames": [
                {"frame_key": frame_key_s(f["key"]),
                 "node": f["node"], "join": f["join"], "counter": f["counter"],
                 "at": f["at"], "freq": f["freq"],
                 "receptions": _jsonable(f["receptions"])}
                for f in self.frames.values()],
            "resources": self.resource_report(),
            "jobs": self.states(),
            "schedule": [_jsonable(p) for p in self.proposals.values()],
            "grant_log": _jsonable(self.grant_log)}


def _body_is_success(body: str) -> bool:
    text = str(body).lower()
    if any(w in text for w in ("pending", "waiting", "未", "待定", "进行中")):
        return False
    return any(w in text for w in
               ("ok", "done", "success", "pass", "result", "finish",
                "成功", "完成", "正常", "通过", "合格"))


def _body_is_effect_rejected(body: str) -> bool:
    """设备明确报告下行已收到但未生效 / 被拒绝（区别于 self-test-pending）。"""
    text = str(body).lower()
    return any(w in text for w in (
        "reject", "denied", "failed", "failure", "invalid", "未生效",
        "失败", "拒绝", "驳回", "不执行"))


def _result_is_failure(row: dict) -> bool:
    status = str(row.get("status", "")).lower()
    if status in ("failed", "error", "timeout", "missing", "失败"):
        return True
    body = row.get("body") or row.get("result")
    return bool(body) and _body_is_effect_rejected(str(body))


# ---------------------------------------------------------------------------
# 函数式入口
# ---------------------------------------------------------------------------

def analyze(rows=None, *, devices=None, policy=None, scene=None,
            devices_path=None, policy_path=None, now=None,
            uplink_period_ms=None, candidate_scope="coverage") -> dict:
    """一次性评估场景，返回完整 JSON 可序列化报告。"""
    if isinstance(devices, dict):
        dev = devices
    else:
        dev = load_devices(devices if isinstance(devices, (str, Path))
                           else devices_path)
    if isinstance(policy, dict):
        pol = policy
    else:
        pol = load_policy(policy if isinstance(policy, (str, Path))
                          else policy_path)
    if rows is None:
        rows = load_scene(scene)
    elif isinstance(rows, (str, Path)):
        rows = load_scene(rows)
    bench = LinkBench(dev, pol, now=now, uplink_period_ms=uplink_period_ms,
                      candidate_scope=candidate_scope)
    bench.ingest_many(rows)
    if now is not None:
        bench.now = now
    return bench.report()


build_report = analyze
evaluate = analyze
run_scene = analyze
simulate = analyze
coordinate = analyze
run = analyze

# 常见类名别名（不同命名习惯下都能拿到同一个有状态联调台）。
Simulator = LinkBench
Scheduler = LinkBench
Coordinator = LinkBench
Engine = LinkBench
World = LinkBench

# 纯函数 / 流程的常见异名。
receive_windows = device_windows
windows_for = device_windows
frame_id = frame_key
frame_identity = frame_key
correlate = analyze
reconcile = analyze
process_events = analyze
evaluate_scene = analyze
build_world = LinkBench.from_files


def choose_gateway(rows=None, job_id=None, **kwargs):
    """返回某笔请求当前最优的“网关 × 窗口 × 起点”，无可行点返回 None。"""
    report = analyze(rows, **kwargs)
    job = next((j for j in report["jobs"] if j["id"] == job_id), None) \
        if job_id else (report["jobs"][0] if report["jobs"] else None)
    return (job or {}).get("selected")


def schedule(rows=None, **kwargs):
    return analyze(rows, **kwargs)["schedule"]


def plan_all(rows=None, **kwargs):
    """不按当前时刻裁剪窗口的“全窗口理想排程”。

    与 :func:`analyze` 的运维视角不同：这里把所有接收窗（含已经过去的）都
    视作可排，回答“这笔请求当时最早应走哪个网关的哪个窗”。其 ``schedule``
    会包含 M1 -> tower-E@3070 这类历史可行预约。
    """
    kwargs = dict(kwargs)
    kwargs["now"] = -10 ** 15
    return analyze(rows, **kwargs)


def diagnose(rows=None, job_id=None, **kwargs):
    report = analyze(rows, **kwargs)
    if job_id is not None:
        return next((j for j in report["jobs"] if j["id"] == job_id), None)
    return {j["id"]: {k: j[k] for k in
                      ("stage", "reason_code", "reason_zh",
                       "recommendation", "recommendation_zh")}
            for j in report["jobs"]}


def format_text_report(report: dict) -> str:
    L = []
    L.append(f"场景时刻 now={report['now']}  设备/来源 {report['device_count']}  "
             f"观测 {report['observation_count']}  "
             f"滚动 {report['policy'].get('rolling_ms')}ms / "
             f"预算 {report['policy'].get('budget_ms')}ms  "
             f"候选范围={report.get('candidate_scope')}")
    L.append("\n== 网关无线资源与占空（独立核算，不合并） ==")
    for gw, view in report["resources"].items():
        for f, duty in view["duty"].items():
            L.append(f"  {gw} group={view['group']} f{f} "
                     f"当前窗已用 {duty['used_now_ms']:g}/"
                     f"{duty['budget_ms']}ms 峰值 {duty['peak']['peak_ms']:g}ms "
                     f"区间 {len(duty['intervals'])} 段")
        for p in view["protect_windows"]:
            L.append(f"  {gw} protect {p['interval']} ({p['label']})")
    L.append("\n== 请求链路与诊断 ==")
    for job in report["jobs"]:
        sel = job.get("selected") or {}
        if sel:
            end = sel.get("end", sel.get("plan_from"))
            sel_text = (f"{sel.get('gateway')}/f{sel.get('freq')} "
                        f"[{sel.get('start', sel.get('plan_from'))},{end}] "
                        f"{sel.get('duration_ms', '')}ms via {sel.get('basis')}")
        else:
            sel_text = "无可行无线资源"
        L.append(f"* {job['id']} [{job['class']}] {job['node']} "
                 f"join={job['join']} ctr={job['counter']} "
                 f"dl={job['deadline']} est={job['estimated_ms']}ms")
        L.append(f"    阶段={job['stage']}  选择={sel_text}")
        L.append(f"    原因={job['reason_code']}（{job['reason_zh']}）"
                 f" -> {job['recommendation']}（{job['recommendation_zh']}）"
                 + ("  [已过截止]" if job["expired"] else ""))
        if job.get("device_note"):
            L.append(f"    设备附言: {job['device_note']}")
        if job["chain"]["stale_heards"]:
            L.append("    旧周期迟到确认: " + ", ".join(
                f"join={h['join']}@{h['at']}"
                for h in job["chain"]["stale_heards"]))
    L.append("\n== 当前有效预约 ==")
    for p in report["schedule"]:
        L.append(f"  {p['job_id']} -> {p['gateway']}/f{p['freq']} "
                 f"W{p['window_index']} [{p['start']},{p['end']}] "
                 f"{p['duration_ms']:g}ms")
    return "\n".join(L)


def main(argv=None) -> int:
    cli = argparse.ArgumentParser(description="本地链路协调 / 联调模拟工具")
    cli.add_argument("--scene", type=Path)
    cli.add_argument("--devices", type=Path)
    cli.add_argument("--policy", type=Path)
    cli.add_argument("--now", type=float)
    cli.add_argument("--cadence", type=float, help="设备上传周期 ms")
    cli.add_argument("--scope", choices=["coverage", "receivers"],
                     default="coverage")
    cli.add_argument("--id", help="只输出某笔请求")
    cli.add_argument("--json", action="store_true")
    cli.add_argument("--command", help="追加网管命令 JSON")
    args = cli.parse_args(argv)

    bench = LinkBench(load_devices(args.devices), load_policy(args.policy),
                      now=args.now, uplink_period_ms=args.cadence,
                      candidate_scope=args.scope)
    bench.ingest_many(load_scene(args.scene))
    if args.now is not None:
        bench.now = args.now
    # 先把资料里的请求协调成基线，再投递命令，语义与 HTTP 服务一致。
    bench.solve()
    if args.command:
        bench.submit_command(json.loads(args.command))
    report = bench.report()
    if args.id:
        job = next((j for j in report["jobs"] if j["id"] == args.id), None)
        if job is None:
            print(f"找不到请求 {args.id}")
            return 2
        if args.json:
            print(json.dumps(job, ensure_ascii=False, indent=2))
        else:
            print(f"{job['id']}: {job['stage']} / {job['reason_code']} / "
                  f"{job['recommendation']}")
            for t in job["trace"]:
                print(f"  {t['step']}. {t['message']}")
        return 0
    if args.json:
        print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
    else:
        print(format_text_report(report))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
