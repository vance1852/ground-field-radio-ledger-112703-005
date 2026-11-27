"""链路协调引擎的验收测试：锁定资料中精心构造的数值与规则。"""

import json
import threading
import unittest
from urllib.request import Request, urlopen

from linkbench import (
    LinkBench,
    airtime_ms,
    analyze,
    device_windows,
    frame_key,
    load_devices,
    load_policy,
    load_scene,
    overlap_ms,
    peak_overlap,
)
from bench import make_server


class SceneOracleTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = load_scene()
        cls.report = analyze(cls.rows)
        cls.jobs = {j["id"]: j for j in cls.report["jobs"]}

    def test_airtime_contract_values(self):
        pol = load_policy()
        self.assertEqual(airtime_ms(9, pol), 61)
        self.assertEqual(airtime_ms(23, pol), 117)
        self.assertEqual(airtime_ms(11, pol), 69)
        self.assertEqual(airtime_ms(19, pol), 101)

    def test_frame_identity_includes_join(self):
        # seq4/5 同一物理帧被 E/W 两网关收见
        f1 = frame_key("meter", 4, 17)
        self.assertEqual(f1, frame_key("meter", 4, 17))
        # 不同入网周期 counter 相同也必须是不同帧
        self.assertNotEqual(f1, frame_key("meter", 5, 17))
        frames = {f["frame_key"]: f for f in self.report["frames"]}
        m1 = frames["meter|4|17"]
        gws = sorted(r["gateway"] for r in m1["receptions"])
        self.assertEqual(gws, ["tower-E", "tower-W"])
        # 共 5 个物理帧（双收见合并，classify 不产生新帧）
        self.assertEqual(len(self.report["frames"]), 5)

    def test_window_deadline_truncation(self):
        dev = load_devices()
        # M2: 第二窗被 deadline 7100 截成 50ms < 69
        wins = device_windows(5050, dev["meter"], 7100)
        self.assertEqual([w["start"] for w in wins], [6050, 7050])
        self.assertEqual(wins[1]["usable_end"], 7100)
        self.assertEqual(wins[1]["usable_length"], 50)
        self.assertLess(wins[1]["usable_length"], 69)
        # U3: 第二窗被 7150 截成 90ms < 101
        wins = device_windows(5060, dev["alarmer"], 7150)
        self.assertEqual(wins[1]["usable_length"], 90)

    def test_independent_duty_ledgers(self):
        res = self.report["resources"]
        self.assertEqual(res["tower-E"]["duty"]["1"]["real_used_now_ms"], 180)
        self.assertEqual(res["tower-W"]["duty"]["1"]["real_used_now_ms"], 150)
        self.assertEqual(res["tower-E"]["duty"]["2"]["real_used_now_ms"], 0)
        # E、W 是两份独立预算，不能出现合并总数 330 之类的键
        self.assertIn("group", res["tower-E"])
        self.assertIn("1", res["tower-W"]["duty"])

    def test_u1_full_chain_and_stale_heard(self):
        u1 = self.jobs["U1"]
        self.assertEqual(u1["stage"], "heard")
        sel = u1["selected"]
        self.assertEqual(sel["gateway"], "tower-W")
        self.assertEqual(sel["freq"], 1)
        self.assertEqual(sel["basis"], "tx_air")
        self.assertEqual(sel["duration_ms"], 110)  # 实占以 tx_air 为准，不是估计117
        # 只有 join=8 的 heard 确认当前请求
        self.assertEqual(len(u1["chain"]["heards"]), 1)
        self.assertEqual(u1["chain"]["heards"][0]["join"], 8)
        stale = u1["chain"]["stale_heards"]
        self.assertEqual(len(stale), 1)
        self.assertEqual(stale[0]["join"], 7)
        # 设备收到但 self-test 仍 pending，截止后无最终结果
        self.assertEqual(u1["device_note"], "self-test-pending")
        self.assertEqual(u1["reason_code"], "device_silent_after_heard")
        self.assertEqual(u1["recommendation"], "recollect")

    def test_m1_not_transmitted_with_feasible_slot(self):
        m1 = self.jobs["M1"]
        self.assertEqual(m1["reason_code"], "not_transmitted")
        self.assertEqual(m1["recommendation"], "recollect")
        best = m1["selected"]
        self.assertEqual(best["gateway"], "tower-E")
        self.assertEqual(best["start"], 3070)   # 避开 protect 3030-3070
        self.assertEqual(best["end"], 3131)     # 61ms，180+61=241<=250

    def test_u2_duty_budget_short_by_one(self):
        u2 = self.jobs["U2"]
        self.assertEqual(u2["reason_code"], "duty_budget")
        self.assertEqual(u2["recommendation"], "recollect")
        # W: 40(prev) + 110(U1 实际) + 101 = 251 > 250
        codes = {(c["gateway"], c["reason_code"]) for c in u2["feasibility"]}
        self.assertIn(("tower-W", "duty_budget"), codes)
        self.assertIn(("tower-E", "duty_budget"), codes)

    def test_u3_uses_freq2_only_e(self):
        u3 = self.jobs["U3"]
        self.assertEqual(u3["stage"], "scheduled")
        p = u3["proposal"]
        self.assertEqual(p["gateway"], "tower-E")
        self.assertEqual(p["freq"], 2)
        self.assertEqual((p["start"], p["end"]), (6060, 6161))

    def test_m2_rehomed_to_w_after_preemption(self):
        m2 = self.jobs["M2"]
        p = m2["proposal"]
        self.assertEqual(p["gateway"], "tower-W")
        self.assertEqual((p["start"], p["end"]), (6050, 6119))
        # 轨迹保留“先选 E（249/250 刀刃）再被 U3 腾挪到 W”
        messages = [t["message"] for t in m2["trace"]]
        self.assertTrue(any("重新安排" in m for m in messages))
        commits = [t for t in m2["trace"] if "形成未生效" in t["message"]]
        gateways = [t.get("gateway") for t in commits]
        self.assertEqual(gateways[:2], ["tower-E", "tower-W"])
        u3 = self.jobs["U3"]
        self.assertTrue(any("腾挪" in t["message"] for t in u3["trace"]))

    def test_report_is_json_serializable(self):
        json.dumps(self.report, ensure_ascii=False)


class RollingBudgetTest(unittest.TestCase):
    def test_overlap_half_open(self):
        self.assertEqual(overlap_ms(0, 10, 5, 15), 5)
        self.assertEqual(overlap_ms(0, 10, 10, 20), 0)
        self.assertEqual(overlap_ms(0, 10, -5, 5), 5)

    def test_peak_breakpoints(self):
        peak = peak_overlap([(0, 100), (50, 150)], 10000)
        self.assertEqual(peak["peak_ms"], 200)

    def test_aging_can_relieve_budget(self):
        from linkbench import DutyLedger
        d = DutyLedger(250, 10000)
        d.add("g", 1, 600, 780, owner="old")  # 180
        self.assertFalse(d.fits_ranges("g", 1, [], (3000, 3071))["fits"])  # 251
        # 与旧区间拉开足够距离后，两个峰值不再叠加
        self.assertTrue(d.fits_ranges("g", 1, [], (11000, 11071))["fits"])


class StatefulRuleTest(unittest.TestCase):
    def _bench(self):
        return LinkBench(load_devices(), load_policy()).ingest_many(load_scene())

    def test_duplicate_reception_does_not_duplicate_dispatch(self):
        b = self._bench()
        b.solve()
        self.assertIn("M2", b.proposals)
        # 同一帧的额外网关收见（重复上报）
        before = len(b.proposals)
        b.ingest({"seq": 21, "device": "tower-E", "kind": "uplink",
                  "at": 5050, "node": "meter", "join": 5, "counter": 17, "freq": 1})
        b.solve()
        self.assertEqual(len(b.proposals), before)
        self.assertEqual([p["job_id"] for p in b.proposals.values()].count("M2"), 1)

    def test_reject_allows_redispatch(self):
        b = self._bench()
        # 构造一个全新的、窗口很远的可排请求
        b.submit_command({"device": "meter", "id": "R1", "action": "sample",
                          "bytes": 5, "deadline": 9000, "at": 5050})
        self.assertIn("R1", b.proposals)
        b.ingest({"seq": 100, "device": "tower-W", "kind": "tx_reject",
                  "id": "R1", "reason": "duty-conflict"})
        self.assertNotIn("R1", b.proposals)
        view = b.job_view("R1")
        self.assertEqual(view["reason_code"], "rejected_by_gateway")
        self.assertEqual(view["recommendation"], "redispatch")
        decision = b.redispatch("R1")
        self.assertTrue(decision["scheduled"])

    def test_started_transmission_not_removable(self):
        # 基线排程让 M2 先持有 W 的未生效预约
        b = self._bench()
        b.solve()
        self.assertEqual(b.proposals["M2"]["gateway"], "tower-W")
        # 观测到 M2 已开始发射：预约转为不可撤的 fixed busy + 事实占空
        b.ingest({"seq": 30, "device": "tower-W", "kind": "tx_begin",
                  "id": "M2", "from": 6050, "freq": 1})
        self.assertNotIn("M2", b.proposals)
        self.assertTrue(any(x["job_id"] == "M2" and x["kind"] == "tx_begin"
                            for x in b.env.fixed_busy["tower-W"]))

        key = frame_key("meter", "x", 1)
        b.frames[key] = {"key": key, "node": "meter", "join": "x",
                         "counter": 1, "at": 5050, "freq": 1, "receptions": []}
        urgent = {"id": "UX", "rank": 0, "class": "urgent", "deadline": 6200,
                  "estimated_ms": 100, "freq": 1, "node": "meter",
                  "join": "x", "counter": 1, "frame_key": key}
        b.jobs["UX"] = urgent
        b.job_order.append("UX")
        d = b.schedule_job("UX")
        # 绝不能通过腾挪 / 撤销已开始的 M2 来安排 UX
        self.assertNotIn("M2", d.get("preempted", []))
        self.assertTrue(any(x["job_id"] == "M2" and x["kind"] == "tx_begin"
                            for x in b.env.fixed_busy["tower-W"]))
        # 已开始发射不允许被高优标记为可腾挪
        fake_effective = {"gateway": "tower-W", "effective": True}
        b.proposals["M2"] = fake_effective
        self.assertFalse(b._preempt_allowed(urgent, "M2"))
        b.proposals.pop("M2", None)

    def test_quota_pick_every_third_normal(self):
        dev = {"n": {"window_ms": [100], "span_ms": 100000},
               "g": {"freqs": [1], "group": "G", "duplex": "half"}}
        pol = {"rolling_ms": 100000, "budget_ms": 10 ** 9,
               "base_ms": 0, "per_byte_ms": 1, "switch_ms": 0}
        b = LinkBench(dev, pol, now=0)
        ids = ["uA", "uB", "uC", "n1"]
        for i, jid in enumerate(ids):
            rank = 1 if jid == "n1" else 0
            key = frame_key("n", jid, 1)
            b.frames[key] = {"key": key, "node": "n", "join": jid,
                             "counter": 1, "at": 0, "freq": 1, "receptions": []}
            b.jobs[jid] = {"id": jid, "rank": rank,
                           "class": "normal" if rank else "urgent",
                           "deadline": 10 ** 9, "seq": i, "node": "n",
                           "bytes": 10, "estimated_ms": 10, "freq": 1,
                           "frame_key": key}
            b.job_order.append(jid)
        picked = []
        for i in range(3):
            pick = b.quota_pick([j for j in b.job_order if j not in b.proposals])
            picked.append(pick)
            b.grant_log.append({"job_id": pick, "rank": b.jobs[pick]["rank"]})
            b.proposals[pick] = {"gateway": "g", "freq": 1,
                                 "start": i * 100, "end": i * 100 + 10}
        self.assertEqual(picked, ["uA", "uB", "n1"])

    def test_command_idempotent(self):
        b = self._bench()
        r1 = b.submit_command({"device": "meter", "id": "D1",
                               "action": "sample", "bytes": 3})
        r2 = b.submit_command({"device": "meter", "id": "D1",
                               "action": "sample", "bytes": 3})
        self.assertFalse(r1.get("duplicate"))
        self.assertTrue(r2.get("duplicate"))

    def test_execute_proposal_closes_three_stage_loop(self):
        b = self._bench()
        b.submit_command({"device": "meter", "id": "LOOP", "action": "sample",
                          "bytes": 1, "deadline": 9000, "at": 5050})
        self.assertIn("LOOP", b.proposals)
        result = b.execute_proposal("LOOP")
        job = result["job"]
        self.assertTrue(result["executed"])
        self.assertTrue(job["chain"]["queued"])
        self.assertTrue(job["chain"]["begin"])
        self.assertTrue(job["chain"]["air"])
        self.assertEqual(len(job["chain"]["heards"]), 1)
        self.assertEqual(job["stage"], "heard")
        self.assertEqual(job["recommendation"], "await_report")
        # 执行后预约已转为事实链，不能再被当作未生效预约腾挪 / 重复执行
        self.assertNotIn("LOOP", b.proposals)
        self.assertFalse(b.execute_proposal("LOOP")["executed"])


class HttpApiTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rows = load_scene()
        cls.server = make_server(cls.rows, 0)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join(timeout=3)

    def _get(self, path):
        with urlopen(self.base + path, timeout=5) as reply:
            return reply.status, json.loads(reply.read().decode())

    def _post(self, path, payload):
        req = Request(self.base + path,
                      data=json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(req, timeout=5) as reply:
            return reply.status, json.loads(reply.read().decode())

    def test_report_endpoint(self):
        status, body = self._get("/report")
        self.assertEqual(status, 200)
        ids = {j["id"] for j in body["jobs"]}
        self.assertEqual(ids, {"M1", "U1", "M2", "U2", "U3"})

    def test_job_and_trace(self):
        _, u2 = self._get("/jobs?id=U2")
        self.assertEqual(u2["reason_code"], "duty_budget")
        _, trace = self._get("/trace?id=U3")
        self.assertTrue(trace["trace"])
        self.assertEqual(trace["selected"]["gateway"], "tower-E")

    def test_commands_remains_transport_only(self):
        status, body = self._post("/commands", {
            "device": "tower-E", "id": "loopback-99", "action": "probe"})
        self.assertEqual(status, 202)
        self.assertEqual(body, {"id": "loopback-99", "device": "tower-E",
                                "receipt": "transport-only"})

    def test_rich_request_flow(self):
        status, body = self._post("/requests", {
            "device": "meter", "id": "HTTP1", "action": "sample",
            "bytes": 1, "class": "normal", "deadline": 9000, "at": 5050})
        self.assertEqual(status, 202)
        self.assertIn("job", body)
        self.assertTrue(body["job"]["proposal"])
        _, job = self._get("/jobs?id=HTTP1")
        self.assertEqual(job["stage"], "scheduled")

    def test_ingest_and_tick(self):
        status, body = self._post("/ingest", {"kind": "protect", "device": "tower-E",
                                              "from": 9000, "until": 9040, "freq": 1})
        self.assertEqual(status, 200)
        self.assertTrue(body["accepted"])
        status, body = self._post("/tick", {"to": 6000})
        self.assertEqual(status, 200)
        self.assertEqual(body["now"], 6000)


if __name__ == "__main__":
    unittest.main()
