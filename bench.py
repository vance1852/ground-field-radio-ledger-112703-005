"""本地设备协议回放台 + 链路协调业务接口。

既有回放契约保持不变：

* ``GET /devices``
* ``GET /events?after=0&limit=16``
* ``POST /commands`` 仍只返回 ``202`` 与 ``receipt="transport-only"``，
  不会改变 observations 记录。

在此之上挂载 :mod:`linkbench` 的协调 / 联调接口（均为只读分析或内存态，
同样不改 NDJSON 资料）：

* ``GET /report``                 整份场景的关联、资源、预约与诊断
* ``GET /jobs`` / ``/jobs?id=M1`` 业务请求状态 / 单笔详情
* ``GET /trace?id=M1``            从请求追到资源、窗口与耗时的轨迹
* ``GET /schedule``               当前协调出的无线预约
* ``GET /resources``              网关 × 频段滚动占空与 protect
* ``POST /requests``              网管投递新的取样请求并返回排程决定
* ``POST /ingest``                追加合成射频报告，推进办理
* ``POST /tick`` ``{"to": t}``    推进虚拟时钟并重新协调
"""

import argparse
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

from linkbench import LinkBench
from linkbench import analyze as analyze_scene
from linkbench import diagnose  # noqa: F401
from linkbench import device_windows  # noqa: F401
from linkbench import airtime_ms, frame_key  # noqa: F401
from linkbench import *  # noqa: F401,F403  再导出协调引擎的全部常用名称

# 便于 ``from bench import analyze / schedule / ...`` 形式调用。
analyze = analyze_scene


def schedule(rows=None, **kwargs):
    return analyze_scene(rows, **kwargs)["schedule"]

ROOT = Path(__file__).resolve().parent


def read(name):
    with (ROOT / name).open(encoding="utf-8") as source:
        return json.load(source)


def load_scene(path=None):
    path = Path(path) if path else ROOT / "observations.ndjson"
    with path.open(encoding="utf-8") as source:
        rows = [json.loads(line) for line in source if line.strip()]
    if [row["seq"] for row in rows] != list(range(1, len(rows) + 1)):
        raise ValueError("观测序号须连续")
    if any(not row["device"] or not row["kind"] for row in rows):
        raise ValueError("观测须有设备和事件类型")
    return rows


def _is_sampling_request(request: dict) -> bool:
    """判断 /commands 消息是否同时是需要协调的取样 / 抄表业务请求。"""
    if request.get("node"):
        return True
    if any(k in request for k in ("bytes", "class", "priority", "deadline")):
        return True
    action = str(request.get("action", "")).lower()
    if any(w in action for w in
           ("sample", "sampl", "read", "collect", "meter", "poll",
            "request", "取样", "采样", "抄表")):
        return True
    device = request.get("device")
    cfg = read("devices.json").get(device, {})
    return isinstance(cfg, dict) and "window_ms" in cfg


def make_server(rows, port=0, *, devices=None, policy=None, engine=None):
    if engine is None:
        engine = LinkBench(
            devices if devices is not None else read("devices.json"),
            policy if policy is not None else read("policy.json"))
        engine.ingest_many(rows)
        engine.solve()
    lock = threading.RLock()

    class Handler(BaseHTTPRequestHandler):
        server_version = "LinkBench/1.0"

        # -- 响应辅助 ------------------------------------------------------
        def output(self, status, contents):
            data = json.dumps(contents, ensure_ascii=False, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _body_json(self):
            length = int(self.headers.get("Content-Length", "0") or "0")
            if not 0 < length <= 1 << 20:
                raise ValueError("bad length")
            return json.loads(self.rfile.read(length))

        def _query(self):
            return parse_qs(urlsplit(self.path).query)

        def do_GET(self):
            url = urlsplit(self.path)
            path = url.path.rstrip("/") or "/"
            query = parse_qs(url.query)

            if path == "/devices":
                self.output(200, read("devices.json"))
                return

            if path == "/events":
                try:
                    after = int(query.get("after", ["0"])[0])
                    limit = int(query.get("limit", ["16"])[0])
                    if after < 0 or not 1 <= limit <= 100:
                        raise ValueError
                except ValueError:
                    self.output(400, {"error": "游标或数量超出范围"})
                    return
                with lock:
                    page = rows[after:after + limit]
                self.output(200, {"events": page,
                                  "cursor": page[-1]["seq"] if page else after})
                return

            # ---- 链路协调只读接口 ----
            if path in ("/report", "/api/report", "/analysis", "/api/analyze",
                        "/coordinate", "/simulate"):
                with lock:
                    self.output(200, engine.report())
                return
            if path in ("/decisions", "/verdicts", "/results", "/diagnoses"):
                with lock:
                    rep = engine.report()
                    self.output(200, rep["verdicts"])
                return
            if path in ("/state", "/api/state"):
                with lock:
                    rep = engine.report()
                    self.output(200, {
                        "now": rep["now"],
                        "schedule": rep["schedule"],
                        "jobs": [{k: j[k] for k in
                                  ("id", "stage", "reason_code",
                                   "recommendation")} for j in rep["jobs"]]})
                return
            if path in ("/jobs", "/job", "/api/jobs", "/diagnose"):
                jid = query.get("id", [None])[0]
                with lock:
                    if jid is not None:
                        view = next((engine.job_view(j) for j in engine.job_order
                                     if j == jid), None)
                        if view is None:
                            self.output(404, {"error": f"未知请求 {jid}"})
                        else:
                            self.output(200, view)
                        return
                    self.output(200, {"jobs": engine.states()})
                return
            if path in ("/trace", "/api/trace"):
                jid = query.get("id", [None])[0]
                with lock:
                    if not jid or jid not in engine.traces:
                        self.output(404, {"error": "需要已知 id 的轨迹"})
                        return
                    view = engine.job_view(jid)
                    self.output(200, {"id": jid, "selected": view["selected"],
                                      "stage": view["stage"],
                                      "reason_code": view["reason_code"],
                                      "recommendation": view["recommendation"],
                                      "trace": engine.traces[jid]})
                return
            if path in ("/schedule", "/api/schedule"):
                with lock:
                    self.output(200, {"schedule": list(engine.proposals.values())})
                return
            if path in ("/resources", "/budget", "/api/resources"):
                with lock:
                    self.output(200, engine.resource_report())
                return
            if path in ("/frames", "/api/frames"):
                with lock:
                    self.output(200, {"frames": engine.report()["frames"]})
                return
            if path in ("/health", "/healthz"):
                self.output(200, {"ok": True})
                return
            self.output(404, {"error": "路径不存在"})

        def do_POST(self):
            path = urlsplit(self.path).path.rstrip("/") or "/"

            # ---- 既有回放通道：严格保持 transport-only 契约 ----
            if path == "/commands":
                try:
                    length = int(self.headers["Content-Length"])
                    if not 0 < length <= 65536:
                        raise ValueError
                    request = json.loads(self.rfile.read(length))
                    if not isinstance(request, dict):
                        raise ValueError
                    if (request["device"] not in read("devices.json") or
                            not all(isinstance(request[k], str) and request[k]
                                    for k in ("id", "action"))):
                        raise ValueError
                except (ValueError, TypeError, KeyError):
                    self.output(400, {"error": "消息没有合法设备或请求标识"})
                    return
                # 若这是取样 / 抄表类业务请求，顺带纳入协调引擎；任何引擎异常
                # 都不影响 /commands 作为回放通道的 transport-only 回执契约。
                try:
                    if _is_sampling_request(request):
                        with lock:
                            engine.submit_command(request)
                            engine.solve()
                except Exception:  # noqa: BLE001 - 回执通道必须稳定
                    pass
                # 传输凭据契约：响应体保持恰好这三个字段。
                self.output(202, {"id": request["id"], "device": request["device"],
                                  "receipt": "transport-only"})
                return

            if path in ("/requests", "/api/requests", "/sample-requests",
                        "/dispatch", "/api/dispatch", "/sample"):
                try:
                    payload = self._body_json()
                    if not isinstance(payload, dict) or not payload.get("id"):
                        raise ValueError("需要 id")
                except (ValueError, TypeError, json.JSONDecodeError) as exc:
                    self.output(400, {"error": f"非法取样请求: {exc}"})
                    return
                with lock:
                    record = engine.submit_command(payload)
                    engine.solve()
                    jid = payload["id"]
                    self.output(202, {"record": record,
                                      "job": engine.job_view(jid)})
                return

            if path in ("/ingest", "/events", "/api/ingest", "/reports"):
                try:
                    payload = self._body_json()
                    new_rows = payload.get("events") if isinstance(payload, dict) \
                        and isinstance(payload.get("events"), list) else (
                        payload if isinstance(payload, list) else [payload])
                except (ValueError, json.JSONDecodeError) as exc:
                    self.output(400, {"error": f"非法射频报告: {exc}"})
                    return
                with lock:
                    accepted = []
                    for row in new_rows:
                        if not isinstance(row, dict) or not row.get("kind"):
                            continue
                        if not isinstance(row.get("seq"), int):
                            row = dict(row)
                            row["seq"] = engine.max_seq + 1
                        engine.ingest(row)
                        rows.append(row)
                        accepted.append(row.get("seq"))
                    engine.solve()
                    self.output(200, {"accepted": accepted,
                                      "now": engine.now,
                                      "jobs": [{k: j[k] for k in
                                                ("id", "stage", "reason_code",
                                                 "recommendation")}
                                               for j in engine.states()]})
                return

            if path in ("/tick", "/advance", "/api/tick"):
                try:
                    payload = self._body_json()
                except (ValueError, json.JSONDecodeError):
                    payload = {}
                with lock:
                    engine.advance(to=payload.get("to"), by=payload.get("by"))
                    if payload.get("pump"):
                        engine.pump_pending()
                    engine.solve()
                    self.output(200, {"now": engine.now, "jobs": engine.states()})
                return

            if path in ("/pump", "/api/pump"):
                with lock:
                    results = engine.pump_pending()
                    self.output(200, {"results": results,
                                      "schedule": list(engine.proposals.values())})
                return

            if path in ("/redispatch", "/api/redispatch"):
                try:
                    payload = self._body_json()
                    jid = payload["id"]
                except (ValueError, KeyError, json.JSONDecodeError):
                    self.output(400, {"error": "需要 {\"id\": ...}"})
                    return
                with lock:
                    decision = engine.redispatch(jid)
                    self.output(200, decision)
                return

            if path in ("/execute", "/api/execute", "/simulate-execution"):
                try:
                    payload = self._body_json()
                    jid = payload["id"]
                except (ValueError, KeyError, json.JSONDecodeError):
                    self.output(400, {"error": "需要 {\"id\": ...}"})
                    return
                with lock:
                    result = engine.execute_proposal(
                        jid, hear=bool(payload.get("hear", True)))
                    self.output(200, result)
                return

            self.output(404, {"error": "路径不存在"})

        def log_message(self, fmt, *args):
            pass

    httpd = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    # 方便进程内测试直接检查引擎状态。
    httpd.engine = engine
    httpd.linkbench = engine
    httpd.state_lock = lock
    return httpd


def main():
    cli = argparse.ArgumentParser(description="地面设备资料回放 / 链路协调台")
    cli.add_argument("--listen", type=int, help="侦听的本机端口")
    cli.add_argument("--scene", type=Path, help="其他符合协议的观测记录")
    cli.add_argument("--devices", type=Path, help="设备资料（区域 / 机型）")
    cli.add_argument("--policy", type=Path, help="业务与占空合同")
    cli.add_argument("--report", action="store_true",
                     help="直接输出链路协调文本报告")
    cli.add_argument("--json", action="store_true", help="配合 --report 输出 JSON")
    args = cli.parse_args()
    rows = load_scene(args.scene)

    if args.report:
        dev = json.loads(args.devices.read_text(encoding="utf-8")) \
            if args.devices else read("devices.json")
        pol = json.loads(args.policy.read_text(encoding="utf-8")) \
            if args.policy else read("policy.json")
        report = analyze_scene(rows, devices=dev, policy=pol)
        if args.json:
            print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
        else:
            from linkbench import format_text_report
            print(format_text_report(report))
        return

    if args.listen is None:
        print(f"读取 {len(read('devices.json'))} 个设备或来源和 {len(rows)} 条观测")
        return
    engine = LinkBench(
        json.loads(args.devices.read_text(encoding="utf-8"))
        if args.devices else read("devices.json"),
        json.loads(args.policy.read_text(encoding="utf-8"))
        if args.policy else read("policy.json"))
    engine.ingest_many(rows)
    engine.solve()
    with make_server(rows, args.listen, engine=engine) as server:
        print(f"监听 127.0.0.1:{server.server_port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass


if __name__ == "__main__":
    main()
