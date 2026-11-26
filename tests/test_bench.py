"""检查本机设备回放与资料。"""

import json
import threading
import unittest
from urllib.request import Request, urlopen

from bench import load_scene, make_server, read


class BenchTest(unittest.TestCase):
    def test_device_exchange(self):
        rows = load_scene()
        devices = read("devices.json")
        self.assertTrue({row["device"] for row in rows} <= set(devices))
        with make_server(rows, 0) as server:
            worker = threading.Thread(target=server.serve_forever)
            worker.start()
            try:
                url = f"http://127.0.0.1:{server.server_port}"
                with urlopen(url + "/events?after=1&limit=2", timeout=4) as reply:
                    self.assertEqual(json.load(reply), {"events": rows[1:3], "cursor": 3})
                with urlopen(url + "/devices", timeout=4) as reply:
                    self.assertEqual(json.load(reply), devices)
                message = json.dumps({"device": rows[0]["device"], "id": "loopback-17", "action": "probe"}).encode()
                with urlopen(Request(url + "/commands", data=message), timeout=4) as reply:
                    self.assertEqual(reply.status, 202)
                    self.assertEqual(json.load(reply)["receipt"], "transport-only")
            finally:
                server.shutdown()
                worker.join()

    def test_material_links(self):
        rows = load_scene()
        by_seq = {row["seq"]: row for row in rows}
        for row in rows:
            for earlier in row.get("refs", []):
                self.assertLess(earlier, row["seq"])
                self.assertIn(earlier, by_seq)
        material_contract(self, rows)


def material_contract(self, rows):
    self.assertEqual((rows[3]["join"], rows[3]["counter"]), (rows[4]["join"], rows[4]["counter"]))
    self.assertNotEqual(rows[3]["join"], rows[14]["join"])
    self.assertTrue(rows[2]["from"] < rows[11]["until"] and rows[11]["from"] < rows[2]["until"])
    self.assertNotEqual(rows[12]["join"], rows[13]["join"])
