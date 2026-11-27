"""兼容入口：链路协调器。完整实现见 :mod:`linkbench`。"""
from linkbench import *  # noqa: F401,F403
import linkbench as _core

LinkBench = _core.LinkBench
Coordinator = _core.Coordinator
Scheduler = _core.Scheduler
analyze = _core.analyze
coordinate = _core.coordinate
schedule = _core.schedule
diagnose = _core.diagnose
build_plan = _core.analyze
plan = _core.schedule
