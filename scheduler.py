"""兼容入口：调度器。完整实现见 :mod:`linkbench`。"""
from linkbench import *  # noqa: F401,F403
import linkbench as _core

Scheduler = _core.Scheduler
LinkBench = _core.LinkBench
schedule = _core.schedule
analyze = _core.analyze
