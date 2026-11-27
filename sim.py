"""兼容入口：本地链路模拟器。完整实现见 :mod:`linkbench`。"""
from linkbench import *  # noqa: F401,F403
import linkbench as _core

LinkBench = _core.LinkBench
Simulator = _core.Simulator
analyze = _core.analyze
simulate = _core.simulate
run = _core.run
load_scene = _core.load_scene

# 常见动名词别名。
run_simulation = _core.analyze
simulate_scene = _core.analyze
build_world = _core.LinkBench.from_files
