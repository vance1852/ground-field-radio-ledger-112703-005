"""保证无论 unittest discover 还是 pytest，都能导入仓库根目录模块。"""
import os
import sys

_ROOT = os.path.dirname(os.path.abspath(__file__))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)
