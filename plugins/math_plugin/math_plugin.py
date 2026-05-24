"""
数学计算插件示例
"""
import math
from typing import Any, Dict, List
from ...plugin.plugin_interface import TaskHandlerPlugin, PluginContext


class MathPlugin(TaskHandlerPlugin):
    """数学计算任务处理器插件"""
    
    def _do_initialize(self) -> bool:
        """初始化插件"""
        self._precision = self.context.config.get("precision", 2) if self.context else 2
        self._supported_ops = ["add", "subtract", "multiply", "divide", "power", "sqrt", "sin", "cos"]
        return True
    
    def can_handle(self, task_type: str, params: Dict[str, Any]) -> bool:
        """判断是否能处理该任务"""
        return task_type.startswith("math.") and task_type[5:] in self._supported_ops
    
    def handle(self, task_type: str, params: Dict[str, Any]) -> Any:
        """执行数学计算"""
        op = task_type[5:]  # 去掉 "math." 前缀
        
        if op == "add":
            return params.get("a", 0) + params.get("b", 0)
        elif op == "subtract":
            return params.get("a", 0) - params.get("b", 0)
        elif op == "multiply":
            return params.get("a", 0) * params.get("b", 0)
        elif op == "divide":
            b = params.get("b", 1)
            if b == 0:
                raise ValueError("Division by zero")
            return round(params.get("a", 0) / b, self._precision)
        elif op == "power":
            return params.get("a", 0) ** params.get("b", 1)
        elif op == "sqrt":
            return round(math.sqrt(params.get("x", 0)), self._precision)
        elif op == "sin":
            return round(math.sin(params.get("x", 0)), self._precision)
        elif op == "cos":
            return round(math.cos(params.get("x", 0)), self._precision)
        else:
            raise ValueError(f"Unknown operation: {op}")
    
    def get_supported_types(self) -> List[str]:
        """返回支持的任务类型"""
        return [f"math.{op}" for op in self._supported_ops]
    
    def _do_destroy(self) -> bool:
        """销毁插件"""
        return True
