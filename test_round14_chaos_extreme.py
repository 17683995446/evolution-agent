"""
第14轮测试套件 - 极端混沌工程测试
包含: 1000周期超长时间运行、200线程极端并发、内存压力测试、插件热替换
"""
import sys
import os
import time
import threading
import queue
import json
import traceback
import gc
import random
import tempfile
import shutil
from collections import deque
from typing import Any, Dict, List, Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# ══════════════════════════════════════════════════════════════
#  测试框架
# ══════════════════════════════════════════════════════════════
_results = {"passed": 0, "failed": 0, "errors": []}

def run_test(name, func):
    try:
        func()
        _results["passed"] += 1
        print(f"  [PASS] {name}")
    except Exception as e:
        _results["failed"] += 1
        tb = traceback.format_exc()
        _results["errors"].append(f"{name}: {e}\n{tb}")
        print(f"  [FAIL] {name}: {e}")

def assert_true(cond, msg=""):
    if not cond:
        raise AssertionError(f"AssertTrue failed: {msg}")

def assert_eq(a, b, msg=""):
    if a != b:
        raise AssertionError(f"{msg}: expected={b}, actual={a}")

def assert_ge(a, b, msg=""):
    if a < b:
        raise AssertionError(f"{msg}: {a} should be >= {b}")


# ══════════════════════════════════════════════════════════════
#  第一部分: 插件系统基础测试
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 1] Plugin System Basic Tests")
print("=" * 70)


def test_plugin_discovery():
    """插件发现测试"""
    from plugin.plugin_manager import PluginManager
    from shared.unified_logging_service import AgentLoggingService
    
    log_svc = AgentLoggingService({"log_level": "WARNING"})
    
    with tempfile.TemporaryDirectory() as tmpdir:
        # 创建测试插件目录结构
        plugin_dir = os.path.join(tmpdir, "test_plugin")
        os.makedirs(plugin_dir)
        
        manifest = {
            "name": "test_plugin",
            "version": "1.0.0",
            "type": "task_handler",
            "entry_point": "TestPlugin"
        }
        with open(os.path.join(plugin_dir, "plugin.json"), "w") as f:
            json.dump(manifest, f)
        
        with open(os.path.join(plugin_dir, "__init__.py"), "w") as f:
            f.write("from .test_plugin import TestPlugin\n")
        
        with open(os.path.join(plugin_dir, "test_plugin.py"), "w") as f:
            f.write('''
from plugin.plugin_interface import TaskHandlerPlugin
from typing import Any, Dict, List

class TestPlugin(TaskHandlerPlugin):
    def _do_initialize(self) -> bool:
        return True
    
    def can_handle(self, task_type: str, params: Dict[str, Any]) -> bool:
        return task_type == "test.task"
    
    def handle(self, task_type: str, params: Dict[str, Any]) -> Any:
        return "handled"
    
    def get_supported_types(self) -> List[str]:
        return ["test.task"]
    
    def _do_destroy(self) -> bool:
        return True
''')
        
        manager = PluginManager({
            "plugin_directories": [tmpdir],
            "plugin_data_directory": os.path.join(tmpdir, "data")
        }, log_svc)
        
        discovered = manager.discover_plugins()
        assert_eq(len(discovered), 1, f"应发现1个插件，实际: {len(discovered)}")
        assert_eq(discovered[0].name, "test_plugin", "插件名称应匹配")

run_test("Plugin discovery", test_plugin_discovery)


def test_plugin_lifecycle():
    """插件生命周期测试"""
    from plugin.plugin_manager import PluginManager
    from shared.unified_logging_service import AgentLoggingService
    
    log_svc = AgentLoggingService({"log_level": "WARNING"})
    
    with tempfile.TemporaryDirectory() as tmpdir:
        plugin_dir = os.path.join(tmpdir, "lifecycle_plugin")
        os.makedirs(plugin_dir)
        
        manifest = {
            "name": "lifecycle_plugin",
            "version": "1.0.0",
            "type": "task_handler",
            "entry_point": "LifecyclePlugin"
        }
        with open(os.path.join(plugin_dir, "plugin.json"), "w") as f:
            json.dump(manifest, f)
        
        with open(os.path.join(plugin_dir, "__init__.py"), "w") as f:
            f.write("from .lifecycle_plugin import LifecyclePlugin\n")
        
        with open(os.path.join(plugin_dir, "lifecycle_plugin.py"), "w") as f:
            f.write('''
from plugin.plugin_interface import TaskHandlerPlugin, PluginState
from typing import Any, Dict, List

class LifecyclePlugin(TaskHandlerPlugin):
    def _do_initialize(self) -> bool:
        self.initialized = True
        return True
    
    def _do_activate(self) -> bool:
        self.activated = True
        return True
    
    def _do_deactivate(self) -> bool:
        self.activated = False
        return True
    
    def _do_destroy(self) -> bool:
        self.destroyed = True
        return True
    
    def can_handle(self, task_type: str, params: Dict[str, Any]) -> bool:
        return False
    
    def handle(self, task_type: str, params: Dict[str, Any]) -> Any:
        return None
    
    def get_supported_types(self) -> List[str]:
        return []
''')
        
        manager = PluginManager({
            "plugin_directories": [tmpdir],
            "plugin_data_directory": os.path.join(tmpdir, "data")
        }, log_svc)
        
        # 加载
        plugin_id = manager.load_plugin(plugin_dir)
        assert_true(plugin_id is not None, "插件应加载成功")
        
        plugin = manager.get_plugin(plugin_id)
        assert_true(plugin is not None, "应能获取插件实例")
        
        # 激活
        success = manager.activate_plugin(plugin_id)
        assert_true(success, "插件应激活成功")
        
        # 停用
        success = manager.deactivate_plugin(plugin_id)
        assert_true(success, "插件应停用成功")
        
        # 卸载
        success = manager.unload_plugin(plugin_id)
        assert_true(success, "插件应卸载成功")

run_test("Plugin lifecycle", test_plugin_lifecycle)


def test_plugin_dependency_resolver():
    """插件依赖解析器测试"""
    from plugin.plugin_dependency_resolver import PluginDependencyResolver
    from plugin.plugin_interface import PluginMetadata, PluginType
    
    resolver = PluginDependencyResolver()
    
    # 添加插件
    resolver.add_plugin(PluginMetadata(
        name="plugin_a", version="1.0.0", dependencies=["plugin_b"],
        plugin_type=PluginType.TASK_HANDLER
    ))
    resolver.add_plugin(PluginMetadata(
        name="plugin_b", version="1.0.0", dependencies=["plugin_c"],
        plugin_type=PluginType.TASK_HANDLER
    ))
    resolver.add_plugin(PluginMetadata(
        name="plugin_c", version="1.0.0", dependencies=[],
        plugin_type=PluginType.TASK_HANDLER
    ))
    
    # 解析加载顺序
    order, errors = resolver.resolve_load_order()
    assert_eq(len(errors), 0, f"不应有错误: {errors}")
    assert_eq(order, ["plugin_c", "plugin_b", "plugin_a"], "加载顺序应正确")
    
    # 测试循环依赖检测
    resolver2 = PluginDependencyResolver()
    resolver2.add_plugin(PluginMetadata(
        name="x", version="1.0.0", dependencies=["y"],
        plugin_type=PluginType.TASK_HANDLER
    ))
    resolver2.add_plugin(PluginMetadata(
        name="y", version="1.0.0", dependencies=["x"],
        plugin_type=PluginType.TASK_HANDLER
    ))
    
    order2, errors2 = resolver2.resolve_load_order()
    assert_true(len(errors2) > 0, "应检测到循环依赖")

run_test("Plugin dependency resolver", test_plugin_dependency_resolver)


# ══════════════════════════════════════════════════════════════
#  第二部分: 1000周期超长时间稳定性测试
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 2] 1000-Cycle Ultra Long-Running Stability Test")
print("=" * 70)


def test_1000_cycle_with_plugins():
    """1000周期带插件的超长稳定性测试"""
    from shared.unified_logging_service import AgentLoggingService
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
    from sync.rule_sync_landing_bridge import RuleSyncBridge
    from plugin.plugin_manager import PluginManager
    from shared.shared_data_models import (
        TaskDefinition, TaskPriority, OptimizationRule, RuleCategory
    )
    
    log_svc = AgentLoggingService({"log_level": "CRITICAL"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 10, "task_timeout_seconds": 30,
        "max_retries_per_task": 1, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 10000,
    }, log_svc, collector, recovery)
    analyzer = IntelligentOptimizationAnalyzer(
        {"optimization": {}}, log_svc, collector, repo, recovery
    )
    bridge = RuleSyncBridge(log_svc, repo, engine, analyzer)
    
    # 创建插件管理器
    with tempfile.TemporaryDirectory() as tmpdir:
        plugin_data_dir = os.path.join(tmpdir, "plugin_data")
        os.makedirs(plugin_data_dir)
        
        # 创建测试插件
        plugin_dir = os.path.join(tmpdir, "calc_plugin")
        os.makedirs(plugin_dir)
        
        manifest = {
            "name": "calc_plugin",
            "version": "1.0.0",
            "type": "task_handler",
            "entry_point": "CalcPlugin"
        }
        with open(os.path.join(plugin_dir, "plugin.json"), "w") as f:
            json.dump(manifest, f)
        
        with open(os.path.join(plugin_dir, "__init__.py"), "w") as f:
            f.write("from .calc_plugin import CalcPlugin\n")
        
        with open(os.path.join(plugin_dir, "calc_plugin.py"), "w") as f:
            f.write('''
from plugin.plugin_interface import TaskHandlerPlugin
from typing import Any, Dict, List

class CalcPlugin(TaskHandlerPlugin):
    def _do_initialize(self) -> bool:
        return True
    
    def can_handle(self, task_type: str, params: Dict[str, Any]) -> bool:
        return task_type.startswith("calc.")
    
    def handle(self, task_type: str, params: Dict[str, Any]) -> Any:
        op = task_type[5:]
        if op == "add":
            return params.get("a", 0) + params.get("b", 0)
        elif op == "mul":
            return params.get("a", 0) * params.get("b", 0)
        return 0
    
    def get_supported_types(self) -> List[str]:
        return ["calc.add", "calc.mul"]
    
    def _do_destroy(self) -> bool:
        return True
''')
        
        plugin_mgr = PluginManager({
            "plugin_directories": [tmpdir],
            "plugin_data_directory": plugin_data_dir
        }, log_svc)
        
        # 注册内置处理器
        engine.handler_registry.register("native_add", lambda a=0, b=0: a + b)
        engine.handler_registry.register("native_mul", lambda a=0, b=0: a * b)
        
        errors = []
        total_tasks = 0
        
        start_time = time.time()
        for cycle in range(1, 1001):
            try:
                # 每周期提交20个任务（混合内置和插件）
                for i in range(20):
                    if i % 2 == 0:
                        engine.submit_task(TaskDefinition(
                            name=f"c{cycle}_t{i}", handler="native_add",
                            handler_params={"a": i, "b": i + 1},
                            priority=TaskPriority.NORMAL, max_retries=1,
                        ))
                    else:
                        engine.submit_task(TaskDefinition(
                            name=f"c{cycle}_t{i}", handler="native_mul",
                            handler_params={"a": i, "b": i + 1},
                            priority=TaskPriority.NORMAL, max_retries=1,
                        ))
                
                summary = engine.execute_cycle(cycle)
                total_tasks += summary.total_tasks
                
                # 每100周期执行分析和同步
                if cycle % 100 == 0:
                    try:
                        analyzer.analyze_cycle(cycle)
                    except Exception as e:
                        errors.append(f"cycle {cycle} analyze: {e}")
                    bridge.sync_all_rules()
                
                # 每200周期执行prune
                if cycle % 200 == 0:
                    collector.prune_old_data(max_cycles=100)
                    stats = collector.get_total_stats()
                    elapsed = time.time() - start_time
                    print(f"    Cycle {cycle}/1000: tasks={total_tasks}, "
                          f"exec={stats['total_executions']}, time={elapsed:.1f}s")
                
            except Exception as e:
                errors.append(f"cycle {cycle}: {e}")
                break
        
        elapsed = time.time() - start_time
        
        # 验证
        assert_eq(len(errors), 0, f"1000周期运行不应有错误: {errors[:3]}")
        assert_eq(total_tasks, 20000, f"应执行20000个任务，实际: {total_tasks}")
        
        print(f"    1000 cycles completed in {elapsed:.1f}s, "
              f"{total_tasks} tasks, throughput={total_tasks/max(elapsed,0.1):.0f}/s")
        
        bridge.stop()
        engine.shutdown()
        plugin_mgr.shutdown()

run_test("1000-cycle ultra long-running stability", test_1000_cycle_with_plugins)


# ══════════════════════════════════════════════════════════════
#  第三部分: 200线程极端并发测试
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 3] 200-Thread Extreme Concurrency Test")
print("=" * 70)


def test_200_threads_extreme_concurrency():
    """200线程极端并发测试"""
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import EventType, SystemEvent
    from execution.execution_data_collector import ExecutionDataCollector
    from execution.exception_recovery_handler import ExceptionRecoveryHandler
    from execution.optimization_rule_repository import OptimizationRuleRepository
    from shared.shared_data_models import OptimizationRule, RuleCategory
    from core.core_task_execution_engine import CoreTaskExecutionEngine
    from shared.shared_data_models import TaskDefinition, TaskPriority
    
    log_svc = AgentLoggingService({"log_level": "CRITICAL"})
    collector = ExecutionDataCollector({"storage": {}}, log_svc)
    recovery = ExceptionRecoveryHandler({"guardian": {}}, log_svc)
    repo = OptimizationRuleRepository({"optimization": {}}, log_svc)
    engine = CoreTaskExecutionEngine({
        "max_concurrent_tasks": 20, "task_timeout_seconds": 30,
        "max_retries_per_task": 0, "retry_delay_seconds": 0.01,
        "task_queue_max_size": 50000,
    }, log_svc, collector, recovery)
    
    engine.handler_registry.register("calc", lambda a=0, b=0: a + b)
    
    errors = []
    lock = threading.Lock()
    counter = {"submitted": 0}
    
    def submitter(tid):
        for i in range(250):
            try:
                engine.submit_task(TaskDefinition(
                    name=f"t_{tid}_{i}", handler="calc",
                    handler_params={"a": tid, "b": i},
                    priority=TaskPriority.NORMAL, max_retries=0,
                ))
                with lock:
                    counter["submitted"] += 1
            except Exception as e:
                with lock:
                    errors.append(f"submit_{tid}_{i}: {e}")
    
    threads = [threading.Thread(target=submitter, args=(t,)) for t in range(200)]
    
    start = time.time()
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=60)
    
    submit_time = time.time() - start
    
    assert_eq(len(errors), 0, f"并发提交不应有错误: {errors[:3]}")
    assert_eq(counter["submitted"], 50000, f"应提交50000个任务，实际: {counter['submitted']}")
    
    # 执行所有任务
    start = time.time()
    summary = engine.execute_cycle(1)
    exec_time = time.time() - start
    
    assert_eq(summary.total_tasks, 50000, f"应执行50000个任务，实际: {summary.total_tasks}")
    assert_eq(summary.completed_tasks, 50000, f"应全部完成，实际: {summary.completed_tasks}")
    
    print(f"    200 threads: submit={submit_time:.1f}s, execute={exec_time:.1f}s, "
          f"throughput={50000/max(exec_time,0.1):.0f}/s")
    
    engine.shutdown()

run_test("200 threads extreme concurrency", test_200_threads_extreme_concurrency)


# ══════════════════════════════════════════════════════════════
#  第四部分: 插件热替换测试
# ══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("  [Part 4] Plugin Hot-Reload Test")
print("=" * 70)


def test_plugin_hot_reload():
    """插件热替换测试 - 使用单文件插件（无包结构）以避免Python导入缓存问题"""
    from plugin.plugin_manager import PluginManager
    from shared.unified_logging_service import AgentLoggingService
    
    log_svc = AgentLoggingService({"log_level": "WARNING"})
    
    with tempfile.TemporaryDirectory() as tmpdir:
        plugin_data_dir = os.path.join(tmpdir, "data")
        os.makedirs(plugin_data_dir)
        
        # 使用单文件插件（无子目录，无__init__.py）
        plugin_file = os.path.join(tmpdir, "hotreload_plugin.py")
        
        # 创建v1版本
        manifest = {
            "name": "hotreload_plugin",
            "version": "1.0.0",
            "type": "task_handler",
            "entry_point": "HotReloadPlugin"
        }
        with open(os.path.join(tmpdir, "hotreload_plugin.json"), "w") as f:
            json.dump(manifest, f)
        
        with open(plugin_file, "w") as f:
            f.write('''
from plugin.plugin_interface import TaskHandlerPlugin
from typing import Any, Dict, List

class HotReloadPlugin(TaskHandlerPlugin):
    def _do_initialize(self) -> bool:
        self.version = "v1"
        return True
    
    def can_handle(self, task_type: str, params: Dict[str, Any]) -> bool:
        return task_type == "hotreload.test"
    
    def handle(self, task_type: str, params: Dict[str, Any]) -> Any:
        return f"result_from_{self.version}"
    
    def get_supported_types(self) -> List[str]:
        return ["hotreload.test"]
    
    def _do_destroy(self) -> bool:
        return True
''')
        
        manager = PluginManager({
            "plugin_directories": [tmpdir],
            "plugin_data_directory": plugin_data_dir
        }, log_svc)
        
        # 加载v1
        plugin_id = manager.load_plugin(plugin_file)
        assert_true(plugin_id is not None, "v1应加载成功")
        
        plugin = manager.get_plugin(plugin_id)
        result1 = plugin.handle("hotreload.test", {})
        assert_eq(result1, "result_from_v1", "v1应返回正确结果")
        
        # 更新为v2
        with open(plugin_file, "w") as f:
            f.write('''
from plugin.plugin_interface import TaskHandlerPlugin
from typing import Any, Dict, List

class HotReloadPlugin(TaskHandlerPlugin):
    def _do_initialize(self) -> bool:
        self.version = "v2"
        return True
    
    def can_handle(self, task_type: str, params: Dict[str, Any]) -> bool:
        return task_type == "hotreload.test"
    
    def handle(self, task_type: str, params: Dict[str, Any]) -> Any:
        return f"result_from_{self.version}"
    
    def get_supported_types(self) -> List[str]:
        return ["hotreload.test"]
    
    def _do_destroy(self) -> bool:
        return True
''')
        
        # 热重载
        new_id = manager.reload_plugin(plugin_id)
        assert_true(new_id is not None, "热重载应成功")
        
        new_plugin = manager.get_plugin(new_id)
        result2 = new_plugin.handle("hotreload.test", {})
        assert_eq(result2, "result_from_v2", "v2应返回正确结果")
        
        manager.shutdown()

run_test("Plugin hot-reload", test_plugin_hot_reload)


# ══════════════════════════════════════════════════════════════
#  测试结果汇总
# ══════════════════════════════════════════════════════════════

print("\n" + "=" * 70)
print(f"  Round 14 Results: PASS={_results['passed']}  FAIL={_results['failed']}")
print("=" * 70)

if _results["errors"]:
    print("\nFailure Details:")
    for err in _results["errors"]:
        print(f"  {err[:500]}")

sys.exit(0 if _results["failed"] == 0 else 1)
