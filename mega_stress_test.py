"""
持续进化Agent - 超大规模极限测试
测试目标：
- 1000+ 周期连续运行
- 100,000+ 任务处理
- 内存泄漏检测
- 性能衰减监控
- 极端混乱场景
"""

from __future__ import annotations

import gc
import os
import psutil
import random
import sys
import tempfile
import threading
import time
import traceback
from pathlib import Path
from typing import Dict, List, Optional

sys.path.insert(0, str(Path(__file__).parent))

from shared.global_configuration_loader import AgentConfig
from shared.shared_data_models import (
    ComponentHealth, CycleSummary, ExecutionMetric, EventType,
    HealthStatus, OptimizationRule, RuleCategory, SystemEvent,
    TaskDefinition, TaskPriority, TaskResult, TaskStatus,
)
from shared.unified_logging_service import AgentLoggingService
from execution.execution_data_collector import ExecutionDataCollector
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from execution.optimization_rule_repository import OptimizationRuleRepository
from core.core_task_execution_engine import CoreTaskExecutionEngine
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.rule_sync_landing_bridge import RuleSyncBridge
from sync.resident_guardian_manager import ResidentGuardianManager


class MegaTestMetrics:
    """测试指标收集器"""
    def __init__(self):
        self.cycle_times: List[float] = []
        self.memory_usage: List[float] = []
        self.success_rates: List[float] = []
        self.errors: List[str] = []
        self.start_time: float = time.time()
        
    def record_cycle(self, cycle_num: int, duration: float, memory_mb: float, success_rate: float):
        self.cycle_times.append(duration)
        self.memory_usage.append(memory_mb)
        self.success_rates.append(success_rate)
        
    def get_stats(self) -> Dict:
        if not self.cycle_times:
            return {}
        return {
            "total_runtime": time.time() - self.start_time,
            "cycles": len(self.cycle_times),
            "avg_cycle_time": sum(self.cycle_times) / len(self.cycle_times),
            "max_cycle_time": max(self.cycle_times),
            "min_cycle_time": min(self.cycle_times),
            "avg_memory_mb": sum(self.memory_usage) / len(self.memory_usage),
            "max_memory_mb": max(self.memory_usage),
            "avg_success_rate": sum(self.success_rates) / len(self.success_rates),
            "error_count": len(self.errors),
        }


def get_memory_usage() -> float:
    """获取当前内存使用(MB)"""
    process = psutil.Process(os.getpid())
    return process.memory_info().rss / 1024 / 1024


def build_mega_system(log_svc: AgentLoggingService, tmpdir: str):
    """构建超大规模测试系统"""
    recovery = ExceptionRecoveryHandler({}, log_svc)
    collector = ExecutionDataCollector(
        {"metrics_persistence_path": os.path.join(tmpdir, "metrics.json"),
         "metrics_retention_cycles": 1000}, 
        log_svc
    )
    rule_repo = OptimizationRuleRepository(
        {"rules_persistence_path": os.path.join(tmpdir, "rules.json"),
         "max_active_rules": 100}, 
        log_svc
    )
    
    # 扩大并发配置
    engine_config = {
        "max_concurrent_tasks": 20,
        "task_timeout_seconds": 60.0,
        "max_retries_per_task": 2,
        "retry_delay_seconds": 0.5,
        "task_queue_max_size": 50000,
    }
    engine = CoreTaskExecutionEngine(engine_config, log_svc, collector, recovery)
    
    analyzer = IntelligentOptimizationAnalyzer(
        {"analysis_interval_cycles": 5,
         "min_cycles_for_analysis": 5,
         "max_active_rules": 100}, 
        log_svc, collector, rule_repo, recovery
    )
    bridge = RuleSyncBridge(log_svc, rule_repo, engine, analyzer)
    guardian = ResidentGuardianManager(
        {"health_check_interval_seconds": 30.0,
         "max_component_errors_before_restart": 10}, 
        log_svc, recovery
    )
    return engine, collector, rule_repo, analyzer, bridge, guardian, recovery


def register_mega_handlers(engine: CoreTaskExecutionEngine):
    """注册超大规模测试处理器"""
    
    # 1. 超快任务
    def ultra_fast_task(x: int = 0) -> dict:
        return {"result": x}
    
    # 2. 内存泄漏模拟任务
    _leak_storage = []
    def memory_leak_task(leak_kb: int = 10) -> dict:
        if leak_kb > 0:
            _leak_storage.append(b"x" * (leak_kb * 1024))
        return {"leaked_kb": leak_kb, "total_leaks": len(_leak_storage)}
    
    # 3. 随机延迟任务
    def variable_delay_task(min_ms: int = 1, max_ms: int = 500) -> dict:
        delay = random.randint(min_ms, max_ms) / 1000.0
        time.sleep(delay)
        return {"delay_ms": delay * 1000}
    
    # 4. 高失败率任务
    def high_fail_task(fail_rate: float = 0.8) -> dict:
        if random.random() < fail_rate:
            raise RuntimeError(f"高概率失败 {fail_rate}")
        return {"success": True}
    
    # 5. 死锁模拟任务
    _lock_a = threading.Lock()
    _lock_b = threading.Lock()
    def deadlock_task(enable_deadlock: bool = False) -> dict:
        if enable_deadlock and random.random() < 0.1:
            # 模拟潜在死锁
            with _lock_a:
                time.sleep(0.001)
                with _lock_b:
                    return {"deadlock": True}
        return {"deadlock": False}
    
    # 6. 异常风暴任务
    def exception_storm_task() -> dict:
        error_types = [
            ValueError, TypeError, KeyError, IndexError, 
            RuntimeError, ConnectionError, TimeoutError, 
            MemoryError, RecursionError, OSError
        ]
        if random.random() < 0.7:
            raise random.choice(error_types)("异常风暴")
        return {"survived": True}
    
    # 7. 级联依赖任务
    def cascade_task(level: int = 0) -> dict:
        if level > 0 and random.random() < 0.3:
            raise RuntimeError(f"级联失败 level={level}")
        return {"level": level}
    
    # 8. 资源耗尽模拟
    def resource_exhaust_task(consume_mb: int = 1) -> dict:
        data = []
        try:
            for _ in range(consume_mb):
                data.append(b"x" * (1024 * 1024))
        except MemoryError:
            return {"error": "MemoryError"}
        finally:
            del data
        return {"consumed_mb": consume_mb}
    
    # 9. 无限循环模拟（超时测试）
    def infinite_loop_task(duration_ms: int = 100) -> dict:
        start = time.time()
        while (time.time() - start) * 1000 < duration_ms:
            pass
        return {"duration_ms": (time.time() - start) * 1000}
    
    # 10. 混沌任务（完全随机行为）
    def chaos_task(seed: int = 0) -> dict:
        random.seed(seed + int(time.time() * 1000))
        behavior = random.choice([
            "instant", "delay", "fail", "memory", "cpu", "exception", "timeout"
        ])
        
        if behavior == "instant":
            return {"behavior": "instant"}
        elif behavior == "delay":
            time.sleep(random.random() * 0.1)
            return {"behavior": "delay"}
        elif behavior == "fail":
            raise RuntimeError("混沌失败")
        elif behavior == "memory":
            _ = [b"x" * 1024 for _ in range(random.randint(100, 1000))]
            return {"behavior": "memory"}
        elif behavior == "cpu":
            _ = sum(i * i for i in range(random.randint(1000, 10000)))
            return {"behavior": "cpu"}
        elif behavior == "exception":
            raise random.choice([ValueError, TypeError, KeyError])("混沌异常")
        else:
            time.sleep(0.2)
            return {"behavior": "timeout"}
    
    handlers = {
        "ultra_fast_task": ultra_fast_task,
        "memory_leak_task": memory_leak_task,
        "variable_delay_task": variable_delay_task,
        "high_fail_task": high_fail_task,
        "deadlock_task": deadlock_task,
        "exception_storm_task": exception_storm_task,
        "cascade_task": cascade_task,
        "resource_exhaust_task": resource_exhaust_task,
        "infinite_loop_task": infinite_loop_task,
        "chaos_task": chaos_task,
    }
    
    for name, handler in handlers.items():
        engine.handler_registry.register(name, handler)


def run_mega_test(cycles: int = 1000, tasks_per_cycle: int = 100):
    """
    运行超大规模极限测试
    
    Args:
        cycles: 运行周期数（默认1000）
        tasks_per_cycle: 每周期任务数（默认100）
    """
    print("\n" + "=" * 70)
    print(f"  🔥 超大规模极限测试启动")
    print(f"  目标: {cycles} 周期 × {tasks_per_cycle} 任务 = {cycles * tasks_per_cycle:,} 总任务")
    print("=" * 70 + "\n")
    
    # 配置
    log_config = {
        "console_enabled": True,
        "file_enabled": True,
        "file_path": "/workspace/evolution_agent/logs/mega_test.log",
        "log_level": "INFO",
    }
    log_svc = AgentLoggingService(log_config)
    
    tmpdir = tempfile.mkdtemp(prefix="mega_test_")
    engine, collector, rule_repo, analyzer, bridge, guardian, recovery = \
        build_mega_system(log_svc, tmpdir)
    register_mega_handlers(engine)
    
    metrics = MegaTestMetrics()
    
    # 注册守护组件
    guardian.register_component(
        "engine",
        lambda: ComponentHealth(
            component_name="engine",
            status=HealthStatus.HEALTHY if engine.queue_size() < 40000 else HealthStatus.DEGRADED
        )
    )
    
    total_tasks_executed = 0
    total_success = 0
    total_failed = 0
    
    try:
        for cycle in range(1, cycles + 1):
            cycle_start = time.time()
            
            # 生成混合任务
            tasks = []
            
            # 60% 超快任务
            for i in range(int(tasks_per_cycle * 0.6)):
                tasks.append(TaskDefinition(
                    name=f"c{cycle}_fast_{i}",
                    handler="ultra_fast_task",
                    handler_params={"x": i},
                    priority=TaskPriority.HIGH,
                ))
            
            # 15% 随机延迟
            for i in range(int(tasks_per_cycle * 0.15)):
                tasks.append(TaskDefinition(
                    name=f"c{cycle}_delay_{i}",
                    handler="variable_delay_task",
                    handler_params={"min_ms": 1, "max_ms": 100},
                    priority=TaskPriority.NORMAL,
                ))
            
            # 10% 高失败率
            for i in range(int(tasks_per_cycle * 0.10)):
                tasks.append(TaskDefinition(
                    name=f"c{cycle}_fail_{i}",
                    handler="high_fail_task",
                    handler_params={"fail_rate": random.uniform(0.5, 0.9)},
                    max_retries=random.randint(0, 2),
                    priority=TaskPriority.LOW,
                ))
            
            # 8% 异常风暴
            for i in range(int(tasks_per_cycle * 0.08)):
                tasks.append(TaskDefinition(
                    name=f"c{cycle}_storm_{i}",
                    handler="exception_storm_task",
                    max_retries=1,
                    priority=TaskPriority.LOW,
                ))
            
            # 5% 混沌任务
            for i in range(int(tasks_per_cycle * 0.05)):
                tasks.append(TaskDefinition(
                    name=f"c{cycle}_chaos_{i}",
                    handler="chaos_task",
                    handler_params={"seed": cycle * 1000 + i},
                    max_retries=2,
                    priority=TaskPriority.NORMAL,
                ))
            
            # 2% 资源耗尽模拟
            for i in range(int(tasks_per_cycle * 0.02)):
                tasks.append(TaskDefinition(
                    name=f"c{cycle}_exhaust_{i}",
                    handler="resource_exhaust_task",
                    handler_params={"consume_mb": random.randint(1, 5)},
                    priority=TaskPriority.LOW,
                ))
            
            # 打乱任务顺序
            random.shuffle(tasks)
            
            # 提交任务
            submitted = engine.submit_tasks(tasks)
            
            # 执行周期
            summary = engine.execute_cycle(cycle)
            
            # 优化分析（每5周期）
            if cycle % 5 == 0:
                new_rules = analyzer.analyze_cycle(cycle)
                if new_rules:
                    bridge.sync_new_rules(new_rules)
            
            # 记录指标
            cycle_duration = time.time() - cycle_start
            memory_mb = get_memory_usage()
            success_rate = summary.success_rate if summary.total_tasks > 0 else 1.0
            
            metrics.record_cycle(cycle, cycle_duration, memory_mb, success_rate)
            
            total_tasks_executed += summary.total_tasks
            total_success += summary.completed_tasks
            total_failed += summary.failed_tasks
            
            # 每10周期输出状态
            if cycle % 10 == 0 or cycle == 1:
                stats = metrics.get_stats()
                print(f"  📊 周期 {cycle:4d}/{cycles} | "
                      f"任务: {summary.total_tasks:3d} | "
                      f"成功: {summary.completed_tasks:3d} | "
                      f"失败: {summary.failed_tasks:3d} | "
                      f"成功率: {success_rate*100:5.1f}% | "
                      f"耗时: {cycle_duration:.3f}s | "
                      f"内存: {memory_mb:.1f}MB")
            
            # 每100周期详细报告
            if cycle % 100 == 0:
                stats = metrics.get_stats()
                print(f"\n  {'─' * 60}")
                print(f"  📈 累计报告 (周期 {cycle}):")
                print(f"     总任务: {total_tasks_executed:,}")
                print(f"     总成功: {total_success:,} ({total_success/total_tasks_executed*100:.1f}%)")
                print(f"     总失败: {total_failed:,}")
                print(f"     平均周期耗时: {stats['avg_cycle_time']:.3f}s")
                print(f"     峰值内存: {stats['max_memory_mb']:.1f}MB")
                print(f"     运行时间: {stats['total_runtime']:.1f}s")
                print(f"  {'─' * 60}\n")
            
            # 内存检查：如果超过500MB，强制GC
            if memory_mb > 500:
                print(f"  ⚠️  内存使用过高 ({memory_mb:.1f}MB)，强制垃圾回收...")
                gc.collect()
                new_memory = get_memory_usage()
                print(f"  ✅ 垃圾回收后: {new_memory:.1f}MB")
    
    except KeyboardInterrupt:
        print("\n  ⛔ 用户中断测试")
    except Exception as e:
        print(f"\n  ❌ 测试异常: {e}")
        traceback.print_exc()
    
    finally:
        # 最终报告
        print("\n" + "=" * 70)
        print("  📊 最终测试报告")
        print("=" * 70)
        
        stats = metrics.get_stats()
        print(f"\n  运行统计:")
        print(f"    总周期: {stats.get('cycles', 0)}")
        print(f"    总任务: {total_tasks_executed:,}")
        print(f"    总成功: {total_success:,} ({total_success/max(1,total_tasks_executed)*100:.1f}%)")
        print(f"    总失败: {total_failed:,}")
        print(f"    运行时间: {stats.get('total_runtime', 0):.1f}s")
        
        print(f"\n  性能统计:")
        print(f"    平均周期耗时: {stats.get('avg_cycle_time', 0):.3f}s")
        print(f"    最快周期: {stats.get('min_cycle_time', 0):.3f}s")
        print(f"    最慢周期: {stats.get('max_cycle_time', 0):.3f}s")
        
        print(f"\n  资源统计:")
        print(f"    平均内存: {stats.get('avg_memory_mb', 0):.1f}MB")
        print(f"    峰值内存: {stats.get('max_memory_mb', 0):.1f}MB")
        print(f"    最终内存: {get_memory_usage():.1f}MB")
        
        print(f"\n  系统统计:")
        print(f"    活跃规则: {len(rule_repo.get_active_rules())}")
        print(f"    总规则: {rule_repo.get_rules_summary()['total_rules']}")
        print(f"    采集记录: {collector.get_total_stats()['total_executions']}")
        
        # 关闭系统
        print("\n  🔄 正在关闭系统...")
        engine.shutdown()
        
        # 持久化
        try:
            rule_repo.persist_to_file()
            collector.persist_to_file()
            print("  ✅ 数据已持久化")
        except Exception as e:
            print(f"  ⚠️  持久化失败: {e}")
        
        print("\n" + "=" * 70)
        
        return stats


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="超大规模极限测试")
    parser.add_argument("--cycles", type=int, default=1000, help="运行周期数")
    parser.add_argument("--tasks", type=int, default=100, help="每周期任务数")
    args = parser.parse_args()
    
    run_mega_test(cycles=args.cycles, tasks_per_cycle=args.tasks)
