#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ultra_chaos_extreme_test.py

终极混乱极端测试套件 - 第四轮迭代测试
结合多种故障模式同时发生，测试系统在极端混乱场景下的稳定性

测试场景设计原则：
1. 多维度故障叠加：内存压力 + 随机失败 + 死锁风险 + 异常风暴 + 资源耗尽
2. 时间维度混乱：随机延迟、突发流量、长时间运行
3. 空间维度混乱：大量并发任务、任务间依赖复杂
4. 状态维度混乱：组件状态频繁切换、规则频繁更新
"""

import sys
import os
import time
import random
import threading
import concurrent.futures
from datetime import datetime
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from enum import Enum, auto
import traceback
import gc

# 添加项目路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.shared_data_models import (
    TaskDefinition, TaskResult, TaskStatus, TaskPriority,
    OptimizationRule, RuleStatus, SystemEvent, EventType,
    RecoveryStrategy, ExceptionRecord
)
from shared.global_configuration_loader import AgentConfig
from shared.unified_logging_service import AgentLoggingService
from execution.execution_data_collector import ExecutionDataCollector
from execution.optimization_rule_repository import OptimizationRuleRepository
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from core.core_task_execution_engine_traced import CoreTaskExecutionEngineTraced
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.rule_sync_landing_bridge import RuleSyncBridge
from sync.resident_guardian_manager import ResidentGuardianManager


class ChaosLevel(Enum):
    """混乱等级"""
    LOW = auto()      # 轻度混乱
    MEDIUM = auto()   # 中度混乱
    HIGH = auto()     # 高度混乱
    EXTREME = auto()  # 极端混乱
    APOCALYPSE = auto()  # 末日级别


@dataclass
class ChaosConfig:
    """混乱测试配置"""
    level: ChaosLevel = ChaosLevel.EXTREME
    
    # 任务相关
    total_cycles: int = 500  # 总循环次数
    tasks_per_cycle: int = 200  # 每周期任务数
    max_concurrent_tasks: int = 50  # 最大并发
    
    # 故障注入
    failure_rate: float = 0.25  # 失败率
    exception_rate: float = 0.15  # 异常率
    deadlock_probability: float = 0.05  # 死锁概率
    
    # 资源压力
    memory_pressure_enabled: bool = True
    memory_pressure_mb: int = 500  # 内存压力(MB)
    cpu_spike_enabled: bool = True
    cpu_spike_duration: float = 0.5  # CPU峰值持续时间
    
    # 时间混乱
    random_delay_enabled: bool = True
    max_random_delay: float = 0.1  # 最大随机延迟
    burst_traffic_enabled: bool = True
    burst_multiplier: int = 3  # 突发流量倍数
    
    # 状态混乱
    frequent_rule_updates: bool = True
    rule_update_interval: int = 10  # 规则更新间隔(周期)
    component_restart_enabled: bool = True
    restart_probability: float = 0.02  # 组件重启概率
    
    # 依赖混乱
    task_dependencies_enabled: bool = True
    dependency_depth: int = 5  # 依赖深度
    circular_dependency_probability: float = 0.1  # 循环依赖概率
    
    # 日志验证
    log_verification_enabled: bool = True
    log_line_check_interval: int = 50  # 每50行检查一次


class ChaosTaskGenerator:
    """混乱任务生成器"""
    
    def __init__(self, config: ChaosConfig, logger):
        self._config = config
        self._log = logger
        self._task_counter = 0
        self._dependency_graph: Dict[str, List[str]] = {}
        self._lock = threading.Lock()
        
    def generate_chaos_task(self, cycle: int) -> TaskDefinition:
        """生成一个混乱任务"""
        with self._lock:
            self._task_counter += 1
            task_id = f"chaos_task_{cycle}_{self._task_counter}_{int(time.time()*1000)%10000}"
        
        # 随机优先级
        priority = random.choice(list(TaskPriority))
        
        # 随机超时
        timeout = random.uniform(0.5, 5.0)
        
        # 随机重试次数
        max_retries = random.randint(0, 3)
        
        # 生成任务元数据（包含混乱参数）
        metadata = self._generate_chaos_metadata(cycle)
        
        task = TaskDefinition(
            task_id=task_id,
            name=f"chaos_task_{cycle}_{self._task_counter}",
            description="Chaos test task",
            priority=priority,
            handler="chaos_operation",
            handler_params=metadata,
            timeout_seconds=timeout,
            max_retries=max_retries
        )
        
        # 添加依赖关系
        if self._config.task_dependencies_enabled and random.random() < 0.3:
            dependencies = self._generate_dependencies(task_id, cycle)
            task.dependencies = dependencies
            
        return task
    
    def _generate_chaos_metadata(self, cycle: int) -> Dict[str, Any]:
        """生成混乱元数据"""
        metadata = {
            "cycle": cycle,
            "chaos_level": self._config.level.name,
            "should_fail": random.random() < self._config.failure_rate,
            "should_exception": random.random() < self._config.exception_rate,
            "should_deadlock": random.random() < self._config.deadlock_probability,
            "memory_pressure": self._config.memory_pressure_enabled and random.random() < 0.3,
            "cpu_spike": self._config.cpu_spike_enabled and random.random() < 0.3,
            "random_delay": self._config.random_delay_enabled and random.random() < 0.5,
            "delay_duration": random.uniform(0.001, self._config.max_random_delay),
            "exception_type": random.choice([
                "ValueError", "RuntimeError", "TimeoutError", 
                "MemoryError", "RecursionError", "KeyError"
            ]),
            "payload_size_kb": random.randint(1, 1000),
        }
        return metadata
    
    def _generate_dependencies(self, task_id: str, cycle: int) -> List[str]:
        """生成任务依赖"""
        dependencies = []
        
        # 可能依赖同周期的其他任务
        if self._task_counter > 1 and random.random() < 0.5:
            dep_task = f"chaos_task_{cycle}_{random.randint(1, self._task_counter-1)}"
            dependencies.append(dep_task)
        
        # 可能依赖前一周期任务
        if cycle > 0 and random.random() < 0.3:
            dep_task = f"chaos_task_{cycle-1}_{random.randint(1, self._config.tasks_per_cycle)}"
            dependencies.append(dep_task)
            
        # 循环依赖检测
        if random.random() < self._config.circular_dependency_probability:
            # 故意创建循环依赖（系统应该能检测并处理）
            dependencies.append(task_id)  # 自己依赖自己
            
        return dependencies


class ChaosExecutor:
    """混乱执行器 - 执行混乱任务"""
    
    def __init__(self, config: ChaosConfig, logger):
        self._config = config
        self._log = logger
        self._execution_count = 0
        self._failure_count = 0
        self._exception_count = 0
        self._deadlock_count = 0
        self._lock = threading.Lock()
        
    def execute_chaos_task(self, task: TaskDefinition) -> TaskResult:
        """执行混乱任务"""
        start_time = time.time()
        metadata = task.payload
        
        try:
            # 随机延迟
            if metadata.get("random_delay"):
                time.sleep(metadata["delay_duration"])
            
            # 内存压力
            if metadata.get("memory_pressure"):
                self._apply_memory_pressure(metadata["payload_size_kb"])
            
            # CPU峰值
            if metadata.get("cpu_spike"):
                self._apply_cpu_spike()
            
            # 模拟死锁
            if metadata.get("should_deadlock"):
                return self._simulate_deadlock(task)
            
            # 抛出异常
            if metadata.get("should_exception"):
                return self._simulate_exception(task, metadata["exception_type"])
            
            # 模拟失败
            if metadata.get("should_fail"):
                with self._lock:
                    self._failure_count += 1
                return TaskResult(
                    task_id=task.task_id,
                    status=TaskStatus.FAILED,
                    result=None,
                    error_message="Simulated failure",
                    execution_time=time.time() - start_time,
                    retry_count=0
                )
            
            # 正常执行
            result = self._normal_execution(task)
            
            with self._lock:
                self._execution_count += 1
                
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.COMPLETED,
                result=result,
                error_message=None,
                execution_time=time.time() - start_time,
                retry_count=0
            )
            
        except Exception as e:
            with self._lock:
                self._exception_count += 1
            return TaskResult(
                task_id=task.task_id,
                status=TaskStatus.FAILED,
                result=None,
                error_message=str(e),
                execution_time=time.time() - start_time,
                retry_count=0
            )
    
    def _apply_memory_pressure(self, size_kb: int):
        """应用内存压力"""
        # 分配大块内存然后释放
        data = [0] * (size_kb * 128)  # 约 size_kb KB
        time.sleep(0.01)
        del data
        
    def _apply_cpu_spike(self):
        """应用CPU峰值"""
        start = time.time()
        while time.time() - start < self._config.cpu_spike_duration:
            _ = sum(i * i for i in range(10000))
            
    def _simulate_deadlock(self, task: TaskDefinition) -> TaskResult:
        """模拟死锁场景"""
        with self._lock:
            self._deadlock_count += 1
        
        # 创建两个锁并尝试以不同顺序获取
        lock_a = threading.Lock()
        lock_b = threading.Lock()
        
        def thread1():
            with lock_a:
                time.sleep(0.01)
                with lock_b:
                    pass
                    
        def thread2():
            with lock_b:
                time.sleep(0.01)
                with lock_a:
                    pass
        
        t1 = threading.Thread(target=thread1)
        t2 = threading.Thread(target=thread2)
        t1.start()
        t2.start()
        t1.join(timeout=0.1)
        t2.join(timeout=0.1)
        
        return TaskResult(
            task_id=task.task_id,
            status=TaskStatus.FAILED,
            result=None,
            error_message="Potential deadlock detected",
            execution_time=0.1,
            retry_count=0
        )
    
    def _simulate_exception(self, task: TaskDefinition, exc_type: str) -> TaskResult:
        """模拟各种异常"""
        with self._lock:
            self._exception_count += 1
            
        if exc_type == "ValueError":
            raise ValueError(f"Simulated ValueError for {task.task_id}")
        elif exc_type == "RuntimeError":
            raise RuntimeError(f"Simulated RuntimeError for {task.task_id}")
        elif exc_type == "TimeoutError":
            raise TimeoutError(f"Simulated TimeoutError for {task.task_id}")
        elif exc_type == "MemoryError":
            raise MemoryError(f"Simulated MemoryError for {task.task_id}")
        elif exc_type == "RecursionError":
            raise RecursionError(f"Simulated RecursionError for {task.task_id}")
        else:
            raise KeyError(f"Simulated KeyError for {task.task_id}")
    
    def _normal_execution(self, task: TaskDefinition) -> Dict[str, Any]:
        """正常执行任务"""
        # 模拟一些计算工作
        result = {
            "task_id": task.task_id,
            "processed_at": time.time(),
            "checksum": hash(task.task_id) % 10000,
            "data": list(range(random.randint(10, 100)))
        }
        return result
    
    def get_stats(self) -> Dict[str, int]:
        """获取执行统计"""
        with self._lock:
            return {
                "execution_count": self._execution_count,
                "failure_count": self._failure_count,
                "exception_count": self._exception_count,
                "deadlock_count": self._deadlock_count
            }


class LogLineVerifier:
    """日志行验证器 - 验证每行代码是否被执行"""
    
    def __init__(self, logger):
        self._log = logger
        self._expected_lines: Dict[str, List[int]] = {}
        self._seen_lines: Dict[str, set] = {}
        self._lock = threading.Lock()
        
    def register_expected_lines(self, file_name: str, line_numbers: List[int]):
        """注册期望执行的代码行"""
        with self._lock:
            self._expected_lines[file_name] = line_numbers
            self._seen_lines[file_name] = set()
    
    def verify_log_line(self, file_name: str, line_number: int) -> bool:
        """验证日志行是否被记录"""
        with self._lock:
            if file_name in self._seen_lines:
                self._seen_lines[file_name].add(line_number)
                return True
            return False
    
    def get_coverage_report(self) -> Dict[str, Any]:
        """获取代码覆盖率报告"""
        report = {}
        with self._lock:
            for file_name, expected in self._expected_lines.items():
                seen = self._seen_lines.get(file_name, set())
                coverage = len(seen) / len(expected) * 100 if expected else 0
                missing = set(expected) - seen
                report[file_name] = {
                    "total_lines": len(expected),
                    "executed_lines": len(seen),
                    "coverage_percent": round(coverage, 2),
                    "missing_lines": sorted(list(missing))
                }
        return report
    
    def print_coverage_report(self):
        """打印覆盖率报告"""
        report = self.get_coverage_report()
        self._log.info("=" * 80)
        self._log.info("代码执行覆盖率报告")
        self._log.info("=" * 80)
        
        for file_name, stats in report.items():
            self._log.info(f"\n文件: {file_name}")
            self._log.info(f"  总行数: {stats['total_lines']}")
            self._log.info(f"  已执行: {stats['executed_lines']}")
            self._log.info(f"  覆盖率: {stats['coverage_percent']}%")
            if stats['missing_lines']:
                self._log.warning(f"  未执行行: {stats['missing_lines'][:20]}...")  # 只显示前20个


class UltraChaosTestRunner:
    """终极混乱测试运行器"""
    
    def __init__(self, config: ChaosConfig):
        self._config = config
        self._log = AgentLoggingService({})
        self._logger = self._log.get_logger("ultra_chaos_test")
        self._line_verifier = LogLineVerifier(self._logger)
        
        # 初始化组件
        self._init_components()
        
        # 统计
        self._start_time = None
        self._end_time = None
        self._total_tasks = 0
        self._completed_tasks = 0
        self._failed_tasks = 0
        self._cycle_times: List[float] = []
        
    def _init_components(self):
        """初始化系统组件"""
        self._logger.info("[ULTRA-CHAOS] 初始化系统组件...")
        
        # 配置加载器
        self._config_loader = AgentConfig()
        
        # 数据收集器
        self._data_collector = ExecutionDataCollector(self._config_loader.section("storage"), self._log)
        
        # 规则仓库
        self._rule_repository = OptimizationRuleRepository(self._config_loader.section("optimization"), self._log)
        
        # 异常恢复处理器
        self._recovery_handler = ExceptionRecoveryHandler(self._config_loader.section("guardian"), self._log)
        
        # 使用 traced 版本的执行引擎
        self._execution_engine = CoreTaskExecutionEngineTraced(
            self._config_loader.section("execution"), self._log, self._data_collector, self._recovery_handler
        )
        
        # 优化分析器
        self._optimization_analyzer = IntelligentOptimizationAnalyzer(
            self._config_loader.section("optimization"), self._log, self._data_collector, 
            self._rule_repository, self._recovery_handler
        )
        
        # 规则同步桥
        self._rule_bridge = RuleSyncBridge(
            self._log, self._rule_repository, self._execution_engine, self._optimization_analyzer
        )
        
        # 守护管理器
        self._guardian_manager = ResidentGuardianManager(
            self._config_loader.section("guardian"), self._log, self._recovery_handler
        )
        
        # 混乱任务生成器
        self._task_generator = ChaosTaskGenerator(self._config, self._logger)
        
        # 混乱执行器
        self._chaos_executor = ChaosExecutor(self._config, self._logger)
        
        self._logger.info("[ULTRA-CHAOS] 所有组件初始化完成")
        
    def run_ultra_chaos_test(self) -> Dict[str, Any]:
        """运行终极混乱测试"""
        self._logger.info("=" * 80)
        self._logger.info("开始终极混乱极端测试")
        self._logger.info(f"混乱等级: {self._config.level.name}")
        self._logger.info(f"总周期数: {self._config.total_cycles}")
        self._logger.info(f"每周期任务数: {self._config.tasks_per_cycle}")
        self._logger.info(f"预计总任务数: {self._config.total_cycles * self._config.tasks_per_cycle}")
        self._logger.info("=" * 80)
        
        self._start_time = time.time()
        
        try:
            # 启动所有组件
            self._startup_components()
            
            # 运行测试周期
            for cycle in range(1, self._config.total_cycles + 1):
                cycle_start = time.time()
                self._run_chaos_cycle(cycle)
                cycle_time = time.time() - cycle_start
                self._cycle_times.append(cycle_time)
                
                # 周期性日志
                if cycle % 50 == 0:
                    self._log_progress(cycle)
                
                # 频繁规则更新
                if (self._config.frequent_rule_updates and 
                    cycle % self._config.rule_update_interval == 0):
                    self._inject_rule_chaos(cycle)
                
                # 组件重启测试
                if (self._config.component_restart_enabled and 
                    random.random() < self._config.restart_probability):
                    self._restart_random_component()
                    
            # 停止所有组件
            self._shutdown_components()
            
        except Exception as e:
            self._logger.error(f"[ULTRA-CHAOS] 测试过程中发生致命错误: {e}")
            self._logger.error(traceback.format_exc())
            raise
        
        self._end_time = time.time()
        
        # 生成测试报告
        return self._generate_report()
    
    def _startup_components(self):
        """启动所有组件"""
        self._logger.info("[ULTRA-CHAOS] 启动系统组件...")
        
        self._execution_engine.start()
        self._optimization_analyzer.start()
        self._rule_bridge.start()
        self._guardian_manager.start()
        
        self._logger.info("[ULTRA-CHAOS] 所有组件已启动")
    
    def _shutdown_components(self):
        """停止所有组件"""
        self._logger.info("[ULTRA-CHAOS] 停止系统组件...")
        
        self._guardian_manager.stop()
        self._rule_bridge.stop()
        self._optimization_analyzer.stop()
        self._execution_engine.stop()
        
        self._logger.info("[ULTRA-CHAOS] 所有组件已停止")
    
    def _run_chaos_cycle(self, cycle: int):
        """运行一个混乱周期"""
        tasks_this_cycle = self._config.tasks_per_cycle
        
        # 突发流量
        if self._config.burst_traffic_enabled and random.random() < 0.1:
            tasks_this_cycle *= self._config.burst_multiplier
            self._logger.warning(f"[ULTRA-CHAOS] 周期 {cycle}: 突发流量! 任务数: {tasks_this_cycle}")
        
        # 生成并提交任务
        tasks = []
        for i in range(tasks_this_cycle):
            task = self._task_generator.generate_chaos_task(cycle)
            tasks.append(task)
            self._execution_engine.submit_task(task)
            self._total_tasks += 1
        
        # 等待任务完成（带超时）
        self._wait_for_tasks(tasks, timeout=30.0)
    
    def _wait_for_tasks(self, tasks: List[TaskDefinition], timeout: float):
        """等待任务完成"""
        start = time.time()
        completed = set()
        
        while len(completed) < len(tasks) and time.time() - start < timeout:
            for task in tasks:
                if task.task_id in completed:
                    continue
                    
                # 检查任务状态
                result = self._execution_engine.get_task_result(task.task_id)
                if result:
                    completed.add(task.task_id)
                    if result.status == TaskStatus.COMPLETED:
                        self._completed_tasks += 1
                    else:
                        self._failed_tasks += 1
            
            time.sleep(0.01)
    
    def _inject_rule_chaos(self, cycle: int):
        """注入规则混乱"""
        self._logger.info(f"[ULTRA-CHAOS] 周期 {cycle}: 注入规则混乱")
        
        # 随机创建、更新、删除规则
        actions = ["create", "update", "delete", "activate", "deactivate"]
        action = random.choice(actions)
        
        if action == "create":
            rule = OptimizationRule(
                rule_id=f"chaos_rule_{cycle}_{int(time.time())}",
                name=f"Chaos Rule {cycle}",
                condition=lambda ctx: random.random() < 0.5,
                action=lambda ctx: None,
                priority=random.randint(1, 100)
            )
            self._rule_repository.add_rule(rule)
            
        elif action == "update":
            rules = self._rule_repository.get_all_rules()
            if rules:
                rule = random.choice(rules)
                rule.priority = random.randint(1, 100)
                
        elif action == "delete":
            rules = self._rule_repository.get_all_rules()
            if rules and len(rules) > 5:  # 保留至少5条规则
                rule = random.choice(rules)
                self._rule_repository.delete_rule(rule.rule_id)
    
    def _restart_random_component(self):
        """随机重启组件"""
        components = [
            ("execution_engine", self._execution_engine),
            ("optimization_analyzer", self._optimization_analyzer),
            ("rule_bridge", self._rule_bridge)
        ]
        
        name, component = random.choice(components)
        self._logger.warning(f"[ULTRA-CHAOS] 随机重启组件: {name}")
        
        component.stop()
        time.sleep(0.1)
        component.start()
    
    def _log_progress(self, cycle: int):
        """记录进度"""
        elapsed = time.time() - self._start_time
        avg_cycle_time = sum(self._cycle_times[-50:]) / min(50, len(self._cycle_times))
        
        chaos_stats = self._chaos_executor.get_stats()
        
        self._logger.info(
            f"[ULTRA-CHAOS] 进度 - 周期: {cycle}/{self._config.total_cycles}, "
            f"总任务: {self._total_tasks}, 完成: {self._completed_tasks}, "
            f"失败: {self._failed_tasks}, "
            f"执行: {chaos_stats['execution_count']}, 异常: {chaos_stats['exception_count']}, "
            f"死锁: {chaos_stats['deadlock_count']}, "
            f"平均周期时间: {avg_cycle_time:.3f}s, 已用时间: {elapsed:.1f}s"
        )
    
    def _generate_report(self) -> Dict[str, Any]:
        """生成测试报告"""
        total_time = self._end_time - self._start_time
        chaos_stats = self._chaos_executor.get_stats()
        
        report = {
            "test_name": "终极混乱极端测试",
            "chaos_level": self._config.level.name,
            "total_cycles": self._config.total_cycles,
            "tasks_per_cycle": self._config.tasks_per_cycle,
            "total_tasks": self._total_tasks,
            "completed_tasks": self._completed_tasks,
            "failed_tasks": self._failed_tasks,
            "success_rate": round(self._completed_tasks / max(1, self._total_tasks) * 100, 2),
            "total_time_seconds": round(total_time, 2),
            "avg_cycle_time": round(sum(self._cycle_times) / len(self._cycle_times), 3) if self._cycle_times else 0,
            "chaos_stats": chaos_stats,
            "timestamp": datetime.now().isoformat()
        }
        
        # 打印报告
        self._logger.info("=" * 80)
        self._logger.info("终极混乱测试报告")
        self._logger.info("=" * 80)
        for key, value in report.items():
            if key != "chaos_stats":
                self._logger.info(f"  {key}: {value}")
        self._logger.info(f"  混乱统计: {chaos_stats}")
        self._logger.info("=" * 80)
        
        # 打印代码覆盖率报告
        self._line_verifier.print_coverage_report()
        
        return report


def main():
    """主函数"""
    # 创建极端混乱配置
    config = ChaosConfig(
        level=ChaosLevel.APOCALYPSE,
        total_cycles=10,  # 10个周期（减少以便快速测试）
        tasks_per_cycle=10,  # 每周期10个任务
        max_concurrent_tasks=5,
        failure_rate=0.20,  # 20%失败率
        exception_rate=0.10,  # 10%异常率
        deadlock_probability=0.02,  # 2%死锁概率
        memory_pressure_enabled=True,
        memory_pressure_mb=50,
        cpu_spike_enabled=True,
        random_delay_enabled=True,
        max_random_delay=0.01,
        burst_traffic_enabled=True,
        burst_multiplier=2,
        frequent_rule_updates=True,
        rule_update_interval=5,
        component_restart_enabled=True,
        restart_probability=0.01,
        task_dependencies_enabled=True,
        circular_dependency_probability=0.05,
        log_verification_enabled=True
    )
    
    # 运行测试
    runner = UltraChaosTestRunner(config)
    report = runner.run_ultra_chaos_test()
    
    print("\n" + "=" * 80)
    print("测试完成!")
    print(f"成功率: {report['success_rate']}%")
    print(f"总耗时: {report['total_time_seconds']}秒")
    print("=" * 80)
    
    return report


if __name__ == "__main__":
    main()
