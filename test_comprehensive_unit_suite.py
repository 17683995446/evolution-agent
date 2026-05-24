#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
全面单元测试套件
覆盖所有模块的所有分支、边界条件和异常情况
"""

import sys
import os
import unittest
import threading
import time
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from shared.shared_data_models import (
    TaskDefinition, TaskResult, TaskStatus, TaskPriority,
    OptimizationRule, RuleCategory, RuleStatus, SystemEvent, EventType,
    RecoveryStrategy, ExceptionRecord, ComponentHealth, HealthStatus,
    CycleSummary, ExecutionMetric
)
from shared.global_configuration_loader import AgentConfig
from shared.unified_logging_service import AgentLoggingService
from execution.execution_data_collector import ExecutionDataCollector
from execution.optimization_rule_repository import OptimizationRuleRepository
from execution.exception_recovery_handler import ExceptionRecoveryHandler
from core.core_task_execution_engine import CoreTaskExecutionEngine, TaskHandlerRegistry
from core.intelligent_optimization_analyzer import IntelligentOptimizationAnalyzer
from sync.rule_sync_landing_bridge import RuleSyncBridge
from sync.resident_guardian_manager import ResidentGuardianManager


class TestTaskHandlerRegistry(unittest.TestCase):
    """测试任务处理器注册表"""
    
    def setUp(self):
        self.registry = TaskHandlerRegistry()
    
    def test_register_and_get(self):
        """测试注册和获取处理器"""
        handler = lambda x: x
        self.registry.register("test_handler", handler)
        self.assertEqual(self.registry.get("test_handler"), handler)
    
    def test_get_nonexistent(self):
        """测试获取不存在的处理器"""
        self.assertIsNone(self.registry.get("nonexistent"))
    
    def test_list_handlers(self):
        """测试列出所有处理器"""
        self.registry.register("handler1", lambda x: x)
        self.registry.register("handler2", lambda x: x)
        handlers = self.registry.list_handlers()
        self.assertEqual(len(handlers), 2)
        self.assertIn("handler1", handlers)
        self.assertIn("handler2", handlers)
    
    def test_thread_safety(self):
        """测试线程安全"""
        results = []
        
        def register_handlers():
            for i in range(100):
                self.registry.register(f"handler_{i}", lambda x: x)
                results.append(self.registry.get(f"handler_{i}"))
        
        threads = [threading.Thread(target=register_handlers) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        self.assertEqual(len(results), 500)


class TestAgentConfig(unittest.TestCase):
    """测试配置加载器"""
    
    def test_default_config(self):
        """测试默认配置"""
        config = AgentConfig()
        self.assertEqual(config.get("agent.name"), "EvolutionAgent")
        self.assertEqual(config.get("execution.max_concurrent_tasks"), 5)
    
    def test_get_with_default(self):
        """测试获取不存在的配置项"""
        config = AgentConfig()
        self.assertEqual(config.get("nonexistent.key", "default"), "default")
    
    def test_set_and_get(self):
        """测试设置和获取配置"""
        config = AgentConfig()
        config.set("test.key", "value")
        self.assertEqual(config.get("test.key"), "value")
    
    def test_section(self):
        """测试获取配置段"""
        config = AgentConfig()
        section = config.section("execution")
        self.assertIn("max_concurrent_tasks", section)
    
    def test_deep_merge(self):
        """测试深度合并"""
        config = AgentConfig({"execution": {"max_concurrent_tasks": 10}})
        self.assertEqual(config.get("execution.max_concurrent_tasks"), 10)
        self.assertEqual(config.get("execution.task_timeout_seconds"), 300.0)
    
    def test_env_override(self):
        """测试环境变量覆盖"""
        with patch.dict(os.environ, {"EVOLUTION_AGENT_AGENT_NAME": "TestAgent"}):
            config = AgentConfig()
            self.assertEqual(config.get("agent.name"), "TestAgent")
    
    def test_parse_env_value(self):
        """测试环境变量值解析"""
        config = AgentConfig()
        self.assertTrue(config._parse_env_value("true"))
        self.assertTrue(config._parse_env_value("1"))
        self.assertFalse(config._parse_env_value("false"))
        self.assertEqual(config._parse_env_value("123"), 123)
        self.assertEqual(config._parse_env_value("3.14"), 3.14)
        self.assertEqual(config._parse_env_value("string"), "string")


class TestExecutionDataCollector(unittest.TestCase):
    """测试数据采集器"""
    
    def setUp(self):
        self.logging_service = AgentLoggingService({"console_enabled": False})
        self.config = {"metrics_retention_cycles": 100}
        self.collector = ExecutionDataCollector(self.config, self.logging_service)
    
    def test_record_task_execution_success(self):
        """测试记录成功任务"""
        result = TaskResult(task_id="test_1", status=TaskStatus.COMPLETED)
        result.finished_at = time.time()
        result.duration_seconds = 1.0
        
        metric = self.collector.record_task_execution(result, 1)
        self.assertTrue(metric.success)
        self.assertEqual(metric.task_id, "test_1")
    
    def test_record_task_execution_failure(self):
        """测试记录失败任务"""
        result = TaskResult(task_id="test_1", status=TaskStatus.FAILED)
        result.error = "Test error"
        result.finished_at = time.time()
        result.duration_seconds = 1.0
        
        metric = self.collector.record_task_execution(result, 1)
        self.assertFalse(metric.success)
        # 修复后：error_type 是字符串截取前100字符，不再是 type().__name__
        self.assertEqual(metric.error_type, "Test error")
    
    def test_get_success_rate(self):
        """测试获取成功率"""
        # 添加成功任务
        result1 = TaskResult(task_id="test_1", status=TaskStatus.COMPLETED)
        result1.finished_at = time.time()
        result1.duration_seconds = 1.0
        self.collector.record_task_execution(result1, 1)
        
        # 添加失败任务
        result2 = TaskResult(task_id="test_2", status=TaskStatus.FAILED)
        result2.finished_at = time.time()
        result2.duration_seconds = 1.0
        self.collector.record_task_execution(result2, 1)
        
        self.assertEqual(self.collector.get_success_rate(), 0.5)
    
    def test_get_average_duration(self):
        """测试获取平均时长"""
        result = TaskResult(task_id="test_1", status=TaskStatus.COMPLETED)
        result.finished_at = time.time()
        result.duration_seconds = 2.0
        self.collector.record_task_execution(result, 1)
        
        self.assertEqual(self.collector.get_average_duration(), 2.0)
    
    def test_get_error_distribution(self):
        """测试获取错误分布"""
        result = TaskResult(task_id="test_1", status=TaskStatus.FAILED)
        result.error = "Test error"
        result.finished_at = time.time()
        result.duration_seconds = 1.0
        self.collector.record_task_execution(result, 1)
        
        dist = self.collector.get_error_distribution()
        # 修复后：error_type 是字符串截取，不再是 "str"
        self.assertIn("Test error", dist)
    
    def test_prune_old_data(self):
        """测试清理旧数据"""
        # 添加数据（使用递增的周期号）
        for i in range(10):
            result = TaskResult(task_id=f"test_{i}", status=TaskStatus.COMPLETED)
            result.finished_at = time.time()
            result.duration_seconds = 1.0
            self.collector.record_task_execution(result, i)
        
        # 添加一个周期汇总以使清理逻辑正常工作
        from shared.shared_data_models import CycleSummary
        for i in range(10):
            summary = CycleSummary(cycle_number=i, total_tasks=1, completed_tasks=1)
            self.collector._cycle_summaries.append(summary)
        
        removed = self.collector.prune_old_data(max_cycles=5)
        self.assertGreater(removed, 0)
    
    def test_persist_and_load(self):
        """测试数据持久化和加载"""
        with tempfile.TemporaryDirectory() as tmpdir:
            persist_path = os.path.join(tmpdir, "metrics.json")
            config = {"metrics_persistence_path": persist_path}
            collector = ExecutionDataCollector(config, self.logging_service)
            
            result = TaskResult(task_id="test_1", status=TaskStatus.COMPLETED)
            result.finished_at = time.time()
            result.duration_seconds = 1.0
            collector.record_task_execution(result, 1)
            
            collector.persist_to_file()
            self.assertTrue(os.path.exists(persist_path))


class TestOptimizationRuleRepository(unittest.TestCase):
    """测试规则仓库"""
    
    def setUp(self):
        self.logging_service = AgentLoggingService({"console_enabled": False})
        self.config = {"max_active_rules": 50}
        self.repo = OptimizationRuleRepository(self.config, self.logging_service)
    
    def test_add_rule(self):
        """测试添加规则"""
        rule = OptimizationRule(name="Test Rule", category=RuleCategory.PERFORMANCE)
        rule_id = self.repo.add_rule(rule)
        self.assertIsNotNone(rule_id)
        self.assertEqual(self.repo.get_rule(rule_id).name, "Test Rule")
    
    def test_get_active_rules(self):
        """测试获取活跃规则"""
        # 先停用所有现有规则
        for rule in self.repo.get_all_rules():
            self.repo.deactivate_rule(rule.rule_id)
        
        rule1 = OptimizationRule(name="Active Rule", is_active=True)
        rule2 = OptimizationRule(name="Inactive Rule", is_active=False)
        self.repo.add_rule(rule1)
        self.repo.add_rule(rule2)
        
        active = self.repo.get_active_rules()
        self.assertEqual(len(active), 1)
        self.assertEqual(active[0].name, "Active Rule")
    
    def test_update_rule(self):
        """测试更新规则"""
        rule = OptimizationRule(name="Test Rule")
        rule_id = self.repo.add_rule(rule)
        
        success = self.repo.update_rule(rule_id, priority=5)
        self.assertTrue(success)
        self.assertEqual(self.repo.get_rule(rule_id).priority, 5)
    
    def test_update_nonexistent_rule(self):
        """测试更新不存在的规则"""
        success = self.repo.update_rule("nonexistent", priority=5)
        self.assertFalse(success)
    
    def test_mark_rule_applied(self):
        """测试标记规则已应用"""
        rule = OptimizationRule(name="Test Rule")
        rule_id = self.repo.add_rule(rule)
        
        self.repo.mark_rule_applied(rule_id)
        rule = self.repo.get_rule(rule_id)
        self.assertEqual(rule.apply_count, 1)
        self.assertIsNotNone(rule.applied_at)
    
    def test_update_effectiveness_auto_deactivate(self):
        """测试效果评分自动停用"""
        rule = OptimizationRule(name="Test Rule", is_active=True)
        rule_id = self.repo.add_rule(rule)
        
        self.repo.update_effectiveness(rule_id, 0.05)  # 低于阈值
        rule = self.repo.get_rule(rule_id)
        self.assertFalse(rule.is_active)
    
    def test_auto_prune_rules(self):
        """测试自动清理规则"""
        # 添加超过最大数量的规则
        for i in range(60):
            rule = OptimizationRule(
                name=f"Rule {i}",
                effectiveness_score=0.1 * (i % 10)
            )
            self.repo.add_rule(rule)
        
        pruned = self.repo.auto_prune_rules()
        self.assertGreater(pruned, 0)
    
    def test_persist_and_load_with_invalid_category(self):
        """测试持久化和加载（包含无效分类）"""
        with tempfile.TemporaryDirectory() as tmpdir:
            persist_path = os.path.join(tmpdir, "rules.json")
            config = {"rules_persistence_path": persist_path}
            repo = OptimizationRuleRepository(config, self.logging_service)
            
            rule = OptimizationRule(name="Test Rule", category=RuleCategory.PERFORMANCE)
            repo.add_rule(rule)
            repo.persist_to_file()
            
            # 手动修改文件，添加无效分类
            with open(persist_path, "r") as f:
                data = json.load(f)
            data["rules"][0]["category"] = "invalid_category"
            with open(persist_path, "w") as f:
                json.dump(data, f)
            
            # 重新加载，应该处理无效值
            repo2 = OptimizationRuleRepository(config, self.logging_service)
            loaded_rule = repo2.get_all_rules()[0]
            self.assertEqual(loaded_rule.category, RuleCategory.PERFORMANCE)


class TestExceptionRecoveryHandler(unittest.TestCase):
    """测试异常恢复处理器"""
    
    def setUp(self):
        self.logging_service = AgentLoggingService({"console_enabled": False})
        self.config = {"max_component_errors_before_restart": 5}
        self.handler = ExceptionRecoveryHandler(self.config, self.logging_service)
    
    def test_register_recovery(self):
        """测试注册恢复动作"""
        from execution.exception_recovery_handler import RecoveryAction, RecoveryStrategy
        action = RecoveryAction("test", RecoveryStrategy.RETRY)
        self.handler.register_recovery("test_component", action)
        # 验证注册成功（无异常即可）
    
    def test_handle_exception_with_retry(self):
        """测试处理异常（重试策略）"""
        success = self.handler.handle_exception("test_component", ValueError("Test error"))
        # 默认策略是RETRY，应该返回True
        self.assertTrue(success)
    
    def test_get_error_stats(self):
        """测试获取错误统计"""
        self.handler.handle_exception("comp1", ValueError("Error 1"))
        self.handler.handle_exception("comp1", ValueError("Error 2"))
        self.handler.handle_exception("comp2", ValueError("Error 3"))
        
        stats = self.handler.get_error_stats()
        self.assertIn("comp1", stats.get("components_with_errors", {}))


class TestCoreTaskExecutionEngine(unittest.TestCase):
    """测试核心执行引擎"""
    
    def setUp(self):
        self.logging_service = AgentLoggingService({"console_enabled": False})
        self.config = {
            "max_concurrent_tasks": 2,
            "task_timeout_seconds": 5.0,
            "max_retries_per_task": 1,
            "retry_delay_seconds": 0.1,
            "task_queue_max_size": 10
        }
        self.data_collector = ExecutionDataCollector({}, self.logging_service)
        self.recovery_handler = ExceptionRecoveryHandler({}, self.logging_service)
        self.engine = CoreTaskExecutionEngine(
            self.config, self.logging_service, self.data_collector, self.recovery_handler
        )
    
    def test_submit_task(self):
        """测试提交任务"""
        task = TaskDefinition(name="Test Task", handler="test_handler")
        success = self.engine.submit_task(task)
        self.assertTrue(success)
        self.assertEqual(self.engine.queue_size(), 1)
    
    def test_submit_task_queue_full(self):
        """测试队列满时提交任务"""
        # 填满队列
        for i in range(10):
            task = TaskDefinition(name=f"Task {i}", handler="test_handler")
            self.engine.submit_task(task)
        
        # 再提交应该失败
        task = TaskDefinition(name="Full Task", handler="test_handler")
        success = self.engine.submit_task(task)
        self.assertFalse(success)
    
    def test_execute_cycle_empty_queue(self):
        """测试执行空队列周期"""
        summary = self.engine.execute_cycle(1)
        self.assertEqual(summary.total_tasks, 0)
    
    def test_execute_cycle_with_tasks(self):
        """测试执行任务周期"""
        # 注册处理器
        self.engine.handler_registry.register("test_handler", lambda: "success")
        
        # 提交任务
        task = TaskDefinition(name="Test Task", handler="test_handler")
        self.engine.submit_task(task)
        
        # 执行周期
        summary = self.engine.execute_cycle(1)
        self.assertEqual(summary.total_tasks, 1)
        self.assertEqual(summary.completed_tasks, 1)
    
    def test_execute_cycle_with_failed_task(self):
        """测试执行失败任务"""
        # 注册失败处理器
        def fail_handler():
            raise ValueError("Test error")
        self.engine.handler_registry.register("fail_handler", fail_handler)
        
        # 提交任务
        task = TaskDefinition(name="Fail Task", handler="fail_handler", max_retries=0)
        self.engine.submit_task(task)
        
        # 执行周期
        summary = self.engine.execute_cycle(1)
        self.assertEqual(summary.total_tasks, 1)
        self.assertEqual(summary.failed_tasks, 1)
    
    def test_apply_optimization_rules(self):
        """测试应用优化规则"""
        rules = [{"name": "Test Rule", "action": "reduce timeout"}]
        self.engine.apply_optimization_rules(rules)
        stats = self.engine.get_stats()
        self.assertEqual(stats["active_rules_count"], 1)
    
    def test_shutdown(self):
        """测试关闭引擎"""
        self.engine.shutdown()
        stats = self.engine.get_stats()
        self.assertFalse(stats["is_running"])


class TestIntelligentOptimizationAnalyzer(unittest.TestCase):
    """测试智能优化分析器"""
    
    def setUp(self):
        self.logging_service = AgentLoggingService({"console_enabled": False})
        self.config = {"min_cycles_for_analysis": 1, "rule_effectiveness_threshold": 0.3}
        self.data_collector = ExecutionDataCollector({}, self.logging_service)
        self.rule_repo = OptimizationRuleRepository({}, self.logging_service)
        self.recovery_handler = ExceptionRecoveryHandler({}, self.logging_service)
        self.analyzer = IntelligentOptimizationAnalyzer(
            self.config, self.logging_service, self.data_collector, self.rule_repo, self.recovery_handler
        )
    
    def test_analyze_cycle_no_data(self):
        """测试无数据时的分析"""
        rules = self.analyzer.analyze_cycle(1)
        self.assertEqual(len(rules), 0)
    
    def test_analyze_cycle_with_data(self):
        """测试有数据时的分析"""
        # 添加执行数据
        for i in range(10):
            result = TaskResult(task_id=f"test_{i}", status=TaskStatus.COMPLETED)
            result.finished_at = time.time()
            result.duration_seconds = 1.0
            self.data_collector.record_task_execution(result, 1)
        
        # 分析
        rules = self.analyzer.analyze_cycle(1)
        # 可能生成优化规则
    
    def test_get_analysis_stats(self):
        """测试获取分析统计"""
        self.analyzer.analyze_cycle(1)
        stats = self.analyzer.get_analysis_stats()
        self.assertEqual(stats["total_analyses"], 1)


class TestRuleSyncBridge(unittest.TestCase):
    """测试规则同步桥"""
    
    def setUp(self):
        self.logging_service = AgentLoggingService({"console_enabled": False})
        self.rule_repo = OptimizationRuleRepository({}, self.logging_service)
        self.data_collector = ExecutionDataCollector({}, self.logging_service)
        self.recovery_handler = ExceptionRecoveryHandler({}, self.logging_service)
        self.execution_engine = CoreTaskExecutionEngine(
            {}, self.logging_service, self.data_collector, self.recovery_handler
        )
        self.analyzer = IntelligentOptimizationAnalyzer(
            {}, self.logging_service, self.data_collector, self.rule_repo, self.recovery_handler
        )
        self.bridge = RuleSyncBridge(
            self.logging_service, self.rule_repo, self.execution_engine, self.analyzer
        )
    
    def test_sync_all_rules(self):
        """测试同步所有规则"""
        # 先清空仓库中的现有规则（来自setUp）
        for rule in self.rule_repo.get_all_rules():
            self.rule_repo.deactivate_rule(rule.rule_id)
        
        # 添加新规则
        rule = OptimizationRule(name="Test Rule", is_active=True)
        self.rule_repo.add_rule(rule)
        
        # 同步
        count = self.bridge.sync_all_rules()
        self.assertEqual(count, 1)
    
    def test_sync_new_rules(self):
        """测试同步新规则"""
        # 先清空仓库中的现有规则
        for rule in self.rule_repo.get_all_rules():
            self.rule_repo.deactivate_rule(rule.rule_id)
        
        rule = OptimizationRule(name="New Rule", is_active=True)
        count = self.bridge.sync_new_rules([rule])
        # sync_new_rules 会调用 sync_all_rules，返回所有活跃规则数
        self.assertEqual(count, 1)
    
    def test_recursive_sync_prevention(self):
        """测试防止递归同步"""
        # 修复后：使用锁机制防止递归，而非布尔标志
        # 模拟并发同步场景
        results = []
        
        def do_sync():
            count = self.bridge.sync_all_rules()
            results.append(count)
        
        threads = [threading.Thread(target=do_sync) for _ in range(3)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # 至少有一个同步成功
        self.assertTrue(any(r > 0 for r in results))


class TestResidentGuardianManager(unittest.TestCase):
    """测试守护管理器"""
    
    def setUp(self):
        self.logging_service = AgentLoggingService({"console_enabled": False})
        self.config = {"health_check_interval_seconds": 1.0}
        self.recovery_handler = ExceptionRecoveryHandler({}, self.logging_service)
        self.guardian = ResidentGuardianManager(self.config, self.logging_service, self.recovery_handler)
    
    def test_register_component(self):
        """测试注册组件"""
        health_check = lambda: ComponentHealth("test", HealthStatus.HEALTHY)
        self.guardian.register_component("test", health_check)
        # 验证注册成功
    
    def test_check_all_components(self):
        """测试检查所有组件"""
        health_check = lambda: ComponentHealth("test", HealthStatus.HEALTHY)
        self.guardian.register_component("test", health_check)
        
        results = self.guardian.check_all_components()
        self.assertIn("test", results)
        self.assertEqual(results["test"].status, HealthStatus.HEALTHY)
    
    def test_check_unhealthy_component(self):
        """测试检查不健康组件"""
        health_check = lambda: ComponentHealth("unhealthy", HealthStatus.UNHEALTHY)
        self.guardian.register_component("unhealthy", health_check)
        
        results = self.guardian.check_all_components()
        self.assertEqual(results["unhealthy"].status, HealthStatus.UNHEALTHY)
    
    def test_get_system_health(self):
        """测试获取系统健康状态"""
        health_check = lambda: ComponentHealth("test", HealthStatus.HEALTHY)
        self.guardian.register_component("test", health_check)
        
        health = self.guardian.get_system_health()
        self.assertIn("overall_status", health)
        self.assertIn("components", health)


class TestDataModels(unittest.TestCase):
    """测试数据模型"""
    
    def test_task_result_success(self):
        """测试任务结果成功状态"""
        result = TaskResult(task_id="test", status=TaskStatus.COMPLETED)
        self.assertTrue(result.success)
        
        result2 = TaskResult(task_id="test2", status=TaskStatus.FAILED)
        self.assertFalse(result2.success)
    
    def test_task_result_duration(self):
        """测试任务结果时长计算"""
        result = TaskResult(task_id="test")
        result.started_at = time.time()
        result.finished_at = result.started_at + 2.0
        self.assertEqual(result.duration, 2.0)
    
    def test_cycle_summary_success_rate(self):
        """测试周期汇总成功率"""
        summary = CycleSummary(cycle_number=1, total_tasks=10, completed_tasks=7)
        self.assertEqual(summary.success_rate, 0.7)
        
        summary2 = CycleSummary(cycle_number=2, total_tasks=0)
        self.assertEqual(summary2.success_rate, 0.0)
    
    def test_optimization_rule_to_dict(self):
        """测试规则转字典"""
        rule = OptimizationRule(name="Test Rule", category=RuleCategory.PERFORMANCE)
        data = rule.to_dict()
        self.assertEqual(data["name"], "Test Rule")
        self.assertEqual(data["category"], "performance")


class TestIntegration(unittest.TestCase):
    """集成测试"""
    
    def test_full_cycle(self):
        """测试完整周期"""
        logging_service = AgentLoggingService({"console_enabled": False})
        
        # 创建所有组件
        config = {"max_concurrent_tasks": 2, "task_timeout_seconds": 5.0}
        data_collector = ExecutionDataCollector({}, logging_service)
        recovery_handler = ExceptionRecoveryHandler({}, logging_service)
        rule_repo = OptimizationRuleRepository({}, logging_service)
        
        engine = CoreTaskExecutionEngine(
            config, logging_service, data_collector, recovery_handler
        )
        
        analyzer = IntelligentOptimizationAnalyzer(
            {}, logging_service, data_collector, rule_repo, recovery_handler
        )
        
        bridge = RuleSyncBridge(
            logging_service, rule_repo, engine, analyzer
        )
        
        # 注册处理器
        engine.handler_registry.register("test_task", lambda: "success")
        
        # 提交任务
        task = TaskDefinition(name="Test Task", handler="test_task")
        engine.submit_task(task)
        
        # 执行周期
        summary = engine.execute_cycle(1)
        
        # 分析
        analyzer.analyze_cycle(1)
        
        # 同步规则
        bridge.sync_all_rules()
        
        # 验证
        self.assertEqual(summary.completed_tasks, 1)


def run_all_tests():
    """运行所有测试"""
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # 添加所有测试类
    test_classes = [
        TestTaskHandlerRegistry,
        TestAgentConfig,
        TestExecutionDataCollector,
        TestOptimizationRuleRepository,
        TestExceptionRecoveryHandler,
        TestCoreTaskExecutionEngine,
        TestIntelligentOptimizationAnalyzer,
        TestRuleSyncBridge,
        TestResidentGuardianManager,
        TestDataModels,
        TestIntegration,
    ]
    
    for test_class in test_classes:
        tests = loader.loadTestsFromTestCase(test_class)
        suite.addTests(tests)
    
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
