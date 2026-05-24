"""
可观测性系统测试模块
===================

本模块包含可观测性系统的完整测试套件。
测试追踪、指标、日志和仪表盘功能。

运行方式:
    python -m observability.test_observability
    pytest observability/test_observability.py -v
"""

import sys
import os
import time
import json
import logging
import unittest
import threading
import tempfile
from datetime import datetime, timezone
from typing import Dict, Any
from unittest.mock import Mock, patch, MagicMock

# 添加父目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 导入被测模块
from observability.tracing import (
    TracingConfig,
    TracingManager,
    SpanContextManager,
    NoOpSpan,
    traced,
    OTEL_AVAILABLE,
)
from observability.metrics import (
    MetricsConfig,
    MetricsManager,
    CounterWrapper,
    GaugeWrapper,
    HistogramWrapper,
    EvolutionAgentMetrics,
    timed,
    counted,
    PROMETHEUS_AVAILABLE,
)
from observability.structured_logging import (
    LoggingConfig,
    StructuredLogger,
    LoggingManager,
    JSONFormatter,
    SensitiveDataFilter,
    LogContextManager,
    get_logger,
    setup_logging,
)
from observability.dashboard import (
    DashboardConfig,
    DashboardManager,
    DashboardAPI,
    HealthStatus,
    Alert,
    get_dashboard,
)


class TestTracing(unittest.TestCase):
    """追踪模块测试"""
    
    def setUp(self):
        """测试前准备"""
        # 重置单例
        TracingManager._instance = None
    
    def test_tracing_config_default(self):
        """测试默认配置"""
        config = TracingConfig()
        self.assertEqual(config.service_name, "evolution-agent")
        self.assertEqual(config.sampling_rate, 1.0)
        self.assertEqual(config.exporter_type, "console")
        self.assertTrue(config.enable_console_export)
    
    def test_tracing_config_validation(self):
        """测试配置验证"""
        # 有效配置
        config = TracingConfig(sampling_rate=0.5)
        self.assertTrue(config.validate())
        
        # 无效采样率
        with self.assertRaises(ValueError):
            TracingConfig(sampling_rate=1.5).validate()
        
        # 无效导出器类型
        with self.assertRaises(ValueError):
            TracingConfig(exporter_type="invalid").validate()
    
    def test_tracing_manager_initialization(self):
        """测试追踪管理器初始化"""
        config = TracingConfig(service_name="test-service")
        manager = TracingManager(config)
        
        self.assertEqual(manager._config.service_name, "test-service")
        self.assertTrue(manager._initialized)
    
    def test_tracing_manager_singleton(self):
        """测试单例模式"""
        manager1 = TracingManager()
        manager2 = TracingManager()
        self.assertIs(manager1, manager2)
    
    def test_tracing_manager_kwargs_config(self):
        """测试 kwargs 配置"""
        manager = TracingManager(service_name="kwargs-service", sampling_rate=0.8)
        self.assertEqual(manager._config.service_name, "kwargs-service")
        self.assertEqual(manager._config.sampling_rate, 0.8)
    
    def test_noop_span(self):
        """测试空操作 Span"""
        span = NoOpSpan()
        
        # 所有操作都应该无异常执行
        span.set_attribute("key", "value")
        span.set_attributes({"key": "value"})
        span.add_event("event")
        span.record_exception(Exception("test"))
        span.set_status(None)
        span.update_name("new_name")
        span.end()
        
        self.assertFalse(span.is_recording())
    
    @unittest.skipIf(not OTEL_AVAILABLE, "OpenTelemetry 不可用")
    def test_span_context_manager(self):
        """测试 Span 上下文管理器"""
        manager = TracingManager()
        
        with manager.span("test_operation") as span:
            if OTEL_AVAILABLE:
                span.set_attribute("test_attr", "test_value")
        
        # Span 应该已经结束
    
    def test_traced_decorator(self):
        """测试追踪装饰器"""
        @traced("test_function")
        def test_func(x, y):
            return x + y
        
        result = test_func(1, 2)
        self.assertEqual(result, 3)
    
    def test_traced_decorator_with_exception(self):
        """测试追踪装饰器异常处理"""
        @traced("error_function")
        def error_func():
            raise ValueError("test error")
        
        with self.assertRaises(ValueError):
            error_func()


class TestMetrics(unittest.TestCase):
    """指标模块测试"""
    
    def setUp(self):
        """测试前准备"""
        MetricsManager._instance = None
    
    def test_metrics_config_default(self):
        """测试默认配置"""
        config = MetricsConfig()
        self.assertEqual(config.namespace, "evolution_agent")
        self.assertEqual(config.http_port, 9090)
        self.assertTrue(config.enable_default_metrics)
    
    def test_metrics_config_validation(self):
        """测试配置验证"""
        # 有效配置
        config = MetricsConfig(http_port=8080)
        self.assertTrue(config.validate())
        
        # 无效端口
        with self.assertRaises(ValueError):
            MetricsConfig(http_port=70000).validate()
    
    def test_metrics_manager_initialization(self):
        """测试指标管理器初始化"""
        config = MetricsConfig(namespace="test-app")
        manager = MetricsManager(config)
        
        self.assertEqual(manager._config.namespace, "test-app")
        self.assertTrue(manager._initialized)
    
    def test_metrics_manager_singleton(self):
        """测试单例模式"""
        manager1 = MetricsManager()
        manager2 = MetricsManager()
        self.assertIs(manager1, manager2)
    
    def test_counter_creation(self):
        """测试 Counter 创建"""
        manager = MetricsManager()
        counter = manager.counter("test_counter", "Test counter description")
        
        self.assertIsInstance(counter, CounterWrapper)
        self.assertIn("test_counter", counter.name)
        
        # 测试操作
        counter.inc()
        counter.inc(5)
    
    def test_gauge_creation(self):
        """测试 Gauge 创建"""
        manager = MetricsManager()
        gauge = manager.gauge("test_gauge", "Test gauge description")
        
        self.assertIsInstance(gauge, GaugeWrapper)
        
        # 测试操作
        gauge.set(42)
        gauge.inc()
        gauge.dec()
    
    def test_histogram_creation(self):
        """测试 Histogram 创建"""
        manager = MetricsManager()
        histogram = manager.histogram("test_histogram", "Test histogram description")
        
        self.assertIsInstance(histogram, HistogramWrapper)
        
        # 测试操作
        histogram.observe(0.5)
        histogram.observe(1.0)
    
    def test_metric_with_labels(self):
        """测试带标签的指标"""
        manager = MetricsManager()
        counter = manager.counter(
            "labeled_counter",
            "Labeled counter",
            labelnames=["method", "status"]
        )
        
        # 使用标签
        labeled = counter.labels(method="GET", status="200")
        labeled.inc()
    
    def test_timed_decorator(self):
        """测试计时装饰器"""
        @timed("test_operation_duration")
        def slow_operation():
            time.sleep(0.01)
            return "done"
        
        result = slow_operation()
        self.assertEqual(result, "done")
    
    def test_counted_decorator(self):
        """测试计数装饰器"""
        @counted("test_function_calls")
        def counted_func():
            return "result"
        
        result = counted_func()
        self.assertEqual(result, "result")
    
    def test_evolution_agent_metrics(self):
        """测试 Evolution Agent 预定义指标"""
        metrics = EvolutionAgentMetrics()
        
        self.assertIsNotNone(metrics.tasks_total)
        self.assertIsNotNone(metrics.task_duration)
        self.assertIsNotNone(metrics.tasks_in_progress)
        self.assertIsNotNone(metrics.rules_total)
        self.assertIsNotNone(metrics.errors_total)
    
    @unittest.skipIf(not PROMETHEUS_AVAILABLE, "Prometheus 不可用")
    def test_generate_latest(self):
        """测试生成最新指标数据"""
        manager = MetricsManager()
        counter = manager.counter("export_test", "Export test")
        counter.inc()
        
        output = manager.generate_latest()
        self.assertIsInstance(output, bytes)
        self.assertIn(b"export_test", output)


class TestStructuredLogging(unittest.TestCase):
    """结构化日志模块测试"""
    
    def setUp(self):
        """测试前准备"""
        LoggingManager._instance = None
    
    def test_logging_config_default(self):
        """测试默认配置"""
        config = LoggingConfig()
        self.assertEqual(config.service_name, "evolution-agent")
        self.assertEqual(config.level, "INFO")
        self.assertEqual(config.output_format, "json")
    
    def test_logging_config_validation(self):
        """测试配置验证"""
        # 有效配置
        config = LoggingConfig(level="DEBUG")
        self.assertTrue(config.validate())
        
        # 无效级别
        with self.assertRaises(ValueError):
            LoggingConfig(level="INVALID").validate()
    
    def test_sensitive_data_filter(self):
        """测试敏感数据过滤器"""
        filter = SensitiveDataFilter(
            sensitive_fields=["password", "token"],
            mask_char="*"
        )
        
        # 测试敏感字段检测
        self.assertTrue(filter.is_sensitive("password"))
        self.assertTrue(filter.is_sensitive("TOKEN"))  # 大小写不敏感
        self.assertFalse(filter.is_sensitive("username"))
        
        # 测试脱敏
        masked = filter.mask("secret123")
        self.assertIn("*", masked)
        self.assertNotEqual(masked, "secret123")
        
        # 测试字典过滤
        data = {
            "username": "admin",
            "password": "secret123",
            "nested": {
                "token": "abc123"
            }
        }
        filtered = filter.filter_dict(data)
        self.assertEqual(filtered["username"], "admin")
        self.assertNotEqual(filtered["password"], "secret123")
        self.assertNotEqual(filtered["nested"]["token"], "abc123")
    
    def test_json_formatter(self):
        """测试 JSON 格式化器"""
        config = LoggingConfig(service_name="test-service")
        formatter = JSONFormatter(config)
        
        # 创建模拟 LogRecord
        record = logging.LogRecord(
            name="test.logger",
            level=logging.INFO,
            pathname="test.py",
            lineno=10,
            msg="Test message",
            args=(),
            exc_info=None,
        )
        record.levelname = "INFO"
        
        output = formatter.format(record)
        
        # 验证输出是有效 JSON
        data = json.loads(output)
        self.assertEqual(data["level"], "INFO")
        self.assertEqual(data["message"], "Test message")
        self.assertEqual(data["service"], "test-service")
        self.assertIn("timestamp", data)
    
    def test_structured_logger(self):
        """测试结构化日志器"""
        logger = get_logger("test.module")
        
        self.assertIsInstance(logger, StructuredLogger)
        self.assertEqual(logger._name, "test.module")
    
    def test_structured_logger_logging(self):
        """测试结构化日志记录"""
        with tempfile.NamedTemporaryFile(mode='w+', delete=False, suffix='.log') as f:
            temp_file = f.name
        
        try:
            config = LoggingConfig(
                output_streams=[open(temp_file, 'a')],
                pretty_print=False
            )
            manager = LoggingManager(config)
            manager.setup()
            
            logger = manager.get_logger("test.logger")
            logger.info("Test message", user_id=123, action="test")
            
            # 读取并验证
            with open(temp_file, 'r') as f:
                content = f.read()
                if content.strip():
                    data = json.loads(content.strip())
                    self.assertEqual(data["message"], "Test message")
                    self.assertIn("data", data)
        finally:
            os.unlink(temp_file)
    
    def test_logger_bind(self):
        """测试日志器绑定"""
        logger = get_logger("test.bind")
        bound_logger = logger.bind(request_id="abc123", user_id=456)
        
        self.assertIn("request_id", bound_logger._context)
        self.assertIn("user_id", bound_logger._context)
    
    def test_log_context_manager(self):
        """测试日志上下文管理器"""
        from observability.structured_logging import _log_context
        
        with LogContextManager({"request_id": "test-123"}):
            context = _log_context.get()
            self.assertEqual(context.get("request_id"), "test-123")
        
        # 上下文应该被清理
        context = _log_context.get()
        self.assertNotIn("request_id", context)


class TestDashboard(unittest.TestCase):
    """仪表盘模块测试"""
    
    def setUp(self):
        """测试前准备"""
        DashboardManager._instance = None
    
    def test_dashboard_config_default(self):
        """测试默认配置"""
        config = DashboardConfig()
        self.assertEqual(config.refresh_interval_seconds, 5.0)
        self.assertEqual(config.history_retention_minutes, 60)
        self.assertTrue(config.enable_alerts)
    
    def test_health_status(self):
        """测试健康状态"""
        status = HealthStatus(
            name="test_component",
            status="healthy",
            message="All good"
        )
        
        self.assertTrue(status.is_healthy())
        self.assertEqual(status.name, "test_component")
        
        # 测试转换为字典
        data = status.to_dict()
        self.assertEqual(data["name"], "test_component")
        self.assertEqual(data["status"], "healthy")
    
    def test_alert(self):
        """测试告警"""
        alert = Alert(
            id="alert-1",
            name="high_cpu",
            severity="warning",
            message="CPU usage high"
        )
        
        self.assertEqual(alert.status, "firing")
        self.assertIsNone(alert.ends_at)
        
        # 测试转换为字典
        data = alert.to_dict()
        self.assertEqual(data["id"], "alert-1")
        self.assertEqual(data["severity"], "warning")
    
    def test_dashboard_manager_initialization(self):
        """测试仪表盘管理器初始化"""
        config = DashboardConfig(refresh_interval_seconds=10.0)
        manager = DashboardManager(config)
        
        self.assertEqual(manager._config.refresh_interval_seconds, 10.0)
        self.assertTrue(manager._initialized)
    
    def test_dashboard_manager_singleton(self):
        """测试单例模式"""
        manager1 = DashboardManager()
        manager2 = DashboardManager()
        self.assertIs(manager1, manager2)
    
    def test_get_system_overview(self):
        """测试获取系统概览"""
        manager = DashboardManager()
        overview = manager.get_system_overview()
        
        self.assertIn("timestamp", overview)
        self.assertIn("uptime_seconds", overview)
        self.assertIn("overall_health", overview)
        self.assertIn("metrics", overview)
        self.assertIn("health_checks", overview)
        self.assertIn("active_alerts", overview)
    
    def test_get_realtime_metrics(self):
        """测试获取实时指标"""
        manager = DashboardManager()
        metrics = manager.get_realtime_metrics()
        
        self.assertIn("timestamp", metrics)
        # 应该包含系统指标
        if "cpu" in metrics:
            self.assertIn("percent", metrics["cpu"])
        if "memory" in metrics:
            self.assertIn("percent", metrics["memory"])
    
    def test_get_health_status(self):
        """测试获取健康状态"""
        manager = DashboardManager()
        health = manager.get_health_status()
        
        self.assertIn("overall", health)
        self.assertIn("checks", health)
        self.assertIn("timestamp", health)
    
    def test_get_alerts(self):
        """测试获取告警"""
        manager = DashboardManager()
        alerts = manager.get_alerts()
        
        self.assertIn("active", alerts)
        self.assertIn("count", alerts)
        self.assertIsInstance(alerts["active"], list)
    
    def test_register_health_check(self):
        """测试注册健康检查"""
        manager = DashboardManager()
        
        def custom_check():
            return HealthStatus(
                name="custom",
                status="healthy",
                message="Custom check passed"
            )
        
        manager.register_health_check("custom", custom_check)
        
        health = manager.get_health_status()
        self.assertIn("custom", health["checks"])
    
    def test_add_alert_rule(self):
        """测试添加告警规则"""
        manager = DashboardManager()
        
        manager.add_alert_rule(
            name="custom_alert",
            condition=lambda m: m.get("custom_value", 0) > 100,
            severity="critical",
            message_template="Custom value too high: {value}",
            threshold=100.0
        )
        
        # 规则应该已添加
        self.assertIn("custom_alert", manager._alert_manager._rules)
    
    def test_record_metric(self):
        """测试记录自定义指标"""
        manager = DashboardManager()
        
        manager.record_metric("custom_metric", 42.0, {"label": "test"})
        
        history = manager.get_metrics_history("custom_metric")
        self.assertEqual(len(history), 1)
        self.assertEqual(history[0]["value"], 42.0)
    
    def test_dashboard_api(self):
        """测试仪表盘 API"""
        api = DashboardAPI()
        
        # 测试各端点
        overview = api.overview()
        self.assertIn("timestamp", overview)
        
        metrics = api.metrics()
        self.assertIn("timestamp", metrics)
        
        health = api.health()
        self.assertIn("overall", health)
        
        alerts = api.alerts()
        self.assertIn("active", alerts)
        
        refresh_result = api.refresh()
        self.assertIn("metrics", refresh_result)


class TestIntegration(unittest.TestCase):
    """集成测试"""
    
    def test_full_observability_stack(self):
        """测试完整可观测性栈"""
        # 初始化所有组件
        tracing_manager = TracingManager(service_name="integration-test")
        metrics_manager = MetricsManager(namespace="integration_test")
        logging_manager = LoggingManager(LoggingConfig(service_name="integration-test"))
        logging_manager.setup()
        dashboard_manager = DashboardManager()
        
        # 获取日志器
        logger = logging_manager.get_logger("integration")
        
        # 创建指标
        counter = metrics_manager.counter("integration_counter", "Integration counter")
        histogram = metrics_manager.histogram("integration_histogram", "Integration histogram")
        
        # 执行操作并记录
        with tracing_manager.span("integration_operation") as span:
            logger.info("Starting integration operation")
            counter.inc()
            
            with histogram.time():
                time.sleep(0.01)
            
            logger.info("Completed integration operation", duration_ms=10)
        
        # 获取仪表盘数据
        overview = dashboard_manager.get_system_overview()
        
        # 验证
        self.assertIn("metrics", overview)
        self.assertIn("health_checks", overview)
    
    def test_concurrent_access(self):
        """测试并发访问"""
        manager = MetricsManager()
        counter = manager.counter("concurrent_counter", "Concurrent counter")
        
        results = []
        
        def increment_counter():
            for _ in range(100):
                counter.inc()
            results.append(True)
        
        # 创建多个线程
        threads = [
            threading.Thread(target=increment_counter)
            for _ in range(10)
        ]
        
        # 启动并等待
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        
        # 所有线程应该完成
        self.assertEqual(len(results), 10)


class TestObservabilitySystem(unittest.TestCase):
    """可观测性系统端到端测试"""
    
    def test_end_to_end_workflow(self):
        """测试端到端工作流"""
        # 1. 设置日志
        setup_logging(
            service_name="e2e-test",
            level="DEBUG",
            environment="test"
        )
        logger = get_logger("e2e")
        
        # 2. 设置指标
        metrics = MetricsManager(namespace="e2e")
        request_counter = metrics.counter(
            "requests_total",
            "Total requests",
            labelnames=["endpoint", "method"]
        )
        latency_histogram = metrics.histogram(
            "request_latency_seconds",
            "Request latency"
        )
        
        # 3. 设置追踪
        tracing = TracingManager(service_name="e2e-test")
        
        # 4. 设置仪表盘
        dashboard = DashboardManager()
        
        # 5. 模拟请求处理
        def process_request(endpoint: str, method: str):
            with tracing.span(f"request_{endpoint}") as span:
                if OTEL_AVAILABLE:
                    span.set_attribute("endpoint", endpoint)
                    span.set_attribute("method", method)
                
                logger.info(
                    "Processing request",
                    endpoint=endpoint,
                    method=method
                )
                
                request_counter.labels(endpoint=endpoint, method=method).inc()
                
                with latency_histogram.time():
                    time.sleep(0.01)  # 模拟处理时间
                
                logger.info("Request completed", endpoint=endpoint)
                
                return {"status": "ok"}
        
        # 6. 执行多个请求
        for i in range(5):
            result = process_request(f"/api/{i}", "GET")
            self.assertEqual(result["status"], "ok")
        
        # 7. 验证仪表盘数据
        overview = dashboard.get_system_overview()
        self.assertIn("metrics", overview)
        
        # 8. 记录自定义指标到仪表盘
        dashboard.record_metric("custom_requests", 5.0)
        
        # 9. 验证指标历史
        history = dashboard.get_metrics_history("custom_requests")
        self.assertEqual(len(history), 1)


def run_tests():
    """运行所有测试"""
    # 创建测试套件
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # 添加测试类
    suite.addTests(loader.loadTestsFromTestCase(TestTracing))
    suite.addTests(loader.loadTestsFromTestCase(TestMetrics))
    suite.addTests(loader.loadTestsFromTestCase(TestStructuredLogging))
    suite.addTests(loader.loadTestsFromTestCase(TestDashboard))
    suite.addTests(loader.loadTestsFromTestCase(TestIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestObservabilitySystem))
    
    # 运行测试
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # 打印摘要
    print("\n" + "=" * 60)
    print("测试摘要")
    print("=" * 60)
    print(f"总测试数: {result.testsRun}")
    print(f"成功: {result.testsRun - len(result.failures) - len(result.errors)}")
    print(f"失败: {len(result.failures)}")
    print(f"错误: {len(result.errors)}")
    print(f"跳过: {len(result.skipped)}")
    
    if result.failures:
        print("\n失败的测试:")
        for test, traceback in result.failures:
            print(f"  - {test}: {traceback}")
    
    if result.errors:
        print("\n出错的测试:")
        for test, traceback in result.errors:
            print(f"  - {test}: {traceback}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    success = run_tests()
    sys.exit(0 if success else 1)
