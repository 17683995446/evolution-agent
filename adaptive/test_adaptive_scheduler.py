"""
自适应调度器测试
"""
import sys
import os
import time
import unittest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from adaptive_scheduler import AdaptiveScheduler, AdaptiveConfig, ExecutionPattern
from ml_predictor import SimpleMLPredictor


class AdaptiveSchedulerTest(unittest.TestCase):
    """自适应调度器测试"""
    
    def test_01_pattern_detection_stable(self):
        """场景1：稳定模式检测"""
        print("\n[测试1] 稳定模式检测...")
        scheduler = AdaptiveScheduler()
        
        # 模拟100次稳定执行
        for i in range(100):
            scheduler.record_execution("stable_handler", 0.5, True)
        
        patterns = scheduler.analyze_patterns()
        self.assertIn("stable_handler", patterns)
        
        pattern = patterns["stable_handler"]
        self.assertEqual(pattern.failure_pattern, "stable")
        self.assertGreater(pattern.success_rate, 0.99)
        
        print(f"  [PASS] 检测到稳定模式，成功率={pattern.success_rate:.2%}")
    
    def test_02_pattern_detection_burst(self):
        """场景2：突发失败模式检测"""
        print("\n[测试2] 突发失败模式检测...")
        scheduler = AdaptiveScheduler()
        
        # 模拟突发失败（前50次成功，后10次连续失败）
        for i in range(50):
            scheduler.record_execution("burst_handler", 0.5, True)
        for i in range(10):
            scheduler.record_execution("burst_handler", 0.5, False)
        
        patterns = scheduler.analyze_patterns()
        pattern = patterns["burst_handler"]
        self.assertEqual(pattern.failure_pattern, "burst")
        
        print(f"  [PASS] 检测到突发失败模式")
    
    def test_03_dynamic_parameter_adjustment(self):
        """场景3：动态参数调整"""
        print("\n[测试3] 动态参数调整...")
        config = AdaptiveConfig(adjustment_interval_cycles=5, learning_rate=0.5)
        scheduler = AdaptiveScheduler(config)
        
        # 初始参数
        initial = scheduler.adjust_parameters()
        print(f"  初始参数: {initial}")
        
        # 模拟高成功率短任务
        for cycle in range(1, 21):
            for i in range(10):
                scheduler.record_execution("fast_handler", 0.05, True)
            scheduler._cycle_count = cycle
            params = scheduler.adjust_parameters()
        
        # 验证参数调整（并发度应该增加或至少不减少）
        self.assertGreaterEqual(params["concurrent_tasks"], initial["concurrent_tasks"])
        
        print(f"  [PASS] 参数调整后: {params}")
    
    def test_04_failure_prediction(self):
        """场景4：失败概率预测"""
        print("\n[测试4] 失败概率预测...")
        scheduler = AdaptiveScheduler()
        
        # 模拟低成功率任务
        for i in range(100):
            success = i < 30  # 30%成功率
            scheduler.record_execution("flaky_handler", 1.0, success)
        
        scheduler.analyze_patterns()
        prob = scheduler.predict_failure_probability("flaky_handler", 0.5)
        
        self.assertGreater(prob, 0.6)  # 预测失败概率应大于60%
        
        print(f"  [PASS] 预测失败概率={prob:.2%}")
    
    def test_05_ml_predictor_features(self):
        """场景5：ML预测器特征提取"""
        print("\n[测试5] ML预测器特征提取...")
        predictor = SimpleMLPredictor()
        
        history = []
        for i in range(50):
            history.append({
                "timestamp": time.time(),
                "duration": 0.5,
                "success": i < 40,  # 80%成功率
                "error_type": None if i < 40 else "TimeoutError",
            })
        
        features = predictor.extract_features("test_handler", history)
        
        self.assertAlmostEqual(features["recent_success_rate"], 0.8, places=1)
        self.assertIn("consecutive_failures", features)
        
        print(f"  [PASS] 特征提取: 成功率={features['recent_success_rate']:.2%}")
    
    def test_06_ml_prediction_confidence(self):
        """场景6：ML预测置信度"""
        print("\n[测试6] ML预测置信度...")
        predictor = SimpleMLPredictor(window_size=50)
        
        # 少量数据 - 低置信度
        small_history = [{"timestamp": time.time(), "duration": 1.0, "success": True, "error_type": None} for _ in range(5)]
        result_small = predictor.predict("handler", small_history)
        self.assertLess(result_small["confidence"], 0.3)
        
        # 大量数据 - 高置信度
        large_history = [{"timestamp": time.time(), "duration": 1.0, "success": True, "error_type": None} for _ in range(100)]
        result_large = predictor.predict("handler", large_history)
        self.assertGreater(result_large["confidence"], 0.5)
        
        print(f"  [PASS] 少量数据置信度={result_small['confidence']:.2f}, 大量数据={result_large['confidence']:.2f}")
    
    def test_07_risk_classification(self):
        """场景7：风险等级分类"""
        print("\n[测试7] 风险等级分类...")
        predictor = SimpleMLPredictor()
        
        levels = [
            (0.1, 0.8, "low"),
            (0.4, 0.8, "medium"),
            (0.7, 0.8, "high"),
            (0.9, 0.8, "critical"),
            (0.5, 0.2, "unknown"),  # 低置信度
        ]
        
        for prob, conf, expected in levels:
            level = predictor.classify_risk_level(prob, conf)
            self.assertEqual(level, expected)
        
        print(f"  [PASS] 风险等级分类正确")
    
    def test_08_integration_with_engine(self):
        """场景8：与执行引擎集成"""
        print("\n[测试8] 与执行引擎集成...")
        
        # 模拟与CoreTaskExecutionEngine的集成
        from evolution_agent_main_entry import EvolutionAgent
        from shared.global_configuration_loader import AgentConfig
        from shared.shared_data_models import TaskDefinition
        
        config = AgentConfig()
        agent = EvolutionAgent(config)
        agent.initialize()
        
        scheduler = AdaptiveScheduler()
        
        # 注册处理器
        def test_handler(**kwargs):
            return {"status": "ok"}
        
        agent._execution_engine.handler_registry.register("adaptive_test", test_handler)
        
        # 执行任务并记录到调度器
        for i in range(20):
            agent._execution_engine.submit_task(TaskDefinition(
                name=f"adaptive_{i}",
                handler="adaptive_test",
                handler_params={}
            ))
        
        summary = agent._execution_engine.execute_cycle(1)
        
        # 记录执行结果到调度器
        for _ in range(summary.completed_tasks):
            scheduler.record_execution("adaptive_test", 0.1, True)
        
        patterns = scheduler.analyze_patterns()
        self.assertIn("adaptive_test", patterns)
        
        agent._execution_engine.shutdown()
        print(f"  [PASS] 与引擎集成成功，检测到{len(patterns)}个模式")


if __name__ == "__main__":
    unittest.main(verbosity=2)
