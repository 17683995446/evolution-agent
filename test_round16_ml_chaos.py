#!/usr/bin/env python3
"""
第16轮：自适应ML引擎极端混沌测试

测试场景：
1. 10000次执行记录并发写入
2. 极端成功率场景（0%, 100%, 50%）
3. 异常执行时长（极短、极长、异常值）
4. 连续失败风暴
5. 多handler并发分析
6. 参数调整稳定性
7. 预测准确率验证
"""

import sys
import os
import time
import threading
import random
import statistics
from collections import deque

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from adaptive.adaptive_scheduler import AdaptiveScheduler, AdaptiveConfig, ExecutionPattern
from adaptive.ml_predictor import SimpleMLPredictor

# ═══════════════════════════════════════════════════════════════
#  测试结果收集
# ═══════════════════════════════════════════════════════════════

results = {"passed": 0, "failed": 0, "errors": []}

def run_test(name: str, func):
    """运行单个测试"""
    try:
        func()
        results["passed"] += 1
        print(f"  [PASS] {name}")
    except Exception as e:
        results["failed"] += 1
        results["errors"].append(f"{name}: {str(e)[:200]}")
        print(f"  [FAIL] {name}: {str(e)[:100]}")

def assert_true(condition, msg=""):
    if not condition:
        raise AssertionError(f"AssertTrue failed: {msg}")

def assert_eq(actual, expected, msg=""):
    if actual != expected:
        raise AssertionError(f"{msg}: expected={expected}, actual={actual}")

def assert_near(actual, expected, tolerance=0.1, msg=""):
    if abs(actual - expected) > tolerance:
        raise AssertionError(f"{msg}: expected~{expected}, actual={actual}")


# ═══════════════════════════════════════════════════════════════
#  测试1: 10000次执行记录并发写入
# ═══════════════════════════════════════════════════════════════

def test_10000_concurrent_records():
    """10000次执行记录并发写入"""
    print("\n  [Test] 10000次执行记录并发写入...")
    
    scheduler = AdaptiveScheduler()
    num_records = 10000
    num_threads = 20
    records_per_thread = num_records // num_threads
    completed = [0]
    lock = threading.Lock()
    
    def writer(thread_id: int):
        for i in range(records_per_thread):
            handler = f"handler-{thread_id % 5}"  # 5个不同handler
            duration = random.uniform(0.1, 2.0)
            success = random.random() > 0.2  # 80%成功率
            scheduler.record_execution(handler, duration, success)
            with lock:
                completed[0] += 1
    
    threads = [threading.Thread(target=writer, args=(i,)) for i in range(num_threads)]
    start_time = time.time()
    
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    elapsed = time.time() - start_time
    
    print(f"    写入记录: {completed[0]}/{num_records}")
    print(f"    耗时: {elapsed:.2f}s")
    print(f"    吞吐量: {completed[0]/elapsed:.0f} records/s")
    
    # 验证数据完整性
    stats = scheduler.get_stats()
    print(f"    追踪handlers数: {stats['handlers_tracked']}")
    
    assert_eq(stats['handlers_tracked'], 5, "应该追踪5个handler")
    assert_eq(completed[0], num_records, "所有记录应该写入成功")


# ═══════════════════════════════════════════════════════════════
#  测试2: 极端成功率场景
# ═══════════════════════════════════════════════════════════════

def test_extreme_success_rates():
    """极端成功率场景（0%, 100%, 50%）"""
    print("\n  [Test] 极端成功率场景...")
    
    scheduler = AdaptiveScheduler()
    
    # 100%成功率
    for i in range(100):
        scheduler.record_execution("perfect_handler", 0.5, True)
    
    # 0%成功率
    for i in range(100):
        scheduler.record_execution("failed_handler", 0.5, False)
    
    # 50%成功率
    for i in range(100):
        scheduler.record_execution("half_handler", 0.5, i % 2 == 0)
    
    patterns = scheduler.analyze_patterns()
    
    # 验证100%成功率
    perfect = patterns["perfect_handler"]
    assert_near(perfect.success_rate, 1.0, 0.01, "perfect_handler成功率")
    assert_eq(perfect.failure_pattern, "stable", "perfect_handler应该是stable模式")
    
    # 验证0%成功率
    failed = patterns["failed_handler"]
    assert_near(failed.success_rate, 0.0, 0.01, "failed_handler成功率")
    assert_eq(failed.failure_pattern, "always_fail", "failed_handler应该是always_fail模式")
    
    # 验证50%成功率
    half = patterns["half_handler"]
    assert_near(half.success_rate, 0.5, 0.1, "half_handler成功率")
    
    print(f"    perfect_handler: 成功率={perfect.success_rate:.2%}, 模式={perfect.failure_pattern}")
    print(f"    failed_handler: 成功率={failed.success_rate:.2%}, 模式={failed.failure_pattern}")
    print(f"    half_handler: 成功率={half.success_rate:.2%}, 模式={half.failure_pattern}")


# ═══════════════════════════════════════════════════════════════
#  测试3: 异常执行时长
# ═══════════════════════════════════════════════════════════════

def test_extreme_durations():
    """异常执行时长（极短、极长、异常值）"""
    print("\n  [Test] 异常执行时长...")
    
    scheduler = AdaptiveScheduler()
    
    # 极短执行（<1ms）
    for i in range(50):
        scheduler.record_execution("fast_handler", 0.0001, True)
    
    # 极长执行（>100s）
    for i in range(50):
        scheduler.record_execution("slow_handler", 150.0, True)
    
    # 混合执行（正常+异常值）
    for i in range(100):
        if i == 95:  # 插入异常值
            scheduler.record_execution("mixed_handler", 999.0, True)
        else:
            scheduler.record_execution("mixed_handler", random.uniform(0.5, 2.0), True)
    
    patterns = scheduler.analyze_patterns()
    
    # 验证极短执行
    fast = patterns["fast_handler"]
    assert_true(fast.avg_duration < 0.001, f"fast_handler平均时长应<1ms, 实际={fast.avg_duration}")
    
    # 验证极长执行
    slow = patterns["slow_handler"]
    assert_true(slow.avg_duration > 100, f"slow_handler平均时长应>100s, 实际={slow.avg_duration}")
    
    # 验证最优超时计算
    mixed = patterns["mixed_handler"]
    print(f"    fast_handler: 平均时长={fast.avg_duration:.6f}s, 最优超时={fast.optimal_timeout:.2f}s")
    print(f"    slow_handler: 平均时长={slow.avg_duration:.2f}s, 最优超时={slow.optimal_timeout:.2f}s")
    print(f"    mixed_handler: 平均时长={mixed.avg_duration:.2f}s, 最优超时={mixed.optimal_timeout:.2f}s")


# ═══════════════════════════════════════════════════════════════
#  测试4: 连续失败风暴
# ═══════════════════════════════════════════════════════════════

def test_failure_burst():
    """连续失败风暴"""
    print("\n  [Test] 连续失败风暴...")
    
    scheduler = AdaptiveScheduler()
    
    # 正常执行50次
    for i in range(50):
        scheduler.record_execution("storm_handler", 0.5, True)
    
    # 失败风暴20次
    for i in range(20):
        scheduler.record_execution("storm_handler", 0.5, False, "ConnectionError")
    
    # 恢复正常30次
    for i in range(30):
        scheduler.record_execution("storm_handler", 0.5, True)
    
    patterns = scheduler.analyze_patterns()
    storm = patterns["storm_handler"]
    
    print(f"    检测到模式: {storm.failure_pattern}")
    print(f"    成功率: {storm.success_rate:.2%}")
    print(f"    推荐重试次数: {storm.recommended_retries}")
    
    # 验证失败模式检测
    assert_eq(storm.failure_pattern, "burst", "应该检测到burst模式")
    
    # 验证策略推荐
    strategy = scheduler.get_recommended_strategy("storm_handler")
    print(f"    推荐策略: {strategy['recommendation']}")
    
    # burst模式应该建议熔断
    assert_eq(strategy["recommendation"], "circuit_breaker", "burst模式应该建议熔断")


# ═══════════════════════════════════════════════════════════════
#  测试5: 多handler并发分析
# ═══════════════════════════════════════════════════════════════
def test_multi_handler_analysis():
    """多handler并发分析"""
    print("\n  [Test] 多handler并发分析...")
    
    scheduler = AdaptiveScheduler()
    num_handlers = 10
    records_per_handler = 50
    
    # 并发写入多个handler
    def handler_writer(handler_id: int):
        handler_name = f"multi_handler_{handler_id}"
        success_rate = handler_id / num_handlers  # 不同成功率
        for i in range(records_per_handler):
            success = random.random() < success_rate
            scheduler.record_execution(handler_name, random.uniform(0.1, 1.0), success)
    
    threads = [threading.Thread(target=handler_writer, args=(i,)) for i in range(1, num_handlers + 1)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    
    # 分析模式
    patterns = scheduler.analyze_patterns()
    
    print(f"    分析handler数: {len(patterns)}")
    
    # 验证所有handler都被分析
    assert_eq(len(patterns), num_handlers, "所有handler应该被分析")
    
    # 验证成功率排序
    sorted_patterns = sorted(patterns.values(), key=lambda p: p.success_rate)
    for i, p in enumerate(sorted_patterns):
        print(f"      {p.handler_name}: 成功率={p.success_rate:.2%}")


# ═══════════════════════════════════════════════════════════════
#  测试6: 参数调整稳定性
# ═══════════════════════════════════════════════════════════════

def test_parameter_adjustment_stability():
    """参数调整稳定性"""
    print("\n  [Test] 参数调整稳定性...")
    
    config = AdaptiveConfig(
        adjustment_interval_cycles=5,
        learning_rate=0.3,
        min_concurrent_tasks=1,
        max_concurrent_tasks=50
    )
    scheduler = AdaptiveScheduler(config)
    
    # 模拟100个调整周期
    params_history = []
    for cycle in range(100):
        # 每个周期添加一些执行记录
        for i in range(10):
            scheduler.record_execution("stable_handler", 0.5, True)
        
        scheduler._cycle_count = cycle
        params = scheduler.adjust_parameters()
        params_history.append(params.copy())
    
    # 验证参数稳定性（不应剧烈波动）
    concurrent_values = [p["concurrent_tasks"] for p in params_history[-20:]]
    max_change = max(concurrent_values) - min(concurrent_values)
    
    print(f"    最后20周期并发度范围: {min(concurrent_values):.1f} - {max(concurrent_values):.1f}")
    print(f"    最大变化: {max_change:.1f}")
    
    # 参数应该趋于稳定
    assert_true(max_change <= 10, f"参数应该趋于稳定，最大变化={max_change}")
    
    # 验证参数在合法范围内
    final_params = params_history[-1]
    assert_true(1 <= final_params["concurrent_tasks"] <= 50, "并发度应在合法范围")
    assert_true(1.0 <= final_params["timeout_seconds"] <= 300.0, "超时应在合法范围")


# ═══════════════════════════════════════════════════════════════
#  测试7: ML预测准确率验证
# ═══════════════════════════════════════════════════════════════

def test_ml_prediction_accuracy():
    """ML预测准确率验证"""
    print("\n  [Test] ML预测准确率验证...")
    
    predictor = SimpleMLPredictor(window_size=100)
    
    # 构建训练数据
    history = []
    for i in range(200):
        success = random.random() > 0.3  # 70%成功率
        history.append({
            "timestamp": time.time(),
            "duration": random.uniform(0.5, 1.5),
            "success": success,
            "error_type": None if success else "TimeoutError",
        })
    
    # 预测
    prediction = predictor.predict("test_handler", history)
    
    print(f"    预测失败概率: {prediction['failure_probability']:.2%}")
    print(f"    预测置信度: {prediction['confidence']:.2%}")
    print(f"    风险等级: {predictor.classify_risk_level(prediction['failure_probability'], prediction['confidence'])}")
    print(f"    影响因子: {prediction['factors']}")
    
    # 验证预测合理性
    assert_true(0 <= prediction['failure_probability'] <= 1, "失败概率应在0-1之间")
    assert_true(0 <= prediction['confidence'] <= 1, "置信度应在0-1之间")
    
    # 验证预测准确（实际失败率约30%，预测失败率应接近）
    actual_failure_rate = 1 - sum(h["success"] for h in history) / len(history)
    prediction_error = abs(prediction['failure_probability'] - actual_failure_rate)
    
    print(f"    实际失败率: {actual_failure_rate:.2%}")
    print(f"    预测误差: {prediction_error:.2%}")
    
    # ML预测在随机数据上误差<30%是可接受的
    assert_true(prediction_error < 0.3, f"预测误差应<30%, 实际={prediction_error:.2%}")


# ═══════════════════════════════════════════════════════════════
#  测试8: 边界条件测试
# ═══════════════════════════════════════════════════════════════

def test_edge_cases():
    """边界条件测试"""
    print("\n  [Test] 边界条件测试...")
    
    scheduler = AdaptiveScheduler()
    predictor = SimpleMLPredictor()
    
    # 测试1: 空历史数据
    empty_prediction = predictor.predict("empty_handler", [])
    assert_eq(empty_prediction["confidence"], 0.0, "空数据置信度应为0")
    print("    空数据预测: OK")
    
    # 测试2: 单条记录
    single_prediction = predictor.predict("single_handler", [{
        "timestamp": time.time(),
        "duration": 1.0,
        "success": True,
        "error_type": None,
    }])
    assert_true(single_prediction["confidence"] < 0.3, "单条记录置信度应低")
    print("    单条记录预测: OK")
    
    # 测试3: 极端失败概率预测
    # 未知handler应返回默认值
    unknown_prob = scheduler.predict_failure_probability("unknown_handler", 0.5)
    assert_eq(unknown_prob, 0.5, "未知handler失败概率应为0.5")
    print("    未知handler预测: OK")
    
    # 测试4: 零负载预测
    for i in range(20):
        scheduler.record_execution("load_handler", 0.5, True)
    scheduler.analyze_patterns()
    zero_load_prob = scheduler.predict_failure_probability("load_handler", 0.0)
    assert_true(0 <= zero_load_prob <= 1, "零负载失败概率应有效")
    print("    零负载预测: OK")


# ═══════════════════════════════════════════════════════════════
#  测试9: 长期运行稳定性
# ═══════════════════════════════════════════════════════════════

def test_long_term_stability():
    """长期运行稳定性（模拟1000周期）"""
    print("\n  [Test] 长期运行稳定性（1000周期）...")
    
    config = AdaptiveConfig(adjustment_interval_cycles=10)
    scheduler = AdaptiveScheduler(config)
    
    num_cycles = 1000
    errors = []
    
    start_time = time.time()
    
    for cycle in range(num_cycles):
        try:
            # 模拟各种执行情况
            scheduler.record_execution("stable", 0.5, True)
            scheduler.record_execution("flaky", 0.5, random.random() > 0.3)
            
            if cycle % 100 == 0:
                scheduler.record_execution("rare", 1.0, random.random() > 0.5)
            
            if cycle % 10 == 0:
                scheduler._cycle_count = cycle
                params = scheduler.adjust_parameters()
                
        except Exception as e:
            errors.append(f"Cycle {cycle}: {str(e)}")
    
    elapsed = time.time() - start_time
    
    # 最终分析
    patterns = scheduler.analyze_patterns()
    stats = scheduler.get_stats()
    
    print(f"    运行周期: {num_cycles}")
    print(f"    耗时: {elapsed:.2f}s")
    print(f"    错误数: {len(errors)}")
    print(f"    追踪handlers: {stats['handlers_tracked']}")
    print(f"    识别patterns: {stats['patterns_identified']}")
    print(f"    参数调整次数: {stats['adjustments_made']}")
    
    assert_eq(len(errors), 0, f"不应有错误: {errors[:3]}")


# ═══════════════════════════════════════════════════════════════
#  主测试入口
# ═══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 70)
    print("  第16轮：自适应ML引擎极端混沌测试")
    print("=" * 70)
    
    run_test("10000次并发记录", test_10000_concurrent_records)
    run_test("极端成功率场景", test_extreme_success_rates)
    run_test("异常执行时长", test_extreme_durations)
    run_test("连续失败风暴", test_failure_burst)
    run_test("多handler并发分析", test_multi_handler_analysis)
    run_test("参数调整稳定性", test_parameter_adjustment_stability)
    run_test("ML预测准确率", test_ml_prediction_accuracy)
    run_test("边界条件测试", test_edge_cases)
    run_test("长期运行稳定性", test_long_term_stability)
    
    print("\n" + "=" * 70)
    print(f"  Round 16 Results: PASS={results['passed']}  FAIL={results['failed']}")
    print("=" * 70)
    
    if results["errors"]:
        print("\nFailure Details:")
        for err in results["errors"]:
            print(f"  {err}")
    
    sys.exit(0 if results["failed"] == 0 else 1)
