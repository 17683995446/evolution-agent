"""
智能自适应调度器
基于历史数据动态调整执行策略
"""
import time
import statistics
import threading
from typing import Dict, Any, List, Optional, Tuple
from dataclasses import dataclass
from collections import deque


@dataclass
class ExecutionPattern:
    """执行模式特征"""
    handler_name: str
    avg_duration: float
    success_rate: float
    failure_pattern: str  # 'random', 'burst', 'time_based', 'stable'
    optimal_concurrency: int
    optimal_timeout: float
    recommended_retries: int


@dataclass
class AdaptiveConfig:
    """自适应配置"""
    min_concurrent_tasks: int = 1
    max_concurrent_tasks: int = 20
    min_timeout_seconds: float = 1.0
    max_timeout_seconds: float = 300.0
    adjustment_interval_cycles: int = 10
    learning_rate: float = 0.1
    confidence_threshold: float = 0.7


class AdaptiveScheduler:
    """
    智能自适应调度器
    
    核心功能：
    1. 历史数据分析：识别任务执行模式
    2. 动态参数调整：根据模式调整并发度、超时、重试
    3. 预测性调度：预测高失败概率任务，提前调整策略
    """
    
    def __init__(self, config: AdaptiveConfig = None):
        self._config = config or AdaptiveConfig()
        self._execution_history: Dict[str, deque] = {}  # handler_name -> deque of results
        self._patterns: Dict[str, ExecutionPattern] = {}
        self._current_params = {
            "concurrent_tasks": 5,
            "timeout_seconds": 30.0,
            "max_retries": 3,
        }
        self._cycle_count = 0
        self._adjustment_log: deque = deque(maxlen=200)
        self._lock = threading.RLock()  # 可重入锁：adjust_parameters持有锁时调用analyze_patterns
    
    def record_execution(self, handler_name: str, duration: float, 
                         success: bool, error_type: Optional[str] = None):
        """记录任务执行结果"""
        with self._lock:
            if handler_name not in self._execution_history:
                self._execution_history[handler_name] = deque(maxlen=100)
            
            self._execution_history[handler_name].append({
                "timestamp": time.time(),
                "duration": duration,
                "success": success,
                "error_type": error_type,
            })
    
    def analyze_patterns(self) -> Dict[str, ExecutionPattern]:
        """分析执行模式"""
        with self._lock:
            patterns = {}
            
            for handler_name, history in self._execution_history.items():
                if len(history) < 10:
                    continue
                
                durations = [r["duration"] for r in history]
                successes = [r["success"] for r in history]
                error_types = [r["error_type"] for r in history if r["error_type"]]
                
                avg_duration = statistics.mean(durations)
                success_rate = sum(successes) / len(successes)
                
                # 识别失败模式
                failure_pattern = self._detect_failure_pattern(history)
                
                # 计算最优参数
                optimal_concurrency = self._calculate_optimal_concurrency(
                    handler_name, durations, success_rate
                )
                optimal_timeout = self._calculate_optimal_timeout(durations, success_rate)
                recommended_retries = self._calculate_recommended_retries(success_rate)
                
                patterns[handler_name] = ExecutionPattern(
                    handler_name=handler_name,
                    avg_duration=avg_duration,
                    success_rate=success_rate,
                    failure_pattern=failure_pattern,
                    optimal_concurrency=optimal_concurrency,
                    optimal_timeout=optimal_timeout,
                    recommended_retries=recommended_retries,
                )
            
            self._patterns = patterns
            return patterns
    
    def _detect_failure_pattern(self, history: deque) -> str:
        """检测失败模式"""
        results = list(history)
        failures = [i for i, r in enumerate(results) if not r["success"]]
        
        if len(failures) == 0:
            return "stable"
        
        if len(failures) == len(results):
            return "always_fail"
        
        # 检查是否是突发模式（连续失败）
        consecutive_failures = 0
        max_consecutive = 0
        for r in results:
            if not r["success"]:
                consecutive_failures += 1
                max_consecutive = max(max_consecutive, consecutive_failures)
            else:
                consecutive_failures = 0
        
        if max_consecutive >= 5:
            return "burst"
        
        # 检查时间模式
        failure_times = [results[i]["timestamp"] for i in failures]
        if len(failure_times) >= 3:
            intervals = [failure_times[i+1] - failure_times[i] 
                        for i in range(len(failure_times)-1)]
            if len(set(int(interval) // 60 for interval in intervals)) == 1:
                return "time_based"
        
        return "random"
    
    def _calculate_optimal_concurrency(self, handler_name: str, 
                                       durations: List[float], 
                                       success_rate: float) -> int:
        """计算最优并发度"""
        base_concurrency = 5
        
        # 根据成功率调整
        if success_rate > 0.95:
            base_concurrency = min(base_concurrency + 5, self._config.max_concurrent_tasks)
        elif success_rate < 0.7:
            base_concurrency = max(base_concurrency - 2, self._config.min_concurrent_tasks)
        
        # 根据平均耗时调整（短任务可以增加并发）
        avg_duration = statistics.mean(durations)
        if avg_duration < 0.1:  # 小于100ms
            base_concurrency = min(base_concurrency + 3, self._config.max_concurrent_tasks)
        elif avg_duration > 10.0:  # 大于10秒
            base_concurrency = max(base_concurrency - 2, self._config.min_concurrent_tasks)
        
        return base_concurrency
    
    def _calculate_optimal_timeout(self, durations: List[float], 
                                   success_rate: float) -> float:
        """计算最优超时时间"""
        if len(durations) < 5:
            return 30.0
        
        # 使用P95作为基础
        sorted_durations = sorted(durations)
        p95_idx = int(len(sorted_durations) * 0.95)
        p95_duration = sorted_durations[min(p95_idx, len(sorted_durations)-1)]
        
        # 添加安全边际
        optimal_timeout = p95_duration * 1.5
        
        # 根据成功率调整
        if success_rate < 0.5:
            # 失败率高，可能是超时导致，增加超时时间
            optimal_timeout *= 1.5
        
        return max(
            self._config.min_timeout_seconds,
            min(optimal_timeout, self._config.max_timeout_seconds)
        )
    
    def _calculate_recommended_retries(self, success_rate: float) -> int:
        """计算推荐重试次数"""
        if success_rate > 0.95:
            return 1  # 成功率高，减少重试
        elif success_rate > 0.8:
            return 2
        elif success_rate > 0.5:
            return 3
        else:
            return 5  # 成功率低，增加重试
    
    def adjust_parameters(self) -> Dict[str, Any]:
        """调整执行参数"""
        with self._lock:
            self._cycle_count += 1
            
            if self._cycle_count % self._config.adjustment_interval_cycles != 0:
                return self._current_params
            
            # 分析模式
            patterns = self.analyze_patterns()
            
            if not patterns:
                return self._current_params
            
            # 计算加权平均参数
            total_weight = 0
            weighted_concurrency = 0
            weighted_timeout = 0
            weighted_retries = 0
            
            for pattern in patterns.values():
                weight = pattern.success_rate  # 成功率高的模式权重更大
                total_weight += weight
                weighted_concurrency += pattern.optimal_concurrency * weight
                weighted_timeout += pattern.optimal_timeout * weight
                weighted_retries += pattern.recommended_retries * weight
            
            if total_weight > 0:
                new_params = {
                    "concurrent_tasks": int(weighted_concurrency / total_weight),
                    "timeout_seconds": weighted_timeout / total_weight,
                    "max_retries": int(weighted_retries / total_weight),
                }
                
                # 保存旧参数快照
                old_params_snapshot = dict(self._current_params)

                # 平滑过渡（使用学习率）
                for key in new_params:
                    old_val = self._current_params[key]
                    new_val = new_params[key]
                    self._current_params[key] = (
                        old_val * (1 - self._config.learning_rate) + 
                        new_val * self._config.learning_rate
                    )

                # 记录调整
                self._adjustment_log.append({
                    "cycle": self._cycle_count,
                    "old_params": old_params_snapshot,
                    "new_params": dict(self._current_params),
                    "patterns_analyzed": len(patterns),
                })
            
            return dict(self._current_params)
    
    def predict_failure_probability(self, handler_name: str, 
                                    current_load: float) -> float:
        """
        预测任务失败概率
        
        基于：
        1. 历史成功率
        2. 当前系统负载
        3. 时间模式
        """
        if handler_name not in self._patterns:
            return 0.5  # 未知任务，默认50%
        
        pattern = self._patterns[handler_name]
        
        # 基础失败概率
        base_failure_prob = 1 - pattern.success_rate
        
        # 负载因子（负载越高，失败概率越高）
        load_factor = min(current_load * 0.3, 0.3)  # 最多增加30%
        
        # 时间因子（某些时段失败率更高）
        time_factor = 0.0
        if pattern.failure_pattern == "time_based":
            current_hour = time.localtime().tm_hour
            if current_hour in [2, 3, 4]:  # 凌晨时段
                time_factor = 0.1
        
        failure_prob = base_failure_prob + load_factor + time_factor
        return min(failure_prob, 0.99)  # 最高99%
    
    def get_recommended_strategy(self, handler_name: str) -> Dict[str, Any]:
        """获取推荐执行策略"""
        if handler_name not in self._patterns:
            return {
                "concurrent_tasks": 5,
                "timeout_seconds": 30.0,
                "max_retries": 3,
                "failure_probability": 0.5,
                "recommendation": "unknown_handler",
            }
        
        pattern = self._patterns[handler_name]
        failure_prob = self.predict_failure_probability(handler_name, 0.5)
        
        recommendation = "normal"
        if pattern.failure_pattern == "burst":
            recommendation = "circuit_breaker"  # 建议熔断
        elif pattern.success_rate < 0.3:
            recommendation = "deprecate"  # 建议弃用
        elif failure_prob > 0.7:
            recommendation = "isolate"  # 建议隔离
        
        return {
            "concurrent_tasks": pattern.optimal_concurrency,
            "timeout_seconds": pattern.optimal_timeout,
            "max_retries": pattern.recommended_retries,
            "failure_probability": failure_prob,
            "recommendation": recommendation,
        }
    
    def get_stats(self) -> Dict[str, Any]:
        """获取调度器统计"""
        with self._lock:
            return {
                "cycle_count": self._cycle_count,
                "handlers_tracked": len(self._execution_history),
                "patterns_identified": len(self._patterns),
                "current_params": dict(self._current_params),
                "adjustments_made": len(self._adjustment_log),
                "adjustment_history": list(self._adjustment_log)[-5:],  # 最近5次调整
            }
