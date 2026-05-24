"""
简单机器学习预测器
使用统计方法和简单启发式规则预测任务执行结果
"""
import time
import statistics
from typing import Dict, Any, List, Optional
from collections import deque


class SimpleMLPredictor:
    """
    简单ML预测器
    
    使用以下特征预测任务失败概率：
    1. 历史成功率趋势
    2. 最近N次执行结果
    3. 执行时长异常检测
    4. 错误类型模式
    """
    
    def __init__(self, window_size: int = 50):
        self._window_size = window_size
        self._features: Dict[str, Dict[str, Any]] = {}
    
    def extract_features(self, handler_name: str, 
                         history: List[Dict[str, Any]]) -> Dict[str, float]:
        """提取特征向量"""
        if len(history) < 5:
            return {"confidence": 0.0, "failure_prob": 0.5}
        
        # 最近N次结果
        recent = list(history)[-self._window_size:]
        
        # 特征1：成功率趋势
        recent_success_rate = sum(1 for r in recent if r["success"]) / len(recent)
        
        # 特征2：连续失败次数
        consecutive_failures = 0
        for r in reversed(recent):
            if not r["success"]:
                consecutive_failures += 1
            else:
                break
        
        # 特征3：执行时长异常
        durations = [r["duration"] for r in recent]
        avg_duration = statistics.mean(durations)
        std_duration = statistics.stdev(durations) if len(durations) > 1 else 0
        last_duration = recent[-1]["duration"] if recent else 0
        duration_zscore = (last_duration - avg_duration) / std_duration if std_duration > 0 else 0
        
        # 特征4：错误类型熵（错误类型越多样，系统越不稳定）
        error_types = [r["error_type"] for r in recent if r["error_type"]]
        unique_errors = len(set(error_types)) if error_types else 0
        error_entropy = unique_errors / len(error_types) if error_types else 0
        
        return {
            "recent_success_rate": recent_success_rate,
            "consecutive_failures": consecutive_failures,
            "duration_zscore": duration_zscore,
            "error_entropy": error_entropy,
            "sample_size": len(recent),
        }
    
    def predict(self, handler_name: str, 
                history: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        预测任务执行结果
        
        返回：
        - failure_probability: 失败概率 (0-1)
        - confidence: 预测置信度 (0-1)
        - factors: 影响因子
        """
        features = self.extract_features(handler_name, history)
        
        if features.get("confidence") == 0.0:
            return {
                "failure_probability": 0.5,
                "confidence": 0.0,
                "factors": {"insufficient_data": True},
            }
        
        # 简单线性模型（权重可调整）
        weights = {
            "recent_success_rate": -0.4,  # 成功率越高，失败概率越低
            "consecutive_failures": 0.15,  # 连续失败增加失败概率
            "duration_zscore": 0.1,  # 执行时长异常增加失败概率
            "error_entropy": 0.05,  # 错误多样性增加失败概率
        }
        
        # 计算基础失败概率
        base_prob = 0.5
        for feature, weight in weights.items():
            if feature in features:
                base_prob += features[feature] * weight
        
        # 连续失败惩罚
        if features["consecutive_failures"] >= 3:
            base_prob += 0.2
        
        # 置信度计算（样本越多，置信度越高）
        confidence = min(features["sample_size"] / self._window_size, 1.0)
        
        failure_prob = max(0.0, min(1.0, base_prob))
        
        return {
            "failure_probability": failure_prob,
            "confidence": confidence,
            "factors": {
                "recent_success_rate": features["recent_success_rate"],
                "consecutive_failures": features["consecutive_failures"],
                "duration_zscore": features["duration_zscore"],
                "error_entropy": features["error_entropy"],
            },
        }
    
    def classify_risk_level(self, failure_prob: float, 
                           confidence: float) -> str:
        """分类风险等级"""
        if confidence < 0.3:
            return "unknown"
        
        if failure_prob < 0.2:
            return "low"
        elif failure_prob < 0.5:
            return "medium"
        elif failure_prob < 0.8:
            return "high"
        else:
            return "critical"
