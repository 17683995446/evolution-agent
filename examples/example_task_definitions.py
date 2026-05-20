"""
示例任务定义集
提供多种示例任务处理器，用于演示和测试Agent系统的完整功能。
包含：数据处理、文本处理、数学计算、模拟I/O等典型场景。
"""

from __future__ import annotations

import hashlib
import json
import math
import random
import re
import string
import time
from typing import Any, Dict, List

from shared.shared_data_models import TaskDefinition, TaskPriority


# ══════════════════════════════════════════════════════════════
#  任务处理器函数
# ══════════════════════════════════════════════════════════════

def process_data_transformation(data: Any, transform_type: str = "uppercase") -> Dict[str, Any]:
    """数据处理任务：对输入数据进行各种转换"""
    result = {"original": data, "transform_type": transform_type}

    if isinstance(data, str):
        if transform_type == "uppercase":
            result["output"] = data.upper()
        elif transform_type == "reverse":
            result["output"] = data[::-1]
        elif transform_type == "hash":
            result["output"] = hashlib.md5(data.encode()).hexdigest()
        elif transform_type == "word_count":
            words = re.findall(r'\w+', data)
            result["output"] = {"word_count": len(words), "unique_words": len(set(words))}
        else:
            result["output"] = data
    elif isinstance(data, list):
        if transform_type == "sort":
            result["output"] = sorted(data)
        elif transform_type == "deduplicate":
            result["output"] = list(dict.fromkeys(data))
        elif transform_type == "sum":
            result["output"] = sum(data) if all(isinstance(x, (int, float)) for x in data) else None
        else:
            result["output"] = data
    else:
        result["output"] = str(data)

    result["processed"] = True
    return result


def calculate_math_operations(expression: str, precision: int = 6) -> Dict[str, Any]:
    """数学计算任务：执行数学运算"""
    result = {"expression": expression}

    try:
        # 安全的数学表达式求值
        allowed = set(string.digits + ".+-*/() sqrt log sin cos tan pi e ")
        safe_expr = expression.replace("sqrt", "math.sqrt").replace("log", "math.log") \
                              .replace("sin", "math.sin").replace("cos", "math.cos") \
                              .replace("tan", "math.tan").replace("pi", "math.pi") \
                              .replace("e", "math.e")

        value = eval(safe_expr, {"__builtins__": {}, "math": math})  # noqa: S307
        result["value"] = round(value, precision)
        result["success"] = True
    except Exception as e:
        result["error"] = str(e)
        result["success"] = False

    return result


def generate_random_dataset(size: int = 100, value_range: tuple = (0, 1000)) -> Dict[str, Any]:
    """生成随机数据集任务"""
    data = [random.randint(value_range[0], value_range[1]) for _ in range(size)]

    return {
        "size": size,
        "data": data,
        "statistics": {
            "min": min(data),
            "max": max(data),
            "mean": sum(data) / len(data),
            "median": sorted(data)[len(data) // 2],
        },
    }


def simulate_io_operation(duration_ms: int = 100, fail_probability: float = 0.0) -> Dict[str, Any]:
    """模拟I/O操作任务（用于测试超时和重试机制）"""
    start = time.time()
    actual_duration = duration_ms / 1000.0

    # 模拟I/O延迟
    time.sleep(actual_duration)

    # 模拟随机失败
    if random.random() < fail_probability:
        raise ConnectionError(f"模拟I/O错误 (fail_prob={fail_probability})")

    return {
        "operation": "io_simulation",
        "requested_duration_ms": duration_ms,
        "actual_duration_ms": (time.time() - start) * 1000,
        "success": True,
    }


def analyze_text_content(text: str, analysis_type: str = "full") -> Dict[str, Any]:
    """文本分析任务"""
    result = {"text_length": len(text), "analysis_type": analysis_type}

    if analysis_type in ("full", "basic"):
        result.update({
            "char_count": len(text),
            "word_count": len(text.split()),
            "line_count": text.count('\n') + 1,
            "paragraph_count": len([p for p in text.split('\n\n') if p.strip()]),
        })

    if analysis_type == "full":
        # 字符频率分析
        char_freq = {}
        for char in text.lower():
            if char.isalpha():
                char_freq[char] = char_freq.get(char, 0) + 1
        top_chars = sorted(char_freq.items(), key=lambda x: x[1], reverse=True)[:10]
        result["top_characters"] = top_chars

        # 句子分析
        sentences = re.split(r'[.!?]+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        result["sentence_count"] = len(sentences)
        result["avg_sentence_length"] = (
            sum(len(s.split()) for s in sentences) / len(sentences) if sentences else 0
        )

    return result


def fibonacci_sequence(n: int = 10) -> Dict[str, Any]:
    """斐波那契数列计算任务"""
    if n <= 0:
        return {"sequence": [], "sum": 0}
    if n == 1:
        return {"sequence": [0], "sum": 0}

    seq = [0, 1]
    for i in range(2, n):
        seq.append(seq[-1] + seq[-2])

    return {
        "sequence": seq,
        "length": len(seq),
        "sum": sum(seq),
        "last_value": seq[-1],
    }


def json_data_validator(data: Dict[str, Any], schema: Dict[str, str]) -> Dict[str, Any]:
    """JSON数据校验任务"""
    errors = []
    validated = {}

    for field_name, field_type in schema.items():
        if field_name not in data:
            errors.append(f"缺少字段: {field_name}")
            continue

        value = data[field_name]
        type_ok = True

        if field_type == "string" and not isinstance(value, str):
            type_ok = False
        elif field_type == "integer" and not isinstance(value, int):
            type_ok = False
        elif field_type == "number" and not isinstance(value, (int, float)):
            type_ok = False
        elif field_type == "boolean" and not isinstance(value, bool):
            type_ok = False
        elif field_type == "array" and not isinstance(value, list):
            type_ok = False
        elif field_type == "object" and not isinstance(value, dict):
            type_ok = False

        if type_ok:
            validated[field_name] = value
        else:
            errors.append(f"字段 '{field_name}' 类型错误: 期望 {field_type}, 实际 {type(value).__name__}")

    return {
        "is_valid": len(errors) == 0,
        "errors": errors,
        "validated_fields": validated,
        "total_fields": len(schema),
        "valid_fields": len(validated),
    }


# ══════════════════════════════════════════════════════════════
#  示例任务生成器
# ══════════════════════════════════════════════════════════════

def create_sample_task_batch(count: int = 10) -> List[TaskDefinition]:
    """创建一批示例任务，用于演示"""
    task_templates = [
        TaskDefinition(
            name="数据转换-大写",
            description="将文本转换为大写",
            handler="process_data_transformation",
            handler_params={"data": "hello evolution agent", "transform_type": "uppercase"},
            priority=TaskPriority.NORMAL,
        ),
        TaskDefinition(
            name="数学计算-平方根",
            description="计算平方根",
            handler="calculate_math_operations",
            handler_params={"expression": "sqrt(144)", "precision": 4},
            priority=TaskPriority.NORMAL,
        ),
        TaskDefinition(
            name="生成随机数据集",
            description="生成随机数据集并统计",
            handler="generate_random_dataset",
            handler_params={"size": 200, "value_range": (1, 500)},
            priority=TaskPriority.LOW,
        ),
        TaskDefinition(
            name="模拟I/O操作",
            description="模拟网络I/O延迟",
            handler="simulate_io_operation",
            handler_params={"duration_ms": 50, "fail_probability": 0.1},
            priority=TaskPriority.NORMAL,
            max_retries=5,
        ),
        TaskDefinition(
            name="文本分析",
            description="分析文本内容特征",
            handler="analyze_text_content",
            handler_params={
                "text": "The Evolution Agent is a self-improving system. "
                        "It continuously executes tasks and optimizes its performance.",
                "analysis_type": "full",
            },
            priority=TaskPriority.NORMAL,
        ),
        TaskDefinition(
            name="斐波那契计算",
            description="计算斐波那契数列",
            handler="fibonacci_sequence",
            handler_params={"n": 20},
            priority=TaskPriority.LOW,
        ),
        TaskDefinition(
            name="数据校验",
            description="校验JSON数据格式",
            handler="json_data_validator",
            handler_params={
                "data": {"name": "test", "age": 25, "active": True, "score": 98.5},
                "schema": {"name": "string", "age": "integer", "active": "boolean", "score": "number"},
            },
            priority=TaskPriority.HIGH,
        ),
        TaskDefinition(
            name="数据转换-排序去重",
            description="对列表排序并去重",
            handler="process_data_transformation",
            handler_params={
                "data": [3, 1, 4, 1, 5, 9, 2, 6, 5, 3, 5],
                "transform_type": "deduplicate",
            },
            priority=TaskPriority.NORMAL,
        ),
        TaskDefinition(
            name="数学计算-三角函数",
            description="计算三角函数值",
            handler="calculate_math_operations",
            handler_params={"expression": "sin(pi/6)", "precision": 6},
            priority=TaskPriority.LOW,
        ),
        TaskDefinition(
            name="数据转换-哈希",
            description="计算文本哈希值",
            handler="process_data_transformation",
            handler_params={"data": "evolution-agent-v1", "transform_type": "hash"},
            priority=TaskPriority.NORMAL,
        ),
    ]

    # 循环生成指定数量的任务
    tasks = []
    for i in range(count):
        template = task_templates[i % len(task_templates)]
        task = TaskDefinition(
            name=f"[周期任务-{i+1}] {template.name}",
            description=template.description,
            handler=template.handler,
            handler_params=template.handler_params,
            priority=template.priority,
            max_retries=template.max_retries,
            tags=["sample", f"batch-{i // len(task_templates) + 1}"],
        )
        tasks.append(task)

    return tasks


# ══════════════════════════════════════════════════════════════
#  处理器注册辅助函数
# ══════════════════════════════════════════════════════════════

ALL_HANDLERS = {
    "process_data_transformation": process_data_transformation,
    "calculate_math_operations": calculate_math_operations,
    "generate_random_dataset": generate_random_dataset,
    "simulate_io_operation": simulate_io_operation,
    "analyze_text_content": analyze_text_content,
    "fibonacci_sequence": fibonacci_sequence,
    "json_data_validator": json_data_validator,
}


def register_all_sample_handlers(handler_registry) -> None:
    """将所有示例处理器注册到执行引擎"""
    for name, handler in ALL_HANDLERS.items():
        handler_registry.register(name, handler)
