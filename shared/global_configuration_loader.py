"""
全局配置加载器
集中管理Agent系统的所有配置参数，支持YAML/JSON文件加载与环境变量覆盖。
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional

import yaml


# ──────────────────────── 默认配置 ────────────────────────

DEFAULT_CONFIG: Dict[str, Any] = {
    "agent": {
        "name": "EvolutionAgent",
        "version": "1.0.0",
        "log_level": "INFO",
        "working_directory": "./workspace",
    },
    "execution": {
        "max_concurrent_tasks": 5,
        "task_timeout_seconds": 300.0,
        "max_retries_per_task": 3,
        "retry_delay_seconds": 2.0,
        "task_queue_max_size": 1000,
        "cycle_cooldown_seconds": 0.1,  # 周期间极短冷却，接近零空闲
    },
    "optimization": {
        "enabled": True,
        "analysis_interval_cycles": 1,   # 每个周期都分析
        "min_cycles_for_analysis": 1,
        "max_active_rules": 50,
        "rule_effectiveness_threshold": 0.3,
        "auto_deactivate_low_score": True,
        "low_score_deactivate_threshold": 0.1,
    },
    "guardian": {
        "health_check_interval_seconds": 10.0,
        "max_component_errors_before_restart": 5,
        "recovery_timeout_seconds": 30.0,
        "auto_restart_enabled": True,
    },
    "storage": {
        "metrics_retention_cycles": 100,
        "rules_persistence_path": "./workspace/rules.json",
        "metrics_persistence_path": "./workspace/metrics.json",
        "auto_persist_enabled": True,
        "persist_interval_cycles": 5,
    },
    "logging": {
        "console_enabled": True,
        "file_enabled": True,
        "file_path": "./workspace/logs/agent.log",
        "max_file_size_mb": 50,
        "backup_count": 5,
        "format_string": "%(asctime)s | %(name)-28s | %(levelname)-8s | %(message)s",
    },
}


class AgentConfig:
    """Agent全局配置管理器"""

    def __init__(self, config_dict: Optional[Dict[str, Any]] = None):
        self._config: Dict[str, Any] = json.loads(json.dumps(DEFAULT_CONFIG))
        if config_dict and isinstance(config_dict, dict):
            self._deep_merge(self._config, config_dict)
        self._apply_env_overrides()

    # ── 公共接口 ──

    def get(self, dot_path: str, default: Any = None) -> Any:
        """通过点分路径获取配置值，如 'execution.max_concurrent_tasks'"""
        keys = dot_path.split(".")
        value = self._config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value

    def set(self, dot_path: str, value: Any) -> None:
        """通过点分路径设置配置值"""
        keys = dot_path.split(".")
        target = self._config
        for key in keys[:-1]:
            if key not in target or not isinstance(target[key], dict):
                target[key] = {}
            target = target[key]
        target[keys[-1]] = value

    def section(self, name: str) -> Dict[str, Any]:
        """获取顶层配置段"""
        return self._config.get(name, {})

    def to_dict(self) -> Dict[str, Any]:
        """导出完整配置字典"""
        return json.loads(json.dumps(self._config))

    def save_to_file(self, file_path: str) -> None:
        """将当前配置保存到YAML文件"""
        path = Path(file_path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            yaml.dump(self._config, f, allow_unicode=True, default_flow_style=False)

    @classmethod
    def from_yaml(cls, file_path: str) -> "AgentConfig":
        """从YAML文件加载配置"""
        path = Path(file_path)
        if not path.exists():
            return cls()
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        return cls(data)

    @classmethod
    def from_json(cls, file_path: str) -> "AgentConfig":
        """从JSON文件加载配置"""
        path = Path(file_path)
        if not path.exists():
            return cls()
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except json.JSONDecodeError:
            data = {}
        return cls(data)

    # ── 内部方法 ──

    def _deep_merge(self, base: Dict, override: Dict) -> None:
        """递归合并字典，override覆盖base"""
        for key, value in override.items():
            if value is None:
                continue
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._deep_merge(base[key], value)
            else:
                base[key] = value

    def _apply_env_overrides(self) -> None:
        """从环境变量覆盖配置，格式：EVOLUTION_AGENT_<SECTION>_<KEY>"""
        prefix = "EVOLUTION_AGENT_"
        for env_key, env_value in os.environ.items():
            if env_key.startswith(prefix):
                config_path = env_key[len(prefix):].lower()
                # 将 SECTION_KEY 转为 section.key
                parts = config_path.split("_", 1)
                if len(parts) == 2:
                    dot_path = f"{parts[0]}.{parts[1]}"
                else:
                    dot_path = config_path
                # 尝试类型转换
                typed_value = self._parse_env_value(env_value)
                self.set(dot_path, typed_value)

    @staticmethod
    def _parse_env_value(value: str) -> Any:
        """智能解析环境变量值类型"""
        # 先尝试数字类型（避免 "0"/"1" 被错误解析为布尔值）
        try:
            return int(value)
        except ValueError:
            pass
        try:
            return float(value)
        except ValueError:
            pass
        # 再尝试布尔类型
        if value.lower() in ("true", "yes"):
            return True
        if value.lower() in ("false", "no"):
            return False
        return value

    def __repr__(self) -> str:
        return f"AgentConfig(name={self.get('agent.name')}, version={self.get('agent.version')})"
