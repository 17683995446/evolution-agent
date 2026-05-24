"""
插件生态系统 - 支持第三方扩展的动态加载与管理
"""
from .plugin_interface import (
    Plugin, PluginType, PluginState, PluginContext,
    TaskHandlerPlugin, AnalysisStrategyPlugin, SyncTargetPlugin
)
from .plugin_manager import PluginManager
from .plugin_sandbox import PluginSandbox
from .plugin_dependency_resolver import PluginDependencyResolver

__all__ = [
    "Plugin", "PluginType", "PluginState", "PluginContext",
    "TaskHandlerPlugin", "AnalysisStrategyPlugin", "SyncTargetPlugin",
    "PluginManager", "PluginSandbox", "PluginDependencyResolver",
]
