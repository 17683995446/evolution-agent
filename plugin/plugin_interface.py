"""
插件接口定义 - 定义所有插件必须实现的契约
"""
import abc
import time
from enum import Enum, auto
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Callable, TypeVar, Generic
from abc import ABC, abstractmethod


class PluginType(str, Enum):
    """插件类型"""
    TASK_HANDLER = "task_handler"           # 任务处理器插件
    ANALYSIS_STRATEGY = "analysis_strategy" # 分析策略插件
    SYNC_TARGET = "sync_target"             # 同步目标插件
    OBSERVABILITY = "observability"         # 可观测性插件
    SECURITY = "security"                   # 安全插件


class PluginState(str, Enum):
    """插件生命周期状态"""
    REGISTERED = "registered"       # 已注册
    LOADING = "loading"             # 加载中
    INITIALIZING = "initializing"   # 初始化中
    ACTIVE = "active"               # 活跃运行
    PAUSED = "paused"               # 暂停
    UNLOADING = "unloading"         # 卸载中
    ERROR = "error"                 # 错误状态
    UNLOADED = "unloaded"           # 已卸载


@dataclass
class PluginContext:
    """插件运行时上下文"""
    plugin_id: str
    plugin_name: str
    version: str
    config: Dict[str, Any] = field(default_factory=dict)
    data_dir: Optional[str] = None
    log_callback: Optional[Callable[[str, str], None]] = None
    event_emitter: Optional[Callable[[str, Dict[str, Any]], None]] = None
    
    def log(self, level: str, message: str):
        """插件日志记录"""
        if self.log_callback:
            self.log_callback(level, message)
    
    def emit_event(self, event_type: str, data: Dict[str, Any]):
        """插件事件发布"""
        if self.event_emitter:
            self.event_emitter(event_type, data)


@dataclass
class PluginMetadata:
    """插件元数据"""
    name: str
    version: str
    description: str = ""
    author: str = ""
    plugin_type: PluginType = PluginType.TASK_HANDLER
    dependencies: List[str] = field(default_factory=list)
    min_platform_version: str = "1.0.0"
    max_platform_version: str = ""
    entry_point: str = ""           # 入口类/函数
    config_schema: Dict[str, Any] = field(default_factory=dict)


class Plugin(ABC):
    """
    插件基类 - 所有插件必须继承此类
    
    生命周期:
    1. __init__() - 实例化（轻量级，不执行IO）
    2. initialize(context) - 初始化（可执行IO、分配资源）
    3. activate() - 激活（开始处理请求）
    4. [pause() -> resume()] - 可选暂停/恢复
    5. deactivate() - 停用（停止处理请求）
    6. destroy() - 销毁（释放所有资源）
    """
    
    def __init__(self):
        self._context: Optional[PluginContext] = None
        self._state = PluginState.REGISTERED
        self._metadata: Optional[PluginMetadata] = None
        self._created_at = time.time()
        self._activated_at: Optional[float] = None
        self._error_count = 0
        self._last_error: Optional[str] = None
    
    @property
    def state(self) -> PluginState:
        return self._state
    
    @property
    def context(self) -> Optional[PluginContext]:
        return self._context
    
    @property
    def metadata(self) -> Optional[PluginMetadata]:
        return self._metadata
    
    def set_metadata(self, metadata: PluginMetadata):
        """设置元数据（由PluginManager调用）"""
        self._metadata = metadata
    
    # ── 生命周期方法 ──
    
    def initialize(self, context: PluginContext) -> bool:
        """
        初始化插件
        
        Args:
            context: 插件运行时上下文
            
        Returns:
            bool: 初始化是否成功
        """
        self._context = context
        self._state = PluginState.INITIALIZING
        try:
            success = self._do_initialize()
            if success:
                self._state = PluginState.INITIALIZING
            else:
                self._state = PluginState.ERROR
            return success
        except Exception as e:
            self._state = PluginState.ERROR
            self._last_error = str(e)
            self._error_count += 1
            if context:
                context.log("ERROR", f"初始化失败: {e}")
            return False
    
    @abstractmethod
    def _do_initialize(self) -> bool:
        """子类实现具体的初始化逻辑"""
        pass
    
    def activate(self) -> bool:
        """激活插件"""
        if self._state not in [PluginState.INITIALIZING, PluginState.PAUSED]:
            return False
        try:
            success = self._do_activate()
            if success:
                self._state = PluginState.ACTIVE
                self._activated_at = time.time()
            return success
        except Exception as e:
            self._last_error = str(e)
            self._error_count += 1
            return False
    
    def _do_activate(self) -> bool:
        """子类可重写激活逻辑"""
        return True
    
    def pause(self) -> bool:
        """暂停插件"""
        if self._state != PluginState.ACTIVE:
            return False
        try:
            success = self._do_pause()
            if success:
                self._state = PluginState.PAUSED
            return success
        except Exception as e:
            self._last_error = str(e)
            return False
    
    def _do_pause(self) -> bool:
        """子类可重写暂停逻辑"""
        return True
    
    def resume(self) -> bool:
        """恢复插件"""
        if self._state != PluginState.PAUSED:
            return False
        return self.activate()
    
    def deactivate(self) -> bool:
        """停用插件"""
        if self._state not in [PluginState.ACTIVE, PluginState.PAUSED]:
            return False
        try:
            success = self._do_deactivate()
            if success:
                self._state = PluginState.UNLOADING
            return success
        except Exception as e:
            self._last_error = str(e)
            return False
    
    def _do_deactivate(self) -> bool:
        """子类可重写停用逻辑"""
        return True
    
    def destroy(self) -> bool:
        """销毁插件，释放所有资源"""
        if self._state == PluginState.ACTIVE:
            self.deactivate()
        try:
            success = self._do_destroy()
            self._state = PluginState.UNLOADED
            return success
        except Exception as e:
            self._last_error = str(e)
            self._state = PluginState.ERROR
            return False
    
    @abstractmethod
    def _do_destroy(self) -> bool:
        """子类实现具体的销毁逻辑"""
        pass
    
    def get_health(self) -> Dict[str, Any]:
        """获取插件健康状态"""
        return {
            "state": self._state.value,
            "error_count": self._error_count,
            "last_error": self._last_error,
            "uptime": time.time() - self._activated_at if self._activated_at else 0,
            "created_at": self._created_at,
        }


# ── 特化插件接口 ──

class TaskHandlerPlugin(Plugin):
    """任务处理器插件 - 提供自定义任务处理能力"""
    
    @abstractmethod
    def can_handle(self, task_type: str, params: Dict[str, Any]) -> bool:
        """判断是否能处理该类型任务"""
        pass
    
    @abstractmethod
    def handle(self, task_type: str, params: Dict[str, Any]) -> Any:
        """执行任务处理"""
        pass
    
    @abstractmethod
    def get_supported_types(self) -> List[str]:
        """返回支持的任务类型列表"""
        pass


class AnalysisStrategyPlugin(Plugin):
    """分析策略插件 - 提供自定义优化分析策略"""
    
    @abstractmethod
    def analyze(self, metrics: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        分析指标数据，返回优化建议
        
        Returns:
            List[Dict]: 优化建议列表，每个建议包含:
                - type: 建议类型
                - priority: 优先级 (1-10)
                - description: 描述
                - action: 建议动作
        """
        pass
    
    @abstractmethod
    def get_analysis_interval(self) -> int:
        """返回分析间隔（周期数）"""
        pass


class SyncTargetPlugin(Plugin):
    """同步目标插件 - 提供自定义规则同步目标"""
    
    @abstractmethod
    def sync(self, rules: List[Dict[str, Any]]) -> bool:
        """同步规则到目标"""
        pass
    
    @abstractmethod
    def validate_connection(self) -> bool:
        """验证与目标的连接"""
        pass
    
    @abstractmethod
    def get_target_info(self) -> Dict[str, str]:
        """获取目标信息"""
        pass
