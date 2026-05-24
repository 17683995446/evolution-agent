"""
插件管理器 - 负责插件的加载、初始化、激活、停用、卸载全生命周期管理
"""
import os
import sys
import json
import time
import importlib
import importlib.util
import threading
import types
from pathlib import Path
from typing import Dict, List, Optional, Type, Any, Callable
from collections import defaultdict

try:
    # 尝试相对导入（包内使用）
    from .plugin_interface import (
        Plugin, PluginType, PluginState, PluginContext, PluginMetadata,
        TaskHandlerPlugin, AnalysisStrategyPlugin, SyncTargetPlugin
    )
    from ..shared.unified_logging_service import AgentLoggingService
    from ..shared.shared_data_models import SystemEvent, EventType
except ImportError:
    # 回退到绝对导入（直接运行测试文件）
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from plugin.plugin_interface import (
        Plugin, PluginType, PluginState, PluginContext, PluginMetadata,
        TaskHandlerPlugin, AnalysisStrategyPlugin, SyncTargetPlugin
    )
    from shared.unified_logging_service import AgentLoggingService
    from shared.shared_data_models import SystemEvent, EventType


class PluginManager:
    """
    插件管理器
    
    职责:
    1. 插件发现与加载
    2. 插件生命周期管理
    3. 插件依赖解析与加载顺序控制
    4. 插件健康监控
    5. 热加载/热卸载支持
    """
    
    def __init__(self, config: Dict[str, Any], logging_service: AgentLoggingService):
        self._config = config
        self._log = logging_service
        self._plugins: Dict[str, Plugin] = {}           # plugin_id -> Plugin
        self._plugins_by_type: Dict[PluginType, List[str]] = defaultdict(list)
        self._metadata_cache: Dict[str, PluginMetadata] = {}  # plugin_id -> Metadata
        self._lock = threading.RLock()
        self._watchdog_thread: Optional[threading.Thread] = None
        self._is_running = False
        self._hot_reload_enabled = config.get("hot_reload_enabled", False)
        self._plugin_dirs = config.get("plugin_directories", ["./plugins"])
        self._data_dir = config.get("plugin_data_directory", "./plugin_data")
        
        # 确保数据目录存在
        os.makedirs(self._data_dir, exist_ok=True)
        
        self._log.info("plugin_manager", "插件管理器初始化完成")
    
    # ── 插件发现与加载 ──
    
    def discover_plugins(self, directory: Optional[str] = None) -> List[PluginMetadata]:
        """
        发现指定目录下的所有插件
        
        Returns:
            List[PluginMetadata]: 发现的插件元数据列表
        """
        discovered = []
        search_dirs = [directory] if directory else self._plugin_dirs
        
        for plugin_dir in search_dirs:
            if not os.path.exists(plugin_dir):
                continue
                
            for item in os.listdir(plugin_dir):
                item_path = os.path.join(plugin_dir, item)
                
                # 检查是否为插件目录或文件
                if os.path.isdir(item_path):
                    manifest_path = os.path.join(item_path, "plugin.json")
                    if os.path.exists(manifest_path):
                        try:
                            metadata = self._load_manifest(manifest_path)
                            discovered.append(metadata)
                            self._log.debug("plugin_manager", f"发现插件: {metadata.name}@{metadata.version}")
                        except Exception as e:
                            self._log.warning("plugin_manager", f"加载插件清单失败 {item}: {e}")
                
                elif item.endswith(".py") and not item.startswith("_"):
                    # 单文件插件
                    try:
                        metadata = self._load_single_file_plugin(item_path)
                        discovered.append(metadata)
                    except Exception as e:
                        self._log.warning("plugin_manager", f"加载单文件插件失败 {item}: {e}")
        
        return discovered
    
    def _load_manifest(self, manifest_path: str) -> PluginMetadata:
        """加载插件清单文件"""
        with open(manifest_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        return PluginMetadata(
            name=data["name"],
            version=data["version"],
            description=data.get("description", ""),
            author=data.get("author", ""),
            plugin_type=PluginType(data.get("type", "task_handler")),
            dependencies=data.get("dependencies", []),
            min_platform_version=data.get("min_platform_version", "1.0.0"),
            max_platform_version=data.get("max_platform_version", ""),
            entry_point=data.get("entry_point", ""),
            config_schema=data.get("config_schema", {})
        )
    
    def _load_single_file_plugin(self, file_path: str) -> PluginMetadata:
        """从单文件加载插件元数据"""
        # 从文件内容解析元数据（通过AST或正则）
        # 简化实现：假设文件名即插件名
        name = os.path.basename(file_path)[:-3]  # 去掉.py
        return PluginMetadata(
            name=name,
            version="1.0.0",
            plugin_type=PluginType.TASK_HANDLER,
            entry_point=f"{name}.PluginClass"
        )
    
    def load_plugin(self, plugin_path: str, metadata: Optional[PluginMetadata] = None) -> Optional[str]:
        """
        加载插件（外部调用时使用锁）
        
        Args:
            plugin_path: 插件路径（目录或文件）
            metadata: 插件元数据（可选，如不提供则从plugin.json加载）
            
        Returns:
            str: 插件ID，加载失败返回None
        """
        with self._lock:
            return self._load_plugin_internal(plugin_path, metadata)
    
    def _load_plugin_internal(self, plugin_path: str, metadata: Optional[PluginMetadata] = None) -> Optional[str]:
        """
        加载插件内部实现（不获取锁，供内部调用）
        """
        try:
            if metadata is None:
                if os.path.isdir(plugin_path):
                    # 目录插件：从 plugin.json 加载
                    manifest_path = os.path.join(plugin_path, "plugin.json")
                    metadata = self._load_manifest(manifest_path)
                else:
                    # 单文件插件：从同名 .json 文件加载，或使用默认值
                    manifest_path = plugin_path.replace(".py", ".json")
                    if os.path.exists(manifest_path):
                        metadata = self._load_manifest(manifest_path)
                    else:
                        metadata = self._load_single_file_plugin(plugin_path)
            
            plugin_id = f"{metadata.name}@{metadata.version}"
            
            # 检查是否已加载
            if plugin_id in self._plugins:
                self._log.warning("plugin_manager", f"插件 {plugin_id} 已加载")
                return plugin_id
            
            # 加载插件模块
            plugin_class = self._load_plugin_class(plugin_path, metadata)
            if plugin_class is None:
                return None
            
            # 实例化插件
            plugin = plugin_class()
            plugin.set_metadata(metadata)
            
            # 创建插件上下文
            plugin_data_dir = os.path.join(self._data_dir, metadata.name)
            os.makedirs(plugin_data_dir, exist_ok=True)
            
            context = PluginContext(
                plugin_id=plugin_id,
                plugin_name=metadata.name,
                version=metadata.version,
                config=self._config.get("plugins", {}).get(metadata.name, {}),
                data_dir=plugin_data_dir,
                log_callback=lambda lvl, msg: self._log.log("plugin", f"[{metadata.name}] {msg}", level=lvl),
                event_emitter=lambda et, data: self._emit_plugin_event(plugin_id, et, data)
            )
            
            # 初始化插件
            if not plugin.initialize(context):
                self._log.error("plugin_manager", f"插件 {plugin_id} 初始化失败")
                return None
            
            # 存储插件
            self._plugins[plugin_id] = plugin
            self._metadata_cache[plugin_id] = metadata
            self._plugins_by_type[metadata.plugin_type].append(plugin_id)
            
            self._log.info("plugin_manager", f"插件 {plugin_id} 加载成功")
            return plugin_id
            
        except Exception as e:
            self._log.error("plugin_manager", f"加载插件失败 {plugin_path}: {e}")
            return None
    
    def _load_plugin_class(self, plugin_path: str, metadata: PluginMetadata) -> Optional[Type[Plugin]]:
        """加载插件类"""
        try:
            if os.path.isdir(plugin_path):
                # 目录插件
                module_name = metadata.name
                init_file = os.path.join(plugin_path, "__init__.py")
                main_file = os.path.join(plugin_path, f"{metadata.name}.py")
                
                if os.path.exists(init_file):
                    # 有__init__.py的包结构
                    # 为了支持热重载，我们使用直接执行文件内容的方式
                    module = types.ModuleType(module_name)
                    module.__file__ = init_file
                    module.__path__ = [plugin_path]
                    module.__package__ = module_name
                    sys.modules[module_name] = module
                    
                    # 执行__init__.py
                    with open(init_file, "r", encoding="utf-8") as f:
                        init_code = compile(f.read(), init_file, "exec")
                    exec(init_code, module.__dict__)
                    
                elif os.path.exists(main_file):
                    # 没有__init__.py，直接从主文件加载
                    module = types.ModuleType(module_name)
                    module.__file__ = main_file
                    sys.modules[module_name] = module
                    
                    with open(main_file, "r", encoding="utf-8") as f:
                        code = compile(f.read(), main_file, "exec")
                    exec(code, module.__dict__)
                else:
                    return None
                
            else:
                # 单文件插件
                module_name = os.path.basename(plugin_path)[:-3]
                module = types.ModuleType(module_name)
                module.__file__ = plugin_path
                sys.modules[module_name] = module
                
                with open(plugin_path, "r", encoding="utf-8") as f:
                    code = compile(f.read(), plugin_path, "exec")
                exec(code, module.__dict__)
            
            # 获取插件类
            entry_point = metadata.entry_point or "Plugin"
            if "." in entry_point:
                class_name = entry_point.split(".")[-1]
            else:
                class_name = entry_point
            
            plugin_class = getattr(module, class_name, None)
            if plugin_class is None:
                self._log.error("plugin_manager", f"找不到插件类 {class_name}")
                return None
            
            return plugin_class
            
        except Exception as e:
            self._log.error("plugin_manager", f"加载插件类失败: {e}")
            return None
    
    def _emit_plugin_event(self, plugin_id: str, event_type: str, data: Dict[str, Any]):
        """发布插件事件"""
        event = SystemEvent(
            event_type=EventType.PLUGIN_EVENT,
            source=f"plugin:{plugin_id}",
            data={"event_type": event_type, "data": data}
        )
        self._log.emit_event(event)
    
    # ── 插件生命周期管理 ──
    
    def activate_plugin(self, plugin_id: str) -> bool:
        """激活插件"""
        with self._lock:
            plugin = self._plugins.get(plugin_id)
            if plugin is None:
                self._log.warning("plugin_manager", f"插件 {plugin_id} 不存在")
                return False
            
            success = plugin.activate()
            if success:
                self._log.info("plugin_manager", f"插件 {plugin_id} 已激活")
            else:
                self._log.error("plugin_manager", f"插件 {plugin_id} 激活失败")
            return success
    
    def deactivate_plugin(self, plugin_id: str) -> bool:
        """停用插件"""
        with self._lock:
            plugin = self._plugins.get(plugin_id)
            if plugin is None:
                return False
            
            success = plugin.deactivate()
            if success:
                self._log.info("plugin_manager", f"插件 {plugin_id} 已停用")
            return success
    
    def unload_plugin(self, plugin_id: str) -> bool:
        """卸载插件（外部调用时使用锁）"""
        with self._lock:
            return self._unload_plugin_internal(plugin_id)
    
    def _unload_plugin_internal(self, plugin_id: str) -> bool:
        """卸载插件内部实现（不获取锁，供内部调用）"""
        plugin = self._plugins.get(plugin_id)
        if plugin is None:
            return False
        
        # 停用并销毁
        if plugin.state == PluginState.ACTIVE:
            plugin.deactivate()
        
        success = plugin.destroy()
        
        # 从注册表中移除
        del self._plugins[plugin_id]
        if plugin_id in self._metadata_cache:
            metadata = self._metadata_cache[plugin_id]
            if plugin_id in self._plugins_by_type[metadata.plugin_type]:
                self._plugins_by_type[metadata.plugin_type].remove(plugin_id)
            del self._metadata_cache[plugin_id]
        
        self._log.info("plugin_manager", f"插件 {plugin_id} 已卸载")
        return success
    
    def reload_plugin(self, plugin_id: str) -> Optional[str]:
        """热重载插件"""
        with self._lock:
            if plugin_id not in self._plugins:
                return None
            
            metadata = self._metadata_cache.get(plugin_id)
            if metadata is None:
                return None
            
            # 查找插件路径
            plugin_path = self._find_plugin_path(metadata.name)
            if plugin_path is None:
                return None
            
            # 卸载旧版本（使用内部方法避免嵌套锁）
            # 注意：先卸载插件，让插件有机会清理资源
            self._unload_plugin_internal(plugin_id)
            
            # 清除Python模块缓存以实现热重载
            # 注意：在卸载之后清除，确保卸载过程中不会重新导入模块
            self._clear_module_cache(metadata.name)
            
            # 重新加载（使用内部方法避免嵌套锁）
            return self._load_plugin_internal(plugin_path, metadata)
    
    def _clear_module_cache(self, plugin_name: str):
        """清除插件相关的模块缓存"""
        # 清除主模块
        if plugin_name in sys.modules:
            del sys.modules[plugin_name]
        
        # 清除子模块（包括包内子模块和子包）
        prefix = f"{plugin_name}."
        modules_to_remove = [name for name in list(sys.modules.keys()) if name.startswith(prefix)]
        for name in modules_to_remove:
            del sys.modules[name]
        
        # 额外清除：如果插件名包含下划线，尝试清除可能的变体
        # 例如 hotreload_plugin 可能对应 hotreload_plugin.hotreload_plugin 子模块
        alt_prefix = f"{plugin_name}.{plugin_name}"
        if alt_prefix in sys.modules:
            del sys.modules[alt_prefix]
    
    def _find_plugin_path(self, plugin_name: str) -> Optional[str]:
        """查找插件路径"""
        for plugin_dir in self._plugin_dirs:
            if not os.path.exists(plugin_dir):
                continue
            for item in os.listdir(plugin_dir):
                if item == plugin_name or item == f"{plugin_name}.py":
                    return os.path.join(plugin_dir, item)
        return None
    
    # ── 插件查询 ──
    
    def get_plugin(self, plugin_id: str) -> Optional[Plugin]:
        """获取插件实例"""
        return self._plugins.get(plugin_id)
    
    def get_plugins_by_type(self, plugin_type: PluginType) -> List[Plugin]:
        """获取指定类型的所有插件"""
        with self._lock:
            return [
                self._plugins[pid]
                for pid in self._plugins_by_type.get(plugin_type, [])
                if pid in self._plugins
            ]
    
    def get_active_plugins(self) -> List[Plugin]:
        """获取所有活跃插件"""
        with self._lock:
            return [
                p for p in self._plugins.values()
                if p.state == PluginState.ACTIVE
            ]
    
    def get_plugin_health(self, plugin_id: str) -> Optional[Dict[str, Any]]:
        """获取插件健康状态"""
        plugin = self._plugins.get(plugin_id)
        if plugin:
            return plugin.get_health()
        return None
    
    def get_all_plugin_status(self) -> Dict[str, Dict[str, Any]]:
        """获取所有插件状态"""
        with self._lock:
            return {
                pid: {
                    "name": p.metadata.name if p.metadata else pid,
                    "version": p.metadata.version if p.metadata else "unknown",
                    "type": p.metadata.plugin_type.value if p.metadata else "unknown",
                    "state": p.state.value,
                    "health": p.get_health()
                }
                for pid, p in self._plugins.items()
            }
    
    # ── 批量操作 ──
    
    def load_all_discovered(self) -> Dict[str, str]:
        """加载所有发现的插件"""
        results = {}
        discovered = self.discover_plugins()
        
        for metadata in discovered:
            plugin_path = self._find_plugin_path(metadata.name)
            if plugin_path:
                plugin_id = self.load_plugin(plugin_path, metadata)
                if plugin_id:
                    results[metadata.name] = plugin_id
        
        return results
    
    def activate_all(self) -> Dict[str, bool]:
        """激活所有插件"""
        with self._lock:
            results = {}
            for plugin_id in list(self._plugins.keys()):
                results[plugin_id] = self.activate_plugin(plugin_id)
            return results
    
    def deactivate_all(self) -> Dict[str, bool]:
        """停用所有插件"""
        with self._lock:
            results = {}
            for plugin_id in list(self._plugins.keys()):
                results[plugin_id] = self.deactivate_plugin(plugin_id)
            return results
    
    def unload_all(self) -> Dict[str, bool]:
        """卸载所有插件"""
        with self._lock:
            results = {}
            for plugin_id in list(self._plugins.keys()):
                results[plugin_id] = self.unload_plugin(plugin_id)
            return results
    
    # ── 监控与维护 ──
    
    def start_watchdog(self, interval_seconds: float = 60.0):
        """启动插件监控线程"""
        if self._watchdog_thread is not None and self._watchdog_thread.is_alive():
            return
        
        self._is_running = True
        self._watchdog_thread = threading.Thread(target=self._watchdog_loop, args=(interval_seconds,))
        self._watchdog_thread.daemon = True
        self._watchdog_thread.start()
        self._log.info("plugin_manager", "插件监控线程已启动")
    
    def stop_watchdog(self):
        """停止插件监控线程"""
        self._is_running = False
        if self._watchdog_thread:
            self._watchdog_thread.join(timeout=5.0)
            self._watchdog_thread = None
        self._log.info("plugin_manager", "插件监控线程已停止")
    
    def _watchdog_loop(self, interval_seconds: float):
        """监控循环"""
        while self._is_running:
            try:
                self._check_plugin_health()
                
                if self._hot_reload_enabled:
                    self._check_for_changes()
                
                time.sleep(interval_seconds)
            except Exception as e:
                self._log.error("plugin_manager", f"监控循环异常: {e}")
    
    def _check_plugin_health(self):
        """检查插件健康状态"""
        with self._lock:
            for plugin_id, plugin in list(self._plugins.items()):
                health = plugin.get_health()
                if health["error_count"] > 10:
                    self._log.warning("plugin_manager", f"插件 {plugin_id} 错误过多，考虑重启")
                    # 尝试重启
                    if plugin.state == PluginState.ERROR:
                        self.reload_plugin(plugin_id)
    
    def _check_for_changes(self):
        """检查插件文件变化（热重载）"""
        # 简化实现：检查文件修改时间
        pass
    
    def shutdown(self):
        """关闭插件管理器"""
        self.stop_watchdog()
        self.unload_all()
        self._log.info("plugin_manager", "插件管理器已关闭")
