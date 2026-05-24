"""
插件依赖解析器 - 解析插件依赖关系，确定加载顺序
"""
from typing import Dict, List, Set, Tuple, Optional, Any
from collections import defaultdict, deque

try:
    from .plugin_interface import PluginMetadata
except ImportError:
    import sys
    import os
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from plugin.plugin_interface import PluginMetadata


class PluginDependencyResolver:
    """
    插件依赖解析器
    
    功能:
    1. 解析插件间的依赖关系
    2. 检测循环依赖
    3. 计算拓扑排序的加载顺序
    4. 版本兼容性检查
    """
    
    def __init__(self):
        self._dependencies: Dict[str, List[str]] = {}  # plugin_name -> [dependency_names]
        self._versions: Dict[str, str] = {}  # plugin_name -> version
        self._metadata: Dict[str, PluginMetadata] = {}  # plugin_name -> metadata
    
    def add_plugin(self, metadata: PluginMetadata):
        """添加插件到依赖图"""
        self._dependencies[metadata.name] = metadata.dependencies
        self._versions[metadata.name] = metadata.version
        self._metadata[metadata.name] = metadata
    
    def resolve_load_order(self) -> Tuple[List[str], List[str]]:
        """
        解析加载顺序
        
        Returns:
            Tuple[List[str], List[str]]: (加载顺序列表, 错误信息列表)
        """
        errors = []
        
        # 检查缺失的依赖
        for name, deps in self._dependencies.items():
            for dep in deps:
                if dep not in self._dependencies:
                    errors.append(f"Plugin '{name}' depends on '{dep}' which is not available")
        
        # 检测循环依赖
        cycle = self._detect_cycle()
        if cycle:
            errors.append(f"Circular dependency detected: {' -> '.join(cycle)}")
        
        if errors:
            return [], errors
        
        # 拓扑排序
        order = self._topological_sort()
        return order, []
    
    def _detect_cycle(self) -> Optional[List[str]]:
        """检测循环依赖，返回循环路径或None"""
        visited = set()
        rec_stack = set()
        path = []
        
        def dfs(node: str) -> Optional[List[str]]:
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            
            for neighbor in self._dependencies.get(node, []):
                if neighbor not in visited:
                    result = dfs(neighbor)
                    if result:
                        return result
                elif neighbor in rec_stack:
                    # 找到循环
                    cycle_start = path.index(neighbor)
                    return path[cycle_start:] + [neighbor]
            
            path.pop()
            rec_stack.remove(node)
            return None
        
        for node in self._dependencies:
            if node not in visited:
                result = dfs(node)
                if result:
                    return result
        
        return None
    
    def _topological_sort(self) -> List[str]:
        """拓扑排序计算加载顺序"""
        # 计算入度
        in_degree = {name: 0 for name in self._dependencies}
        for name, deps in self._dependencies.items():
            for dep in deps:
                if dep in in_degree:
                    in_degree[name] += 1
        
        # Kahn算法
        queue = deque([name for name, degree in in_degree.items() if degree == 0])
        result = []
        
        while queue:
            node = queue.popleft()
            result.append(node)
            
            # 减少依赖此节点的其他节点的入度
            for name, deps in self._dependencies.items():
                if node in deps:
                    in_degree[name] -= 1
                    if in_degree[name] == 0:
                        queue.append(name)
        
        return result
    
    def check_version_compatibility(self, platform_version: str) -> List[str]:
        """
        检查版本兼容性
        
        Args:
            platform_version: 平台版本号
            
        Returns:
            List[str]: 不兼容的插件错误信息列表
        """
        errors = []
        
        for name, metadata in self._metadata.items():
            # 检查最低版本要求
            if metadata.min_platform_version:
                if self._compare_versions(platform_version, metadata.min_platform_version) < 0:
                    errors.append(
                        f"Plugin '{name}' requires platform version >= {metadata.min_platform_version}, "
                        f"but current version is {platform_version}"
                    )
            
            # 检查最高版本限制
            if metadata.max_platform_version:
                if self._compare_versions(platform_version, metadata.max_platform_version) > 0:
                    errors.append(
                        f"Plugin '{name}' requires platform version <= {metadata.max_platform_version}, "
                        f"but current version is {platform_version}"
                    )
        
        return errors
    
    def _compare_versions(self, v1: str, v2: str) -> int:
        """
        比较两个版本号
        
        Returns:
            int: -1 if v1 < v2, 0 if v1 == v2, 1 if v1 > v2
        """
        def parse_version(v: str) -> List[int]:
            return [int(x) for x in v.split('.') if x.isdigit()]
        
        parts1 = parse_version(v1)
        parts2 = parse_version(v2)
        
        for p1, p2 in zip(parts1, parts2):
            if p1 < p2:
                return -1
            elif p1 > p2:
                return 1
        
        if len(parts1) < len(parts2):
            return -1
        elif len(parts1) > len(parts2):
            return 1
        return 0
    
    def get_dependency_tree(self, plugin_name: str, visited: Optional[Set[str]] = None) -> Dict[str, Any]:
        """
        获取插件的依赖树
        
        Args:
            plugin_name: 插件名称
            visited: 已访问的插件集合（用于防止循环）
            
        Returns:
            Dict: 依赖树结构
        """
        if visited is None:
            visited = set()
        
        if plugin_name in visited:
            return {"name": plugin_name, "version": self._versions.get(plugin_name, "?"), "circular": True}
        
        visited.add(plugin_name)
        
        deps = self._dependencies.get(plugin_name, [])
        children = [self.get_dependency_tree(dep, visited.copy()) for dep in deps if dep in self._dependencies]
        
        return {
            "name": plugin_name,
            "version": self._versions.get(plugin_name, "?"),
            "dependencies": children
        }
    
    def get_dependents(self, plugin_name: str) -> List[str]:
        """
        获取依赖于指定插件的所有插件
        
        Args:
            plugin_name: 插件名称
            
        Returns:
            List[str]: 依赖于该插件的插件名称列表
        """
        dependents = []
        for name, deps in self._dependencies.items():
            if plugin_name in deps:
                dependents.append(name)
        return dependents
