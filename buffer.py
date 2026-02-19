"""
操作系统课程设计 - 内存缓冲区模块
功能：缓冲页管理、LRU页面置换、脏页写回
"""

import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from collections import OrderedDict
from filesystem import BLOCK_SIZE, FileSystem

# 缓冲区配置
BUFFER_SIZE = 16  # 缓冲页数量K=16

@dataclass
class BufferPage:
    """缓冲页结构"""
    page_id: int                          # 页号
    block_num: int = -1                   # 对应的磁盘块号
    filename: str = ""                    # 所属文件名
    data: bytes = field(default_factory=lambda: b'\x00' * BLOCK_SIZE)
    is_dirty: bool = False                # 是否被修改
    is_valid: bool = False                # 是否有效
    owner_pid: int = -1                   # 所有者进程ID
    access_time: float = 0.0              # 最后访问时间
    load_time: float = 0.0                # 加载时间
    access_count: int = 0                 # 访问次数

class BufferManager:
    """
    内存缓冲区管理器
    实现LRU页面置换算法
    """
    
    def __init__(self, fs: FileSystem, buffer_size: int = BUFFER_SIZE):
        self.fs = fs
        self.buffer_size = buffer_size
        self.pages: Dict[int, BufferPage] = {}
        self.lru_order: OrderedDict = OrderedDict()  # 用于LRU排序
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        
        # 统计信息
        self.hit_count = 0
        self.miss_count = 0
        self.writeback_count = 0
        
        # 初始化缓冲页
        for i in range(buffer_size):
            self.pages[i] = BufferPage(page_id=i)
        
        # 事件回调（用于可视化）
        self.on_page_load = None
        self.on_page_evict = None
        self.on_page_access = None
    
    def _find_free_page(self) -> Optional[int]:
        """查找空闲页"""
        for page_id, page in self.pages.items():
            if not page.is_valid:
                return page_id
        return None
    
    def _find_lru_page(self) -> Optional[int]:
        """找到最近最少使用的页（LRU）"""
        if not self.lru_order:
            return None
        # 返回最老的页（OrderedDict的第一个）
        oldest_key = next(iter(self.lru_order))
        return oldest_key
    
    def _update_lru(self, page_id: int):
        """更新LRU顺序"""
        if page_id in self.lru_order:
            self.lru_order.move_to_end(page_id)
        else:
            self.lru_order[page_id] = True
    
    def _remove_from_lru(self, page_id: int):
        """从LRU列表移除"""
        if page_id in self.lru_order:
            del self.lru_order[page_id]
    
    def _evict_page(self, page_id: int) -> bool:
        """
        驱逐页面
        如果页面是脏页，需要写回磁盘
        """
        page = self.pages[page_id]
        if not page.is_valid:
            return True
        
        # 如果是脏页，写回磁盘
        if page.is_dirty:
            print(f"[Buffer] 写回脏页: 页{page_id} -> 块{page.block_num}")
            self.fs.disk.write_block(page.block_num, page.data)
            self.writeback_count += 1
            time.sleep(0.1)  # 模拟写回延时
        
        # 触发驱逐事件
        if self.on_page_evict:
            self.on_page_evict(page_id, page.block_num, page.filename)
        
        # 清空页面
        page.is_valid = False
        page.is_dirty = False
        page.block_num = -1
        page.filename = ""
        page.owner_pid = -1
        page.data = b'\x00' * BLOCK_SIZE
        
        self._remove_from_lru(page_id)
        return True
    
    def load_block(self, filename: str, block_num: int, pid: int = -1) -> Tuple[Optional[int], bytes]:
        """
        加载磁盘块到缓冲区
        返回: (页号, 数据)
        """
        with self.condition:
            # 检查是否已在缓冲区中
            for page_id, page in self.pages.items():
                if page.is_valid and page.block_num == block_num and page.filename == filename:
                    # 缓冲命中
                    self.hit_count += 1
                    page.access_time = time.time()
                    page.access_count += 1
                    self._update_lru(page_id)
                    
                    if self.on_page_access:
                        self.on_page_access(page_id, "HIT")
                    
                    print(f"[Buffer] 缓冲命中: 页{page_id}, 块{block_num}")
                    return page_id, page.data
            
            # 缓冲未命中
            self.miss_count += 1
            
            # 查找空闲页或执行LRU置换
            page_id = self._find_free_page()
            if page_id is None:
                # 需要置换
                page_id = self._find_lru_page()
                if page_id is None:
                    return None, b''
                print(f"[Buffer] LRU置换: 选择页{page_id}")
                self._evict_page(page_id)
            
            # 从磁盘读取数据
            print(f"[Buffer] 加载块: 块{block_num} -> 页{page_id}")
            data = self.fs.disk.read_block(block_num)
            time.sleep(0.15)  # 模拟读取延时
            
            # 更新页面信息
            page = self.pages[page_id]
            page.block_num = block_num
            page.filename = filename
            page.data = data
            page.is_valid = True
            page.is_dirty = False
            page.owner_pid = pid
            page.access_time = time.time()
            page.load_time = time.time()
            page.access_count = 1
            
            self._update_lru(page_id)
            
            if self.on_page_load:
                self.on_page_load(page_id, block_num, filename)
            
            return page_id, data
    
    def write_block(self, filename: str, block_num: int, data: bytes, pid: int = -1) -> bool:
        """
        写入数据到缓冲区（延迟写）
        """
        with self.condition:
            # 先加载到缓冲区
            page_id, _ = self.load_block(filename, block_num, pid)
            if page_id is None:
                return False
            
            # 更新数据
            page = self.pages[page_id]
            page.data = data[:BLOCK_SIZE].ljust(BLOCK_SIZE, b'\x00')
            page.is_dirty = True
            page.access_time = time.time()
            
            print(f"[Buffer] 写入缓冲: 页{page_id}, 块{block_num}, 标记为脏页")
            return True
    
    def flush_all(self):
        """将所有脏页写回磁盘"""
        with self.condition:
            for page_id, page in self.pages.items():
                if page.is_valid and page.is_dirty:
                    print(f"[Buffer] 刷新脏页: 页{page_id} -> 块{page.block_num}")
                    self.fs.disk.write_block(page.block_num, page.data)
                    page.is_dirty = False
                    self.writeback_count += 1
    
    def flush_file(self, filename: str):
        """将指定文件的所有脏页写回"""
        with self.condition:
            for page_id, page in self.pages.items():
                if page.is_valid and page.is_dirty and page.filename == filename:
                    print(f"[Buffer] 刷新文件脏页: {filename}, 页{page_id}")
                    self.fs.disk.write_block(page.block_num, page.data)
                    page.is_dirty = False
                    self.writeback_count += 1
    
    def invalidate_file(self, filename: str):
        """使指定文件的所有缓冲页失效"""
        with self.condition:
            for page_id, page in self.pages.items():
                if page.is_valid and page.filename == filename:
                    if page.is_dirty:
                        # 先写回
                        self.fs.disk.write_block(page.block_num, page.data)
                        self.writeback_count += 1
                    page.is_valid = False
                    page.is_dirty = False
                    self._remove_from_lru(page_id)
    
    def get_buffer_status(self) -> List[Dict]:
        """获取缓冲区状态（用于可视化）"""
        with self.lock:
            status = []
            for page_id in range(self.buffer_size):
                page = self.pages[page_id]
                status.append({
                    'page_id': page_id,
                    'block_num': page.block_num,
                    'filename': page.filename,
                    'is_valid': page.is_valid,
                    'is_dirty': page.is_dirty,
                    'owner_pid': page.owner_pid,
                    'access_count': page.access_count,
                    'lru_position': list(self.lru_order.keys()).index(page_id) if page_id in self.lru_order else -1
                })
            return status
    
    def get_statistics(self) -> Dict:
        """获取缓冲区统计信息"""
        with self.lock:
            total = self.hit_count + self.miss_count
            hit_rate = (self.hit_count / total * 100) if total > 0 else 0
            return {
                'hit_count': self.hit_count,
                'miss_count': self.miss_count,
                'hit_rate': f"{hit_rate:.2f}%",
                'writeback_count': self.writeback_count,
                'valid_pages': sum(1 for p in self.pages.values() if p.is_valid),
                'dirty_pages': sum(1 for p in self.pages.values() if p.is_dirty)
            }
    
    def get_page_data_preview(self, page_id: int) -> str:
        """获取页面数据预览"""
        with self.lock:
            if page_id in self.pages:
                page = self.pages[page_id]
                if page.is_valid:
                    # 尝试解码为文本，失败则显示十六进制
                    try:
                        text = page.data.rstrip(b'\x00').decode('utf-8')
                        return text[:50] + "..." if len(text) > 50 else text
                    except:
                        return page.data[:20].hex()
            return "空"


# ==================== 测试代码 ====================
if __name__ == "__main__":
    from filesystem import FileSystem
    
    fs = FileSystem()
    fs.mount()
    
    # 创建测试文件
    content = b"Test content block " * 50
    fs.create_file("buffer_test.txt", content)
    
    # 创建缓冲区管理器
    buffer_mgr = BufferManager(fs)
    
    # 获取文件的块列表
    files = fs.list_directory()
    print(f"文件列表: {files}")
    
    # 模拟读取操作
    print("\n=== 模拟缓冲区操作 ===")
    for i in range(20):
        block_num = 35 + (i % 5)  # 循环访问几个块
        page_id, data = buffer_mgr.load_block("buffer_test.txt", block_num, pid=1001)
        print(f"加载块{block_num} -> 页{page_id}")
    
    # 显示统计
    print("\n=== 缓冲区统计 ===")
    stats = buffer_mgr.get_statistics()
    for k, v in stats.items():
        print(f"  {k}: {v}")
    
    # 显示缓冲区状态
    print("\n=== 缓冲区状态 ===")
    status = buffer_mgr.get_buffer_status()
    for s in status[:8]:
        if s['is_valid']:
            print(f"  页{s['page_id']}: 块{s['block_num']}, 文件={s['filename']}, 脏={s['is_dirty']}")
