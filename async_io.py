"""
操作系统课程设计 - 异步I/O模块
功能：异步文件读写操作、回调机制、并发I/O处理
"""

import asyncio
import threading
import time
import queue
from dataclasses import dataclass, field
from typing import Callable, Optional, Any, Dict, List
from enum import Enum
from concurrent.futures import ThreadPoolExecutor
import functools

from filesystem import FileSystem, BLOCK_SIZE
from buffer import BufferManager

# ==================== I/O请求状态 ====================

class IOStatus(Enum):
    """I/O请求状态"""
    PENDING = "等待中"
    RUNNING = "执行中"
    COMPLETED = "已完成"
    FAILED = "失败"
    CANCELLED = "已取消"

class IOType(Enum):
    """I/O操作类型"""
    READ = "读取"
    WRITE = "写入"
    CREATE = "创建"
    DELETE = "删除"

@dataclass
class IORequest:
    """异步I/O请求"""
    request_id: int
    io_type: IOType
    filename: str
    block_index: int = -1                    # -1表示整个文件
    data: bytes = b''                        # 写入数据
    status: IOStatus = IOStatus.PENDING
    result: Any = None
    error: Optional[str] = None
    callback: Optional[Callable] = None      # 完成回调
    create_time: float = field(default_factory=time.time)
    start_time: float = 0.0
    end_time: float = 0.0
    priority: int = 5                        # 0-9, 0最高

# ==================== 异步I/O管理器 ====================

class AsyncIOManager:
    """
    异步I/O管理器
    实现非阻塞的文件操作
    """
    
    def __init__(self, fs: FileSystem, buffer_mgr: BufferManager, max_workers: int = 4):
        self.fs = fs
        self.buffer_mgr = buffer_mgr
        self.max_workers = max_workers
        
        # 请求队列和管理
        self.request_queue = queue.PriorityQueue()
        self.requests: Dict[int, IORequest] = {}
        self.request_id_counter = 0
        self.lock = threading.RLock()
        
        # 线程池
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.running = False
        self.worker_thread: Optional[threading.Thread] = None
        
        # 统计信息
        self.completed_count = 0
        self.failed_count = 0
        self.total_bytes_read = 0
        self.total_bytes_written = 0
        
        # 事件回调
        self.on_request_start = None
        self.on_request_complete = None
    
    def _generate_request_id(self) -> int:
        """生成唯一请求ID"""
        with self.lock:
            self.request_id_counter += 1
            return self.request_id_counter
    
    def _worker_loop(self):
        """工作线程主循环"""
        while self.running:
            try:
                # 从队列获取请求（带超时）
                _, request_id = self.request_queue.get(timeout=0.1)
                
                with self.lock:
                    if request_id not in self.requests:
                        continue
                    request = self.requests[request_id]
                    if request.status != IOStatus.PENDING:
                        continue
                    request.status = IOStatus.RUNNING
                    request.start_time = time.time()
                
                if self.on_request_start:
                    self.on_request_start(request)
                
                # 提交到线程池执行
                future = self.executor.submit(self._process_request, request)
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[AsyncIO] 工作线程错误: {e}")
    
    def _process_request(self, request: IORequest):
        """处理单个I/O请求"""
        try:
            if request.io_type == IOType.READ:
                self._do_read(request)
            elif request.io_type == IOType.WRITE:
                self._do_write(request)
            elif request.io_type == IOType.CREATE:
                self._do_create(request)
            elif request.io_type == IOType.DELETE:
                self._do_delete(request)
            
            request.status = IOStatus.COMPLETED
            self.completed_count += 1
            
        except Exception as e:
            request.status = IOStatus.FAILED
            request.error = str(e)
            self.failed_count += 1
            print(f"[AsyncIO] 请求 {request.request_id} 失败: {e}")
        
        finally:
            request.end_time = time.time()
            
            # 执行回调
            if request.callback:
                try:
                    request.callback(request)
                except Exception as e:
                    print(f"[AsyncIO] 回调执行失败: {e}")
            
            if self.on_request_complete:
                self.on_request_complete(request)
    
    def _do_read(self, request: IORequest):
        """执行读操作 - 通过缓冲区"""
        # 获取文件引用
        self.fs.acquire_file(request.filename)
        try:
            inode = self.fs._find_inode(request.filename)
            if inode is None:
                raise FileNotFoundError(f"文件 {request.filename} 不存在")

            # 获取所有数据块
            all_blocks = [b for b in inode.direct_blocks if b >= 0]
            if inode.indirect_block >= 0:
                from filesystem import IndexBlock
                idx_data = self.fs.disk.read_block(inode.indirect_block)
                idx_block = IndexBlock.from_bytes(idx_data)
                all_blocks.extend([b for b in idx_block.indices if b >= 0])

            if request.block_index >= 0:
                # 读取指定块 - 通过缓冲区
                if request.block_index >= len(all_blocks):
                    raise IndexError(f"块索引 {request.block_index} 超出范围")

                block_num = all_blocks[request.block_index]
                _, data = self.buffer_mgr.load_block(request.filename, block_num)
                request.result = data
                self.total_bytes_read += len(data)
            else:
                # 读取整个文件 - 每个块都通过缓冲区
                content = b''
                for block_num in all_blocks:
                    _, block_data = self.buffer_mgr.load_block(request.filename, block_num)
                    content += block_data

                # 截取实际文件大小
                request.result = content[:inode.file_size]
                self.total_bytes_read += inode.file_size

            time.sleep(0.05)  # 模拟I/O延时

        finally:
            self.fs.release_file(request.filename)

    def _do_write(self, request: IORequest):
        """执行写操作 - 通过缓冲区"""
        try:
            if request.block_index >= 0:
                # 写入指定块 - 需要获取文件引用
                self.fs.acquire_file(request.filename)
                try:
                    inode = self.fs._find_inode(request.filename)
                    if inode is None:
                        raise FileNotFoundError(f"文件 {request.filename} 不存在")

                    all_blocks = [b for b in inode.direct_blocks if b >= 0]
                    if inode.indirect_block >= 0:
                        from filesystem import IndexBlock
                        idx_data = self.fs.disk.read_block(inode.indirect_block)
                        idx_block = IndexBlock.from_bytes(idx_data)
                        all_blocks.extend([b for b in idx_block.indices if b >= 0])

                    if request.block_index >= len(all_blocks):
                        raise IndexError(f"块索引 {request.block_index} 超出范围")

                    block_num = all_blocks[request.block_index]
                    self.buffer_mgr.write_block(request.filename, block_num, request.data)
                    self.total_bytes_written += len(request.data)

                    # 更新文件大小（如果写入的块是最后一块且内容变长）
                    new_size = request.block_index * BLOCK_SIZE + len(request.data)
                    if new_size > inode.file_size:
                        inode.file_size = new_size
                        inode.modify_time = time.time()
                        self.fs._save_inodes()

                    request.result = True
                finally:
                    self.fs.release_file(request.filename)
            else:
                # 整文件写入
                # 1. 先使该文件的缓冲页失效（避免旧数据）
                self.buffer_mgr.invalidate_file(request.filename)

                # 2. 调用write_file处理块分配和写入
                success, msg = self.fs.write_file(request.filename, request.data)

                if success:
                    # 3. 写入成功后，将新数据加载到缓冲区
                    inode = self.fs._find_inode(request.filename)
                    if inode:
                        all_blocks = [b for b in inode.direct_blocks if b >= 0]
                        if inode.indirect_block >= 0:
                            from filesystem import IndexBlock
                            idx_data = self.fs.disk.read_block(inode.indirect_block)
                            idx_block = IndexBlock.from_bytes(idx_data)
                            all_blocks.extend([b for b in idx_block.indices if b >= 0])

                        # 加载每个块到缓冲区
                        for block_num in all_blocks:
                            self.buffer_mgr.load_block(request.filename, block_num)

                    self.total_bytes_written += len(request.data)
                    request.result = True
                else:
                    request.result = False
                    request.error = msg

            time.sleep(0.05)  # 模拟I/O延时

        except Exception as e:
            request.result = False
            request.error = str(e)

    def _do_create(self, request: IORequest):
        """执行创建操作 - 创建后加载到缓冲区"""
        success, msg = self.fs.create_file(request.filename, request.data)
        request.result = success
        if not success:
            request.error = msg
        else:
            self.total_bytes_written += len(request.data)

            # 创建成功后，将文件数据加载到缓冲区
            inode = self.fs._find_inode(request.filename)
            if inode:
                all_blocks = [b for b in inode.direct_blocks if b >= 0]
                if inode.indirect_block >= 0:
                    from filesystem import IndexBlock
                    idx_data = self.fs.disk.read_block(inode.indirect_block)
                    idx_block = IndexBlock.from_bytes(idx_data)
                    all_blocks.extend([b for b in idx_block.indices if b >= 0])

                # 加载每个块到缓冲区
                for block_num in all_blocks:
                    self.buffer_mgr.load_block(request.filename, block_num)

        time.sleep(0.1)

    def _do_delete(self, request: IORequest):
        """执行删除操作"""
        # 先使缓冲区失效
        self.buffer_mgr.invalidate_file(request.filename)
        success, msg = self.fs.delete_file(request.filename)
        request.result = success
        if not success:
            request.error = msg
        time.sleep(0.1)

    def start(self):
        """启动异步I/O管理器"""
        with self.lock:
            if self.running:
                return
            self.running = True
            self.worker_thread = threading.Thread(target=self._worker_loop, daemon=True)
            self.worker_thread.start()
            print("[AsyncIO] 异步I/O管理器已启动")

    def stop(self):
        """停止异步I/O管理器"""
        self.running = False
        if self.worker_thread:
            self.worker_thread.join(timeout=2.0)
        self.executor.shutdown(wait=False)
        print("[AsyncIO] 异步I/O管理器已停止")

    def submit_read(self, filename: str, block_index: int = -1,
                   callback: Callable = None, priority: int = 5) -> int:
        """
        提交异步读请求
        返回请求ID
        """
        request = IORequest(
            request_id=self._generate_request_id(),
            io_type=IOType.READ,
            filename=filename,
            block_index=block_index,
            callback=callback,
            priority=priority
        )

        with self.lock:
            self.requests[request.request_id] = request

        self.request_queue.put((priority, request.request_id))
        print(f"[AsyncIO] 提交读请求: ID={request.request_id}, 文件={filename}, 块={block_index}")
        return request.request_id

    def submit_write(self, filename: str, block_index: int, data: bytes,
                    callback: Callable = None, priority: int = 5) -> int:
        """提交异步写请求"""
        request = IORequest(
            request_id=self._generate_request_id(),
            io_type=IOType.WRITE,
            filename=filename,
            block_index=block_index,
            data=data,
            callback=callback,
            priority=priority
        )

        with self.lock:
            self.requests[request.request_id] = request

        self.request_queue.put((priority, request.request_id))
        print(f"[AsyncIO] 提交写请求: ID={request.request_id}, 文件={filename}, 块={block_index}")
        return request.request_id

    def submit_create(self, filename: str, content: bytes,
                     callback: Callable = None, priority: int = 5) -> int:
        """提交异步创建请求"""
        request = IORequest(
            request_id=self._generate_request_id(),
            io_type=IOType.CREATE,
            filename=filename,
            data=content,
            callback=callback,
            priority=priority
        )

        with self.lock:
            self.requests[request.request_id] = request

        self.request_queue.put((priority, request.request_id))
        print(f"[AsyncIO] 提交创建请求: ID={request.request_id}, 文件={filename}")
        return request.request_id

    def submit_delete(self, filename: str, callback: Callable = None, priority: int = 5) -> int:
        """提交异步删除请求"""
        request = IORequest(
            request_id=self._generate_request_id(),
            io_type=IOType.DELETE,
            filename=filename,
            callback=callback,
            priority=priority
        )

        with self.lock:
            self.requests[request.request_id] = request

        self.request_queue.put((priority, request.request_id))
        print(f"[AsyncIO] 提交删除请求: ID={request.request_id}, 文件={filename}")
        return request.request_id

    def get_request_status(self, request_id: int) -> Optional[Dict]:
        """获取请求状态"""
        with self.lock:
            if request_id in self.requests:
                req = self.requests[request_id]
                return {
                    'request_id': req.request_id,
                    'io_type': req.io_type.value,
                    'filename': req.filename,
                    'status': req.status.value,
                    'result': str(req.result)[:50] if req.result else None,
                    'error': req.error,
                    'elapsed': f"{req.end_time - req.start_time:.3f}s" if req.end_time else "N/A"
                }
            return None

    def wait_for_request(self, request_id: int, timeout: float = 10.0) -> bool:
        """等待请求完成"""
        start = time.time()
        while time.time() - start < timeout:
            with self.lock:
                if request_id in self.requests:
                    req = self.requests[request_id]
                    if req.status in [IOStatus.COMPLETED, IOStatus.FAILED, IOStatus.CANCELLED]:
                        return req.status == IOStatus.COMPLETED
            time.sleep(0.05)
        return False

    def get_pending_requests(self) -> List[Dict]:
        """获取所有待处理请求"""
        with self.lock:
            return [
                {
                    'request_id': r.request_id,
                    'io_type': r.io_type.value,
                    'filename': r.filename,
                    'status': r.status.value,
                    'priority': r.priority
                }
                for r in self.requests.values()
                if r.status in [IOStatus.PENDING, IOStatus.RUNNING]
            ]

    def get_statistics(self) -> Dict:
        """获取统计信息"""
        with self.lock:
            return {
                'total_requests': len(self.requests),
                'completed': self.completed_count,
                'failed': self.failed_count,
                'pending': sum(1 for r in self.requests.values() if r.status == IOStatus.PENDING),
                'running': sum(1 for r in self.requests.values() if r.status == IOStatus.RUNNING),
                'total_bytes_read': self.total_bytes_read,
                'total_bytes_written': self.total_bytes_written
            }


# ==================== 异步接口封装 ====================

class AsyncFileAPI:
    """
    异步文件API
    提供便捷的异步文件操作接口
    """

    def __init__(self, async_io: AsyncIOManager):
        self.async_io = async_io

    async def read_file_async(self, filename: str) -> Optional[bytes]:
        """异步读取整个文件"""
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        def callback(request):
            if not future.done():
                if request.status == IOStatus.COMPLETED:
                    loop.call_soon_threadsafe(future.set_result, request.result)
                else:
                    loop.call_soon_threadsafe(
                        future.set_exception,
                        Exception(request.error or "读取失败")
                    )

        request_id = self.async_io.submit_read(filename, callback=callback)
        return await future

    async def write_block_async(self, filename: str, block_index: int, data: bytes) -> bool:
        """异步写入文件块"""
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        def callback(request):
            if not future.done():
                if request.status == IOStatus.COMPLETED:
                    loop.call_soon_threadsafe(future.set_result, request.result)
                else:
                    loop.call_soon_threadsafe(future.set_result, False)

        request_id = self.async_io.submit_write(filename, block_index, data, callback=callback)
        return await future

    async def create_file_async(self, filename: str, content: bytes) -> bool:
        """异步创建文件"""
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        def callback(request):
            if not future.done():
                loop.call_soon_threadsafe(future.set_result, request.result)

        request_id = self.async_io.submit_create(filename, content, callback=callback)
        return await future

    async def delete_file_async(self, filename: str) -> bool:
        """异步删除文件"""
        loop = asyncio.get_event_loop()
        future = loop.create_future()

        def callback(request):
            if not future.done():
                loop.call_soon_threadsafe(future.set_result, request.result)

        request_id = self.async_io.submit_delete(filename, callback=callback)
        return await future


# ==================== 测试代码 ====================
if __name__ == "__main__":
    from filesystem import FileSystem
    from buffer import BufferManager

    # 初始化
    fs = FileSystem()
    fs.mount()
    buffer_mgr = BufferManager(fs)
    async_io = AsyncIOManager(fs, buffer_mgr)

    # 启动
    async_io.start()

    # 回调函数
    def on_complete(request):
        print(f"  [回调] 请求 {request.request_id} 完成, 状态: {request.status.value}")

    # 测试异步创建文件
    print("\n=== 异步创建文件 ===")
    req1 = async_io.submit_create("async_test1.txt", b"Async file content 1" * 10, callback=on_complete)
    req2 = async_io.submit_create("async_test2.txt", b"Async file content 2" * 10, callback=on_complete)
    req3 = async_io.submit_create("async_test3.txt", b"Async file content 3" * 10, callback=on_complete)

    # 等待完成
    async_io.wait_for_request(req1)
    async_io.wait_for_request(req2)
    async_io.wait_for_request(req3)

    # 显示文件列表
    print("\n=== 文件列表 ===")
    for f in fs.list_directory():
        print(f"  {f['name']} - {f['size']}B")

    # 测试异步读取
    print("\n=== 异步读取 ===")
    req4 = async_io.submit_read("async_test1.txt", callback=on_complete)
    async_io.wait_for_request(req4)

    status = async_io.get_request_status(req4)
    print(f"  读取结果: {status}")

    # 测试异步写入
    print("\n=== 异步写入 ===")
    req5 = async_io.submit_write("async_test1.txt", 0, b"MODIFIED CONTENT!!!", callback=on_complete)
    async_io.wait_for_request(req5)

    # 测试删除
    print("\n=== 异步删除 ===")
    req6 = async_io.submit_delete("async_test3.txt", callback=on_complete)
    async_io.wait_for_request(req6)

    # 显示统计
    print("\n=== 统计信息 ===")
    stats = async_io.get_statistics()
    for k, v in stats.items():
        print(f"  {k}: {v}")

    # 停止
    async_io.stop()
    print("\n=== 测试完成 ===")