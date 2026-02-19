"""
操作系统课程设计 - 进程管理模块
功能：进程调度(优先级策略)、管道通信、条件变量同步互斥
"""

import os
import threading
import multiprocessing
import queue
import time
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Any
from enum import Enum
from collections import deque

# ==================== 进程状态和优先级 ====================

class ProcessState(Enum):
    """进程状态"""
    NEW = "新建"
    READY = "就绪"
    RUNNING = "运行"
    WAITING = "等待"
    TERMINATED = "终止"

class Priority(Enum):
    """进程优先级（数值越小优先级越高）"""
    HIGH = 1
    MEDIUM = 2
    LOW = 3

@dataclass
class ProcessControlBlock:
    """进程控制块 PCB"""
    pid: int                                    # 进程ID
    name: str                                   # 进程名称
    priority: Priority = Priority.MEDIUM        # 优先级
    state: ProcessState = ProcessState.NEW      # 状态
    create_time: float = field(default_factory=time.time)
    start_time: float = 0.0
    end_time: float = 0.0
    cpu_time: float = 0.0                       # CPU使用时间
    wait_time: float = 0.0                      # 等待时间
    task: Optional[Callable] = None             # 任务函数
    args: tuple = ()                            # 任务参数
    result: Any = None                          # 执行结果
    thread: Optional[threading.Thread] = None   # 对应线程

# ==================== 管道通信 ====================

class Pipe:
    """
    管道 - 进程间通信机制
    实现单向数据流通信
    """
    
    def __init__(self, name: str, buffer_size: int = 1024):
        self.name = name
        self.buffer_size = buffer_size
        self.buffer = deque(maxlen=buffer_size)
        self.lock = threading.Lock()
        self.not_empty = threading.Condition(self.lock)
        self.not_full = threading.Condition(self.lock)
        self.closed = False
        
        # 统计
        self.write_count = 0
        self.read_count = 0
    
    def write(self, data: Any, timeout: float = None) -> bool:
        """
        向管道写入数据
        """
        with self.not_full:
            if self.closed:
                return False
            
            # 等待管道有空间
            while len(self.buffer) >= self.buffer_size:
                if not self.not_full.wait(timeout):
                    return False
                if self.closed:
                    return False
            
            self.buffer.append(data)
            self.write_count += 1
            self.not_empty.notify()
            return True
    
    def read(self, timeout: float = None) -> Optional[Any]:
        """
        从管道读取数据
        """
        with self.not_empty:
            # 等待管道有数据
            while len(self.buffer) == 0:
                if self.closed:
                    return None
                if not self.not_empty.wait(timeout):
                    return None
            
            data = self.buffer.popleft()
            self.read_count += 1
            self.not_full.notify()
            return data
    
    def close(self):
        """关闭管道"""
        with self.lock:
            self.closed = True
            self.not_empty.notify_all()
            self.not_full.notify_all()
    
    def is_empty(self) -> bool:
        with self.lock:
            return len(self.buffer) == 0
    
    def size(self) -> int:
        with self.lock:
            return len(self.buffer)
    
    def get_stats(self) -> Dict:
        with self.lock:
            return {
                'name': self.name,
                'buffer_size': self.buffer_size,
                'current_size': len(self.buffer),
                'write_count': self.write_count,
                'read_count': self.read_count,
                'closed': self.closed
            }

class PipeManager:
    """管道管理器"""
    
    def __init__(self):
        self.pipes: Dict[str, Pipe] = {}
        self.lock = threading.Lock()
    
    def create_pipe(self, name: str, buffer_size: int = 1024) -> Pipe:
        """创建新管道"""
        with self.lock:
            if name in self.pipes:
                return self.pipes[name]
            pipe = Pipe(name, buffer_size)
            self.pipes[name] = pipe
            return pipe
    
    def get_pipe(self, name: str) -> Optional[Pipe]:
        """获取管道"""
        with self.lock:
            return self.pipes.get(name)
    
    def delete_pipe(self, name: str):
        """删除管道"""
        with self.lock:
            if name in self.pipes:
                self.pipes[name].close()
                del self.pipes[name]
    
    def list_pipes(self) -> List[Dict]:
        """列出所有管道状态"""
        with self.lock:
            return [pipe.get_stats() for pipe in self.pipes.values()]

# ==================== 条件变量同步 ====================

class ConditionVariable:
    """
    条件变量 - 用于进程/线程同步
    """
    
    def __init__(self, name: str):
        self.name = name
        self.lock = threading.RLock()
        self.condition = threading.Condition(self.lock)
        self.wait_count = 0
        self.signal_count = 0
        self.broadcast_count = 0
    
    def acquire(self):
        """获取锁"""
        self.lock.acquire()
    
    def release(self):
        """释放锁"""
        self.lock.release()
    
    def wait(self, timeout: float = None) -> bool:
        """
        等待条件变量
        调用前必须持有锁
        """
        self.wait_count += 1
        return self.condition.wait(timeout)
    
    def signal(self):
        """
        唤醒一个等待的线程
        """
        self.signal_count += 1
        self.condition.notify()
    
    def broadcast(self):
        """
        唤醒所有等待的线程
        """
        self.broadcast_count += 1
        self.condition.notify_all()
    
    def __enter__(self):
        self.acquire()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        return False
    
    def get_stats(self) -> Dict:
        return {
            'name': self.name,
            'wait_count': self.wait_count,
            'signal_count': self.signal_count,
            'broadcast_count': self.broadcast_count
        }

class SyncManager:
    """同步原语管理器"""
    
    def __init__(self):
        self.conditions: Dict[str, ConditionVariable] = {}
        self.semaphores: Dict[str, threading.Semaphore] = {}
        self.lock = threading.Lock()
    
    def create_condition(self, name: str) -> ConditionVariable:
        """创建条件变量"""
        with self.lock:
            if name not in self.conditions:
                self.conditions[name] = ConditionVariable(name)
            return self.conditions[name]
    
    def get_condition(self, name: str) -> Optional[ConditionVariable]:
        """获取条件变量"""
        with self.lock:
            return self.conditions.get(name)
    
    def create_semaphore(self, name: str, value: int = 1) -> threading.Semaphore:
        """创建信号量"""
        with self.lock:
            if name not in self.semaphores:
                self.semaphores[name] = threading.Semaphore(value)
            return self.semaphores[name]
    
    def list_conditions(self) -> List[Dict]:
        """列出所有条件变量"""
        with self.lock:
            return [cv.get_stats() for cv in self.conditions.values()]

# ==================== 优先级调度器 ====================

class PriorityScheduler:
    """
    优先级调度器
    实现基于优先级的进程调度
    """
    
    def __init__(self):
        # 三个优先级队列
        self.ready_queues: Dict[Priority, deque] = {
            Priority.HIGH: deque(),
            Priority.MEDIUM: deque(),
            Priority.LOW: deque()
        }
        self.processes: Dict[int, ProcessControlBlock] = {}
        self.running: Optional[ProcessControlBlock] = None
        self.pid_counter = 1000
        self.lock = threading.RLock()
        self.scheduler_running = False
        self.scheduler_thread: Optional[threading.Thread] = None
        
        # 条件变量用于调度同步
        self.schedule_cv = threading.Condition(self.lock)
        
        # 统计
        self.total_scheduled = 0
        self.context_switches = 0
        
        # 事件回调
        self.on_process_start = None
        self.on_process_end = None
        self.on_context_switch = None
    
    def _generate_pid(self) -> int:
        """生成唯一进程ID"""
        self.pid_counter += 1
        return self.pid_counter
    
    def create_process(self, name: str, task: Callable, args: tuple = (), 
                      priority: Priority = Priority.MEDIUM) -> ProcessControlBlock:
        """
        创建新进程
        """
        with self.lock:
            pid = self._generate_pid()
            pcb = ProcessControlBlock(
                pid=pid,
                name=name,
                priority=priority,
                state=ProcessState.NEW,
                task=task,
                args=args
            )
            self.processes[pid] = pcb
            print(f"[Scheduler] 创建进程: PID={pid}, 名称={name}, 优先级={priority.name}")
            return pcb
    
    def submit_process(self, pcb: ProcessControlBlock):
        """
        提交进程到就绪队列
        """
        with self.lock:
            if pcb.pid not in self.processes:
                self.processes[pcb.pid] = pcb
            
            pcb.state = ProcessState.READY
            self.ready_queues[pcb.priority].append(pcb)
            print(f"[Scheduler] 进程 {pcb.pid} 进入就绪队列 (优先级: {pcb.priority.name})")
            self.schedule_cv.notify()
    
    def _select_next(self) -> Optional[ProcessControlBlock]:
        """
        选择下一个要运行的进程
        按优先级从高到低选择
        """
        for priority in [Priority.HIGH, Priority.MEDIUM, Priority.LOW]:
            if self.ready_queues[priority]:
                return self.ready_queues[priority].popleft()
        return None
    
    def _run_process(self, pcb: ProcessControlBlock):
        """
        执行进程
        """
        pcb.state = ProcessState.RUNNING
        pcb.start_time = time.time()
        self.running = pcb
        
        if self.on_process_start:
            self.on_process_start(pcb)
        
        print(f"[Scheduler] 运行进程: PID={pcb.pid}, 名称={pcb.name}")
        
        try:
            # 执行任务
            if pcb.task:
                pcb.result = pcb.task(*pcb.args)
        except Exception as e:
            pcb.result = f"Error: {e}"
            print(f"[Scheduler] 进程 {pcb.pid} 执行出错: {e}")
        finally:
            pcb.end_time = time.time()
            pcb.cpu_time = pcb.end_time - pcb.start_time
            pcb.state = ProcessState.TERMINATED
            self.running = None
            
            if self.on_process_end:
                self.on_process_end(pcb)
            
            print(f"[Scheduler] 进程 {pcb.pid} 完成, 耗时: {pcb.cpu_time:.3f}s")
    
    def _scheduler_loop(self):
        """
        调度器主循环
        """
        while self.scheduler_running:
            with self.schedule_cv:
                # 等待有进程可调度
                while self.scheduler_running and not any(self.ready_queues.values()):
                    self.schedule_cv.wait(timeout=0.1)
                
                if not self.scheduler_running:
                    break
                
                # 选择并运行进程
                pcb = self._select_next()
                if pcb:
                    self.total_scheduled += 1
                    
            # 在锁外执行进程
            if pcb:
                self._run_process(pcb)
    
    def start(self):
        """启动调度器"""
        with self.lock:
            if self.scheduler_running:
                return
            
            self.scheduler_running = True
            self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
            self.scheduler_thread.start()
            print("[Scheduler] 调度器已启动")
    
    def stop(self):
        """停止调度器"""
        with self.schedule_cv:
            self.scheduler_running = False
            self.schedule_cv.notify_all()
        
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=2.0)
        print("[Scheduler] 调度器已停止")
    
    def get_queue_status(self) -> Dict:
        """获取队列状态"""
        with self.lock:
            return {
                'high': [p.pid for p in self.ready_queues[Priority.HIGH]],
                'medium': [p.pid for p in self.ready_queues[Priority.MEDIUM]],
                'low': [p.pid for p in self.ready_queues[Priority.LOW]],
                'running': self.running.pid if self.running else None,
                'total_scheduled': self.total_scheduled
            }
    
    def get_process_list(self) -> List[Dict]:
        """获取所有进程列表"""
        with self.lock:
            return [{
                'pid': p.pid,
                'name': p.name,
                'priority': p.priority.name,
                'state': p.state.value,
                'cpu_time': f"{p.cpu_time:.3f}s",
                'result': str(p.result)[:50] if p.result else None
            } for p in self.processes.values()]

# ==================== 命令处理器 ====================

class CommandProcessor:
    """
    命令处理器
    每个命令作为独立进程/线程运行
    """
    
    def __init__(self, scheduler: PriorityScheduler, pipe_mgr: PipeManager, sync_mgr: SyncManager):
        self.scheduler = scheduler
        self.pipe_mgr = pipe_mgr
        self.sync_mgr = sync_mgr
        self.result_pipe = pipe_mgr.create_pipe("results", 100)
        self.fs_lock = sync_mgr.create_condition("fs_lock")
    
    def execute_command(self, cmd_name: str, cmd_func: Callable, args: tuple = (),
                       priority: Priority = Priority.MEDIUM) -> int:
        """
        执行命令
        将命令包装为进程并提交调度
        """
        def wrapped_task(*task_args):
            # 使用条件变量保护文件系统访问
            with self.fs_lock:
                result = cmd_func(*task_args)
                # 将结果写入管道
                self.result_pipe.write({
                    'cmd': cmd_name,
                    'result': result,
                    'time': time.time()
                })
                return result
        
        pcb = self.scheduler.create_process(cmd_name, wrapped_task, args, priority)
        self.scheduler.submit_process(pcb)
        return pcb.pid
    
    def get_result(self, timeout: float = 5.0) -> Optional[Dict]:
        """获取命令执行结果"""
        return self.result_pipe.read(timeout)


# ==================== 测试代码 ====================
if __name__ == "__main__":
    # 创建管理器
    pipe_mgr = PipeManager()
    sync_mgr = SyncManager()
    scheduler = PriorityScheduler()
    
    # 启动调度器
    scheduler.start()
    
    # 定义测试任务
    def compute_task(n):
        total = 0
        for i in range(n):
            total += i
        time.sleep(0.5)  # 模拟耗时
        return total
    
    def io_task(name):
        time.sleep(0.3)  # 模拟IO
        return f"IO完成: {name}"
    
    # 创建不同优先级的进程
    print("\n=== 创建进程 ===")
    p1 = scheduler.create_process("计算任务-高", compute_task, (1000,), Priority.HIGH)
    p2 = scheduler.create_process("IO任务-中", io_task, ("文件A",), Priority.MEDIUM)
    p3 = scheduler.create_process("计算任务-低", compute_task, (500,), Priority.LOW)
    p4 = scheduler.create_process("IO任务-高", io_task, ("文件B",), Priority.HIGH)
    
    # 提交进程
    print("\n=== 提交进程 ===")
    scheduler.submit_process(p1)
    scheduler.submit_process(p2)
    scheduler.submit_process(p3)
    scheduler.submit_process(p4)
    
    # 等待执行
    time.sleep(3)
    
    # 显示结果
    print("\n=== 进程列表 ===")
    for p in scheduler.get_process_list():
        print(f"  PID={p['pid']}, 名称={p['name']}, 状态={p['state']}, 优先级={p['priority']}")
    
    # 测试管道
    print("\n=== 管道测试 ===")
    test_pipe = pipe_mgr.create_pipe("test_pipe", 10)
    
    # 生产者
    def producer():
        for i in range(5):
            test_pipe.write(f"消息{i}")
            print(f"  [Producer] 发送: 消息{i}")
            time.sleep(0.1)
    
    # 消费者
    def consumer():
        for i in range(5):
            msg = test_pipe.read(timeout=1)
            print(f"  [Consumer] 接收: {msg}")
    
    t1 = threading.Thread(target=producer)
    t2 = threading.Thread(target=consumer)
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    
    print(f"\n管道统计: {test_pipe.get_stats()}")
    
    # 停止调度器
    scheduler.stop()
    
    print("\n=== 测试完成 ===")
