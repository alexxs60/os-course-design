#!/usr/bin/env python3
"""
操作系统课程设计 - 主程序
功能：整合文件系统、缓冲区、进程管理、异步I/O等模块
提供命令行界面进行交互操作

选题要求：
- 文件操作：异步I/O
- 进程通信：管道
- 同步互斥：条件变量
- 进程调度：优先级策略
- 空闲块管理：位图 + 索引结构
"""

import os
import sys
import time
import threading
from typing import Optional

# 导入各模块
from filesystem import FileSystem, BLOCK_SIZE, TOTAL_BLOCKS, DATA_START
from buffer import BufferManager, BUFFER_SIZE
from process import (PriorityScheduler, PipeManager, SyncManager, 
                     CommandProcessor, Priority, ProcessState)
from async_io import AsyncIOManager, IOStatus
from visualization import Visualizer

# ==================== 系统管理类 ====================

class OperatingSystemSimulator:
    """
    操作系统模拟器
    整合所有子系统，提供统一接口
    """
    
    def __init__(self):
        print("=" * 60)
        print("       操作系统课程设计 - 文件系统模拟器")
        print("=" * 60)
        print("选题：异步I/O | 管道 | 条件变量 | 优先级调度 | 位图+索引")
        print("=" * 60)
        
        # 初始化各子系统
        print("\n[系统] 正在初始化各子系统...")
        
        # 1. 文件系统
        self.fs = FileSystem()
        self.fs.mount()
        print("  ✓ 文件系统初始化完成")
        
        # 2. 缓冲区管理器
        self.buffer_mgr = BufferManager(self.fs)
        print(f"  ✓ 缓冲区管理器初始化完成 (容量: {BUFFER_SIZE}页)")
        
        # 3. 进程调度器
        self.scheduler = PriorityScheduler()
        print("  ✓ 优先级调度器初始化完成")
        
        # 4. 管道管理器
        self.pipe_mgr = PipeManager()
        print("  ✓ 管道管理器初始化完成")
        
        # 5. 同步管理器
        self.sync_mgr = SyncManager()
        # 创建文件系统条件变量
        self.fs_condition = self.sync_mgr.create_condition("fs_access")
        print("  ✓ 条件变量同步管理器初始化完成")
        
        # 6. 异步I/O管理器
        self.async_io = AsyncIOManager(self.fs, self.buffer_mgr)
        print("  ✓ 异步I/O管理器初始化完成")
        
        # 7. 可视化器
        self.visualizer = Visualizer()
        print("  ✓ 可视化模块初始化完成")
        
        # 命令处理器
        self.cmd_processor = CommandProcessor(self.scheduler, self.pipe_mgr, self.sync_mgr)
        
        print("\n[系统] 所有子系统初始化完成!")
        print("-" * 60)
    
    def start(self):
        """启动系统"""
        self.scheduler.start()
        self.async_io.start()
        print("[系统] 调度器和异步I/O已启动")
    
    def stop(self):
        """停止系统"""
        # 刷新所有脏页
        print("[系统] 正在刷新缓冲区...")
        self.buffer_mgr.flush_all()
        
        self.async_io.stop()
        self.scheduler.stop()
        print("[系统] 系统已停止")
    
    # ==================== 文件操作命令 ====================
    
    def cmd_create_file(self, filename: str, content: str, priority: str = "medium"):
        """创建文件命令"""
        pri = self._parse_priority(priority)
        content_bytes = content.encode('utf-8')
        
        print(f"\n[命令] 创建文件: {filename} (优先级: {pri.name})")
        
        def task():
            with self.fs_condition:
                return self.fs.create_file(filename, content_bytes)
        
        # 使用异步I/O
        def callback(request):
            if request.status == IOStatus.COMPLETED:
                print(f"  ✓ 文件 '{filename}' 创建成功")
            else:
                print(f"  ✗ 创建失败: {request.error}")
        
        req_id = self.async_io.submit_create(filename, content_bytes, 
                                             callback=callback, 
                                             priority=pri.value)
        return req_id
    
    def cmd_read_file(self, filename: str, block_index: int = -1, priority: str = "medium"):
        """读取文件命令"""
        pri = self._parse_priority(priority)
        
        if block_index >= 0:
            print(f"\n[命令] 读取文件块: {filename}[{block_index}] (优先级: {pri.name})")
        else:
            print(f"\n[命令] 读取文件: {filename} (优先级: {pri.name})")
        
        def callback(request):
            if request.status == IOStatus.COMPLETED:
                data = request.result
                if data:
                    preview = data[:100].decode('utf-8', errors='replace')
                    print(f"  ✓ 读取成功, 内容预览: {preview}...")
                else:
                    print(f"  ✓ 读取成功, 但内容为空")
            else:
                print(f"  ✗ 读取失败: {request.error}")
        
        req_id = self.async_io.submit_read(filename, block_index,
                                           callback=callback,
                                           priority=pri.value)
        return req_id
    
    def cmd_write_block(self, filename: str, block_index: int, content: str, priority: str = "medium"):
        """写入文件块命令"""
        pri = self._parse_priority(priority)
        content_bytes = content.encode('utf-8')
        
        print(f"\n[命令] 写入文件块: {filename}[{block_index}] (优先级: {pri.name})")
        
        def callback(request):
            if request.status == IOStatus.COMPLETED:
                print(f"  ✓ 写入成功")
            else:
                print(f"  ✗ 写入失败: {request.error}")
        
        req_id = self.async_io.submit_write(filename, block_index, content_bytes,
                                            callback=callback,
                                            priority=pri.value)
        return req_id
    
    def cmd_delete_file(self, filename: str, priority: str = "medium"):
        """删除文件命令"""
        pri = self._parse_priority(priority)
        
        print(f"\n[命令] 删除文件: {filename} (优先级: {pri.name})")
        
        def callback(request):
            if request.status == IOStatus.COMPLETED and request.result:
                print(f"  ✓ 文件 '{filename}' 删除成功")
            else:
                print(f"  ✗ 删除失败: {request.error}")
        
        req_id = self.async_io.submit_delete(filename, callback=callback,
                                             priority=pri.value)
        return req_id
    
    def cmd_list_directory(self):
        """列出目录命令"""
        print("\n[命令] 列出目录")
        files = self.fs.list_directory()
        
        if not files:
            print("  (目录为空)")
            return []
        
        print(f"\n  {'文件名':<20} {'大小':>10} {'块数':>6} {'权限':>6} {'引用':>4} {'创建时间'}")
        print("  " + "-" * 80)
        
        for f in files:
            print(f"  {f['name']:<20} {f['size']:>10}B {f['blocks']:>6} {f['permission']:>6} {f['ref_count']:>4} {f['create_time']}")
        
        return files
    
    # ==================== 系统信息命令 ====================
    
    def cmd_disk_info(self):
        """显示磁盘信息"""
        print("\n[命令] 磁盘信息")
        info = self.fs.get_disk_info()
        
        print(f"\n  磁盘配置:")
        print(f"    块大小: {info['block_size']} 字节")
        print(f"    总块数: {info['total_blocks']}")
        print(f"    数据区起始: 块 {info['data_start']}")
        
        print(f"\n  使用情况:")
        print(f"    已使用块: {info['used_blocks']}")
        print(f"    空闲块: {info['free_blocks']}")
        usage = info['used_blocks'] / info['total_blocks'] * 100
        print(f"    使用率: {usage:.1f}%")
        
        print(f"\n  iNode:")
        print(f"    总数: {info['total_inodes']}")
        print(f"    空闲: {info['free_inodes']}")
        
        return info
    
    def cmd_buffer_status(self):
        """显示缓冲区状态"""
        print("\n[命令] 缓冲区状态")
        
        status = self.buffer_mgr.get_buffer_status()
        stats = self.buffer_mgr.get_statistics()
        
        print(f"\n  统计信息:")
        print(f"    命中次数: {stats['hit_count']}")
        print(f"    未命中次数: {stats['miss_count']}")
        print(f"    命中率: {stats['hit_rate']}")
        print(f"    写回次数: {stats['writeback_count']}")
        print(f"    有效页: {stats['valid_pages']}")
        print(f"    脏页: {stats['dirty_pages']}")
        
        print(f"\n  缓冲页详情:")
        print(f"  {'页号':>4} {'块号':>6} {'文件名':<15} {'状态':>8} {'访问次数':>8}")
        print("  " + "-" * 50)
        
        for page in status:
            if page['is_valid']:
                state = '脏' if page['is_dirty'] else '有效'
                print(f"  {page['page_id']:>4} {page['block_num']:>6} {page['filename']:<15} {state:>8} {page['access_count']:>8}")
        
        return status, stats
    
    def cmd_process_status(self):
        """显示进程状态"""
        print("\n[命令] 进程状态")
        
        processes = self.scheduler.get_process_list()
        queue_status = self.scheduler.get_queue_status()
        
        print(f"\n  调度队列:")
        print(f"    高优先级: {queue_status['high']}")
        print(f"    中优先级: {queue_status['medium']}")
        print(f"    低优先级: {queue_status['low']}")
        print(f"    当前运行: {queue_status['running']}")
        print(f"    总调度次数: {queue_status['total_scheduled']}")
        
        if processes:
            print(f"\n  进程列表:")
            print(f"  {'PID':>6} {'名称':<20} {'优先级':<8} {'状态':<8} {'CPU时间'}")
            print("  " + "-" * 60)
            
            for p in processes[-10:]:
                print(f"  {p['pid']:>6} {p['name']:<20} {p['priority']:<8} {p['state']:<8} {p['cpu_time']}")
        
        return processes, queue_status
    
    def cmd_pipe_status(self):
        """显示管道状态"""
        print("\n[命令] 管道状态")
        
        pipes = self.pipe_mgr.list_pipes()
        
        if not pipes:
            print("  (无活动管道)")
            return []
        
        for p in pipes:
            print(f"\n  管道: {p['name']}")
            print(f"    缓冲大小: {p['buffer_size']}")
            print(f"    当前数据: {p['current_size']}")
            print(f"    写入次数: {p['write_count']}")
            print(f"    读取次数: {p['read_count']}")
            print(f"    已关闭: {p['closed']}")
        
        return pipes
    
    def cmd_async_io_status(self):
        """显示异步I/O状态"""
        print("\n[命令] 异步I/O状态")
        
        stats = self.async_io.get_statistics()
        pending = self.async_io.get_pending_requests()
        
        print(f"\n  统计信息:")
        print(f"    总请求数: {stats['total_requests']}")
        print(f"    已完成: {stats['completed']}")
        print(f"    失败: {stats['failed']}")
        print(f"    待处理: {stats['pending']}")
        print(f"    执行中: {stats['running']}")
        print(f"    读取字节: {stats['total_bytes_read']}")
        print(f"    写入字节: {stats['total_bytes_written']}")
        
        if pending:
            print(f"\n  待处理请求:")
            for req in pending:
                print(f"    ID={req['request_id']}, 类型={req['io_type']}, 文件={req['filename']}")
        
        return stats
    
    # ==================== 可视化命令 ====================
    
    def cmd_visualize_bitmap(self, save_path: str = "bitmap.png"):
        """生成位图可视化"""
        print(f"\n[命令] 生成位图可视化 -> {save_path}")
        
        bitmap_data = self.fs.get_bitmap_visual()
        fig = self.visualizer.create_bitmap_figure(bitmap_data, DATA_START)
        self.visualizer.save_figure(fig, save_path)
        
        print(f"  ✓ 已保存到 {save_path}")
        return save_path
    
    def cmd_visualize_buffer(self, save_path: str = "buffer.png"):
        """生成缓冲区可视化"""
        print(f"\n[命令] 生成缓冲区可视化 -> {save_path}")
        
        status = self.buffer_mgr.get_buffer_status()
        fig = self.visualizer.create_buffer_figure(status)
        self.visualizer.save_figure(fig, save_path)
        
        print(f"  ✓ 已保存到 {save_path}")
        return save_path
    
    def cmd_visualize_process(self, save_path: str = "process.png"):
        """生成进程调度可视化"""
        print(f"\n[命令] 生成进程调度可视化 -> {save_path}")
        
        processes = self.scheduler.get_process_list()
        queue_status = self.scheduler.get_queue_status()
        fig = self.visualizer.create_process_figure(processes, queue_status)
        self.visualizer.save_figure(fig, save_path)
        
        print(f"  ✓ 已保存到 {save_path}")
        return save_path
    
    def cmd_visualize_disk(self, save_path: str = "disk_info.png"):
        """生成磁盘信息可视化"""
        print(f"\n[命令] 生成磁盘信息可视化 -> {save_path}")
        
        disk_info = self.fs.get_disk_info()
        file_list = self.fs.list_directory()
        fig = self.visualizer.create_disk_info_figure(disk_info, file_list)
        self.visualizer.save_figure(fig, save_path)
        
        print(f"  ✓ 已保存到 {save_path}")
        return save_path
    
    def cmd_visualize_buffer_stats(self, save_path: str = "buffer_stats.png"):
        """生成缓冲区统计可视化"""
        print(f"\n[命令] 生成缓冲区统计可视化 -> {save_path}")
        
        stats = self.buffer_mgr.get_statistics()
        fig = self.visualizer.create_buffer_stats_figure(stats)
        self.visualizer.save_figure(fig, save_path)
        
        print(f"  ✓ 已保存到 {save_path}")
        return save_path
    
    # ==================== 辅助方法 ====================
    
    def _parse_priority(self, priority_str: str) -> Priority:
        """解析优先级字符串"""
        mapping = {
            'high': Priority.HIGH,
            'h': Priority.HIGH,
            '1': Priority.HIGH,
            'medium': Priority.MEDIUM,
            'm': Priority.MEDIUM,
            '2': Priority.MEDIUM,
            'low': Priority.LOW,
            'l': Priority.LOW,
            '3': Priority.LOW,
        }
        return mapping.get(priority_str.lower(), Priority.MEDIUM)
    
    def wait_for_io(self, request_id: int, timeout: float = 10.0) -> bool:
        """等待异步I/O完成"""
        return self.async_io.wait_for_request(request_id, timeout)


# ==================== 演示程序 ====================

def run_demo():
    """运行演示程序"""
    os_sim = OperatingSystemSimulator()
    os_sim.start()
    
    try:
        print("\n" + "=" * 60)
        print("              开始系统演示")
        print("=" * 60)
        
        # 1. 创建测试文件
        print("\n>>> 1. 创建测试文件")
        os_sim.cmd_create_file("hello.txt", "Hello, World! 这是一个测试文件。" * 20, "high")
        os_sim.cmd_create_file("data.txt", "Data file content. " * 50, "medium")
        os_sim.cmd_create_file("log.txt", "Log entry 1\nLog entry 2\n" * 30, "low")
        
        time.sleep(2)
        
        # 2. 列出目录
        print("\n>>> 2. 列出目录")
        os_sim.cmd_list_directory()
        
        # 3. 读取文件
        print("\n>>> 3. 读取文件")
        os_sim.cmd_read_file("hello.txt", priority="high")
        os_sim.cmd_read_file("data.txt", block_index=0, priority="medium")
        
        time.sleep(2)
        
        # 4. 修改文件
        print("\n>>> 4. 修改文件块")
        os_sim.cmd_write_block("hello.txt", 0, "MODIFIED CONTENT! ", "high")
        
        time.sleep(1)
        
        # 5. 显示系统状态
        print("\n>>> 5. 系统状态")
        os_sim.cmd_disk_info()
        os_sim.cmd_buffer_status()
        os_sim.cmd_async_io_status()
        
        # 6. 生成可视化图表
        print("\n>>> 6. 生成可视化图表")
        os_sim.cmd_visualize_bitmap()
        os_sim.cmd_visualize_buffer()
        os_sim.cmd_visualize_disk()
        os_sim.cmd_visualize_buffer_stats()
        
        # 7. 删除文件测试
        print("\n>>> 7. 删除文件")
        os_sim.cmd_delete_file("log.txt")
        
        time.sleep(1)
        
        # 8. 最终状态
        print("\n>>> 8. 最终目录")
        os_sim.cmd_list_directory()
        
        print("\n" + "=" * 60)
        print("              演示完成")
        print("=" * 60)
        
    finally:
        os_sim.stop()


# ==================== 交互式Shell ====================

def run_shell():
    """运行交互式Shell"""
    os_sim = OperatingSystemSimulator()
    os_sim.start()
    
    print("\n输入 'help' 查看可用命令, 'quit' 退出\n")
    
    try:
        while True:
            try:
                cmd_line = input("OS> ").strip()
                if not cmd_line:
                    continue
                
                parts = cmd_line.split()
                cmd = parts[0].lower()
                args = parts[1:]
                
                if cmd in ['quit', 'exit', 'q']:
                    break
                elif cmd == 'help':
                    print_help()
                elif cmd == 'create':
                    if len(args) >= 2:
                        filename = args[0]
                        content = ' '.join(args[1:])
                        os_sim.cmd_create_file(filename, content)
                        time.sleep(0.5)
                    else:
                        print("用法: create <文件名> <内容>")
                elif cmd == 'read':
                    if len(args) >= 1:
                        filename = args[0]
                        block = int(args[1]) if len(args) > 1 else -1
                        os_sim.cmd_read_file(filename, block)
                        time.sleep(0.5)
                    else:
                        print("用法: read <文件名> [块号]")
                elif cmd == 'write':
                    if len(args) >= 3:
                        filename = args[0]
                        block = int(args[1])
                        content = ' '.join(args[2:])
                        os_sim.cmd_write_block(filename, block, content)
                        time.sleep(0.5)
                    else:
                        print("用法: write <文件名> <块号> <内容>")
                elif cmd == 'delete':
                    if len(args) >= 1:
                        os_sim.cmd_delete_file(args[0])
                        time.sleep(0.5)
                    else:
                        print("用法: delete <文件名>")
                elif cmd in ['ls', 'dir', 'list']:
                    os_sim.cmd_list_directory()
                elif cmd == 'disk':
                    os_sim.cmd_disk_info()
                elif cmd == 'buffer':
                    os_sim.cmd_buffer_status()
                elif cmd == 'process':
                    os_sim.cmd_process_status()
                elif cmd == 'pipe':
                    os_sim.cmd_pipe_status()
                elif cmd == 'io':
                    os_sim.cmd_async_io_status()
                elif cmd == 'viz':
                    if len(args) >= 1:
                        viz_type = args[0]
                        if viz_type == 'bitmap':
                            os_sim.cmd_visualize_bitmap()
                        elif viz_type == 'buffer':
                            os_sim.cmd_visualize_buffer()
                        elif viz_type == 'process':
                            os_sim.cmd_visualize_process()
                        elif viz_type == 'disk':
                            os_sim.cmd_visualize_disk()
                        else:
                            print("可视化类型: bitmap, buffer, process, disk")
                    else:
                        print("用法: viz <bitmap|buffer|process|disk>")
                else:
                    print(f"未知命令: {cmd}, 输入 'help' 查看帮助")
                    
            except KeyboardInterrupt:
                print("\n")
                continue
            except Exception as e:
                print(f"错误: {e}")
    
    finally:
        os_sim.stop()


def print_help():
    """打印帮助信息"""
    print("""
可用命令:
  文件操作:
    create <文件名> <内容>      创建文件
    read <文件名> [块号]        读取文件或指定块
    write <文件名> <块号> <内容> 写入指定块
    delete <文件名>             删除文件
    ls / dir / list            列出目录
    
  系统信息:
    disk                        显示磁盘信息
    buffer                      显示缓冲区状态
    process                     显示进程状态
    pipe                        显示管道状态
    io                          显示异步I/O状态
    
  可视化:
    viz bitmap                  生成位图可视化
    viz buffer                  生成缓冲区可视化
    viz process                 生成进程可视化
    viz disk                    生成磁盘信息可视化
    
  其他:
    help                        显示帮助
    quit / exit                 退出
""")


# ==================== 主程序入口 ====================

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == 'demo':
        run_demo()
    else:
        run_shell()
