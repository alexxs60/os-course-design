#!/usr/bin/env python3
"""
操作系统课程设计 - GUI图形界面
功能：提供可视化的文件系统操作界面
"""

import os
import sys
import time
import threading
import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext, simpledialog
from tkinter import filedialog
import matplotlib

matplotlib.use('TkAgg')
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
from matplotlib.figure import Figure
import numpy as np

# 设置matplotlib中文字体
import matplotlib.pyplot as plt

plt.rcParams['font.sans-serif'] = ['Noto Sans CJK SC', 'WenQuanYi Zen Hei', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['font.family'] = 'sans-serif'

# 导入各模块
from filesystem import FileSystem, BLOCK_SIZE, TOTAL_BLOCKS, DATA_START, MAX_INODES
from buffer import BufferManager, BUFFER_SIZE
from process import (PriorityScheduler, PipeManager, SyncManager,
                     CommandProcessor, Priority, ProcessState)
from async_io import AsyncIOManager, IOStatus
from visualization import Visualizer, COLORS


class FileSystemGUI:
    """文件系统图形界面"""

    def __init__(self, root):
        self.root = root
        self.root.title("操作系统课程设计 - 文件系统模拟器")
        self.root.geometry("1400x900")
        self.root.configure(bg='#f0f0f0')

        # 初始化子系统
        self.init_subsystems()

        # 创建界面
        self.create_ui()

        # 启动系统
        self.start_system()

        # 定时刷新
        self.auto_refresh()

        # 绑定关闭事件
        self.root.protocol("WM_DELETE_WINDOW", self.on_closing)

    def init_subsystems(self):
        """初始化所有子系统"""
        # 文件系统
        self.fs = FileSystem()
        self.fs.mount()

        # 缓冲区管理器
        self.buffer_mgr = BufferManager(self.fs)

        # 进程调度器
        self.scheduler = PriorityScheduler()

        # 管道管理器
        self.pipe_mgr = PipeManager()

        # 同步管理器
        self.sync_mgr = SyncManager()
        self.fs_condition = self.sync_mgr.create_condition("fs_access")

        # 异步I/O管理器
        self.async_io = AsyncIOManager(self.fs, self.buffer_mgr)

        # 可视化器
        self.visualizer = Visualizer()

    def start_system(self):
        """启动系统"""
        self.scheduler.start()
        self.async_io.start()

    def create_ui(self):
        """创建用户界面"""
        # 创建主框架
        self.main_frame = ttk.Frame(self.root)
        self.main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # 创建标题
        title_frame = ttk.Frame(self.main_frame)
        title_frame.pack(fill=tk.X, pady=(0, 10))

        title_label = ttk.Label(title_frame,
                                text="操作系统课程设计 - 文件系统模拟器",
                                font=('Microsoft YaHei', 18, 'bold'))
        title_label.pack()

        subtitle_label = ttk.Label(title_frame,
                                   text="选题：异步I/O | 管道 | 条件变量 | 优先级调度 | 位图+索引",
                                   font=('Microsoft YaHei', 10))
        subtitle_label.pack()

        # 创建左右分栏
        paned = ttk.PanedWindow(self.main_frame, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)

        # 左侧面板 - 操作区
        left_frame = ttk.Frame(paned, width=400)
        paned.add(left_frame, weight=1)

        # 右侧面板 - 可视化区
        right_frame = ttk.Frame(paned, width=900)
        paned.add(right_frame, weight=2)

        # 创建左侧内容
        self.create_left_panel(left_frame)

        # 创建右侧内容
        self.create_right_panel(right_frame)

    def create_left_panel(self, parent):
        """创建左侧操作面板"""
        # 文件操作区
        file_frame = ttk.LabelFrame(parent, text="文件操作", padding=10)
        file_frame.pack(fill=tk.X, pady=5)

        # 文件名输入
        ttk.Label(file_frame, text="文件名:").grid(row=0, column=0, sticky=tk.W)
        self.filename_entry = ttk.Entry(file_frame, width=25)
        self.filename_entry.grid(row=0, column=1, columnspan=2, pady=2)
        self.filename_entry.insert(0, "test.txt")

        # 内容输入
        ttk.Label(file_frame, text="内容:").grid(row=1, column=0, sticky=tk.W)
        self.content_text = scrolledtext.ScrolledText(file_frame, width=30, height=4)
        self.content_text.grid(row=1, column=1, columnspan=2, pady=2)
        self.content_text.insert(tk.END, "Hello, World! 这是测试内容。")

        # 优先级选择
        ttk.Label(file_frame, text="优先级:").grid(row=2, column=0, sticky=tk.W)
        self.priority_var = tk.StringVar(value="medium")
        priority_combo = ttk.Combobox(file_frame, textvariable=self.priority_var,
                                      values=["high", "medium", "low"], width=10)
        priority_combo.grid(row=2, column=1, sticky=tk.W, pady=2)

        # 操作按钮
        btn_frame = ttk.Frame(file_frame)
        btn_frame.grid(row=3, column=0, columnspan=3, pady=10)

        ttk.Button(btn_frame, text="创建文件", command=self.create_file, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="读取文件", command=self.read_file, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="修改文件", command=self.edit_file, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame, text="删除文件", command=self.delete_file, width=10).pack(side=tk.LEFT, padx=2)

        # 文件列表
        list_frame = ttk.LabelFrame(parent, text="文件目录", padding=10)
        list_frame.pack(fill=tk.BOTH, expand=True, pady=5)

        # 创建Treeview
        columns = ('name', 'size', 'blocks', 'permission', 'time')
        self.file_tree = ttk.Treeview(list_frame, columns=columns, show='headings', height=8)

        self.file_tree.heading('name', text='文件名')
        self.file_tree.heading('size', text='大小')
        self.file_tree.heading('blocks', text='块数')
        self.file_tree.heading('permission', text='权限')
        self.file_tree.heading('time', text='创建时间')

        self.file_tree.column('name', width=100)
        self.file_tree.column('size', width=60)
        self.file_tree.column('blocks', width=40)
        self.file_tree.column('permission', width=50)
        self.file_tree.column('time', width=80)

        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.file_tree.yview)
        self.file_tree.configure(yscrollcommand=scrollbar.set)

        self.file_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        # 双击读取文件
        self.file_tree.bind('<Double-1>', self.on_file_double_click)

        ttk.Button(list_frame, text="刷新列表", command=self.refresh_file_list).pack(pady=5)

        # 系统信息区
        info_frame = ttk.LabelFrame(parent, text="系统信息", padding=10)
        info_frame.pack(fill=tk.X, pady=5)

        self.info_text = tk.Text(info_frame, height=8, width=40, state=tk.DISABLED)
        self.info_text.pack(fill=tk.X)

        # 操作按钮
        btn_frame2 = ttk.Frame(info_frame)
        btn_frame2.pack(pady=5)

        ttk.Button(btn_frame2, text="磁盘信息", command=self.show_disk_info, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame2, text="缓冲状态", command=self.show_buffer_status, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame2, text="块操作", command=self.show_block_operation, width=10).pack(side=tk.LEFT, padx=2)
        ttk.Button(btn_frame2, text="进程管理", command=self.show_process_management, width=10).pack(side=tk.LEFT, padx=2)

    def create_right_panel(self, parent):
        """创建右侧可视化面板"""
        # 创建Notebook选项卡
        self.notebook = ttk.Notebook(parent)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # 位图页面
        self.bitmap_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.bitmap_frame, text="磁盘位图")
        self.create_bitmap_canvas(self.bitmap_frame)

        # 缓冲区页面
        self.buffer_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.buffer_frame, text="缓冲区状态")
        self.create_buffer_canvas(self.buffer_frame)

        # 磁盘信息页面
        self.disk_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.disk_frame, text="磁盘使用")
        self.create_disk_canvas(self.disk_frame)

        # 进程调度页面
        self.process_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.process_frame, text="进程调度")
        self.create_process_canvas(self.process_frame)

        # 日志页面
        self.log_frame = ttk.Frame(self.notebook)
        self.notebook.add(self.log_frame, text="操作日志")
        self.create_log_panel(self.log_frame)

        # 刷新按钮
        refresh_frame = ttk.Frame(parent)
        refresh_frame.pack(fill=tk.X, pady=5)
        ttk.Button(refresh_frame, text="刷新所有视图", command=self.refresh_all_views).pack()

    def create_bitmap_canvas(self, parent):
        """创建位图画布"""
        self.bitmap_fig = Figure(figsize=(9, 7), dpi=100)
        self.bitmap_canvas = FigureCanvasTkAgg(self.bitmap_fig, parent)
        self.bitmap_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.update_bitmap_view()

    def create_buffer_canvas(self, parent):
        """创建缓冲区画布"""
        self.buffer_fig = Figure(figsize=(9, 5), dpi=100)
        self.buffer_canvas = FigureCanvasTkAgg(self.buffer_fig, parent)
        self.buffer_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.update_buffer_view()

    def create_disk_canvas(self, parent):
        """创建磁盘信息画布"""
        self.disk_fig = Figure(figsize=(9, 5), dpi=100)
        self.disk_canvas = FigureCanvasTkAgg(self.disk_fig, parent)
        self.disk_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.update_disk_view()

    def create_process_canvas(self, parent):
        """创建进程调度画布"""
        self.process_fig = Figure(figsize=(9, 6), dpi=100)
        self.process_canvas = FigureCanvasTkAgg(self.process_fig, parent)
        self.process_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
        self.update_process_view()

    def create_log_panel(self, parent):
        """创建日志面板"""
        self.log_text = scrolledtext.ScrolledText(parent, width=100, height=30)
        self.log_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.log("系统启动完成")

    def log(self, message):
        """添加日志"""
        timestamp = time.strftime("%H:%M:%S")
        self.log_text.insert(tk.END, f"[{timestamp}] {message}\n")
        self.log_text.see(tk.END)

    # ==================== 文件操作 ====================

    def create_file(self):
        """创建文件"""
        filename = self.filename_entry.get().strip()
        content = self.content_text.get("1.0", tk.END).strip()
        priority = self.priority_var.get()

        if not filename:
            messagebox.showwarning("警告", "请输入文件名")
            return

        if not content:
            content = " "  # 至少一个字符

        content_bytes = content.encode('utf-8')
        pri_value = {"high": 1, "medium": 5, "low": 9}.get(priority, 5)
        pri_enum = {"high": Priority.HIGH, "medium": Priority.MEDIUM, "low": Priority.LOW}.get(priority,
                                                                                               Priority.MEDIUM)

        # 创建对应的进程
        def create_task():
            time.sleep(0.5)  # 模拟处理时间
            return f"创建文件: {filename}"

        pcb = self.scheduler.create_process(f"创建:{filename}", create_task, (), pri_enum)
        self.scheduler.submit_process(pcb)
        self.update_process_view()  # 立即更新进程视图

        def callback(request):
            if request.status == IOStatus.COMPLETED:
                self.root.after(0, lambda: self.on_file_created(filename, True))
            else:
                self.root.after(0, lambda: self.on_file_created(filename, False, request.error))

        self.async_io.submit_create(filename, content_bytes, callback=callback, priority=pri_value)
        self.log(f"提交创建文件请求: {filename}")

    def on_file_created(self, filename, success, error=None):
        """文件创建完成回调"""
        if success:
            self.log(f"文件 '{filename}' 创建成功")
            messagebox.showinfo("成功", f"文件 '{filename}' 创建成功")
        else:
            self.log(f"文件 '{filename}' 创建失败: {error}")
            messagebox.showerror("失败", f"创建失败: {error}")
        self.refresh_all_views()

    def read_file(self):
        """读取文件"""
        filename = self.filename_entry.get().strip()
        if not filename:
            messagebox.showwarning("警告", "请输入文件名")
            return

        # 创建对应的进程
        def read_task():
            time.sleep(0.5)
            return f"读取文件: {filename}"

        pcb = self.scheduler.create_process(f"读取:{filename}", read_task, (), Priority.MEDIUM)
        self.scheduler.submit_process(pcb)
        self.update_process_view()  # 立即更新进程视图

        def callback(request):
            if request.status == IOStatus.COMPLETED:
                self.root.after(0, lambda: self.on_file_read(filename, request.result))
            else:
                self.root.after(0, lambda: self.on_file_read(filename, None, request.error))

        self.async_io.submit_read(filename, callback=callback)
        self.log(f"提交读取文件请求: {filename}")

    def on_file_read(self, filename, data, error=None):
        """文件读取完成回调"""
        if data is not None:
            try:
                content = data.decode('utf-8')
            except:
                content = data.hex()

            self.log(f"文件 '{filename}' 读取成功, 大小: {len(data)} 字节")

            # 显示内容窗口
            self.show_file_content(filename, content)
        else:
            self.log(f"文件 '{filename}' 读取失败: {error}")
            messagebox.showerror("失败", f"读取失败: {error}")
        self.refresh_all_views()

    def show_file_content(self, filename, content):
        """显示文件内容窗口（只读）"""
        window = tk.Toplevel(self.root)
        window.title(f"文件内容 - {filename}")
        window.geometry("600x400")

        text = scrolledtext.ScrolledText(window, width=70, height=20)
        text.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        text.insert(tk.END, content)
        text.configure(state=tk.DISABLED)

        ttk.Button(window, text="关闭", command=window.destroy).pack(pady=5)

    def edit_file(self):
        """编辑文件"""
        filename = self.filename_entry.get().strip()
        if not filename:
            messagebox.showwarning("警告", "请输入文件名")
            return

        def callback(request):
            if request.status == IOStatus.COMPLETED:
                self.root.after(0, lambda: self.on_file_read_for_edit(filename, request.result))
            else:
                self.root.after(0, lambda: self.on_file_read_for_edit(filename, None, request.error))

        self.async_io.submit_read(filename, callback=callback)
        self.log(f"提交读取文件请求（用于编辑）: {filename}")

    def on_file_read_for_edit(self, filename, data, error=None):
        """文件读取完成后打开编辑窗口"""
        if data is not None:
            try:
                content = data.decode('utf-8')
            except:
                content = data.hex()

            self.log(f"文件 '{filename}' 读取成功, 准备编辑")
            self.show_edit_window(filename, content)
        else:
            self.log(f"文件 '{filename}' 读取失败: {error}")
            messagebox.showerror("失败", f"读取失败: {error}")

    def show_edit_window(self, filename, content):
        """显示文件编辑窗口"""
        window = tk.Toplevel(self.root)
        window.title(f"编辑文件 - {filename}")
        window.geometry("700x500")
        window.transient(self.root)

        # 顶部信息栏
        info_frame = ttk.Frame(window)
        info_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(info_frame, text=f"正在编辑: {filename}", font=('Microsoft YaHei', 10, 'bold')).pack(side=tk.LEFT)

        # 原始大小标签
        original_size = len(content.encode('utf-8'))
        size_label = ttk.Label(info_frame, text=f"原始大小: {original_size} 字节")
        size_label.pack(side=tk.RIGHT)

        # 编辑区域
        edit_frame = ttk.Frame(window)
        edit_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        text_widget = scrolledtext.ScrolledText(edit_frame, width=80, height=25, font=('Consolas', 10))
        text_widget.pack(fill=tk.BOTH, expand=True)
        text_widget.insert(tk.END, content)

        # 当前大小标签
        current_size_var = tk.StringVar(value=f"当前大小: {original_size} 字节")
        current_size_label = ttk.Label(info_frame, textvariable=current_size_var)
        current_size_label.pack(side=tk.RIGHT, padx=20)

        # 更新大小显示的函数
        def update_size(*args):
            try:
                current_content = text_widget.get("1.0", tk.END).rstrip('\n')
                current_bytes = len(current_content.encode('utf-8'))
                current_size_var.set(f"当前大小: {current_bytes} 字节")
            except:
                pass

        # 绑定文本变化事件
        text_widget.bind('<KeyRelease>', update_size)

        # 按钮区域
        btn_frame = ttk.Frame(window)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        def save_file():
            """保存文件内容"""
            new_content = text_widget.get("1.0", tk.END).rstrip('\n')
            new_content_bytes = new_content.encode('utf-8')

            # 确认保存
            new_size = len(new_content_bytes)
            if not messagebox.askyesno("确认保存",
                                       f"确定要保存文件 '{filename}' 吗？\n\n原始大小: {original_size} 字节\n新大小: {new_size} 字节"):
                return

            def save_callback(request):
                if request.status == IOStatus.COMPLETED and request.result:
                    self.root.after(0, lambda: self.on_file_saved(filename, window, True))
                else:
                    self.root.after(0, lambda: self.on_file_saved(filename, window, False, request.error))

            # 提交写入请求
            self.async_io.submit_write(filename, -1, new_content_bytes, callback=save_callback)
            self.log(f"提交保存文件请求: {filename}, 大小: {new_size} 字节")

        def cancel_edit():
            """取消编辑"""
            if messagebox.askyesno("确认", "确定要放弃修改吗？"):
                window.destroy()

        ttk.Button(btn_frame, text="保存", command=save_file, width=12).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="取消", command=cancel_edit, width=12).pack(side=tk.LEFT, padx=5)

        # 添加快捷键
        window.bind('<Control-s>', lambda e: save_file())
        window.bind('<Escape>', lambda e: cancel_edit())

        # 状态提示
        ttk.Label(btn_frame, text="提示: Ctrl+S 保存, Esc 取消", foreground='gray').pack(side=tk.RIGHT)

    def on_file_saved(self, filename, edit_window, success, error=None):
        """文件保存完成回调"""
        if success:
            self.log(f"文件 '{filename}' 保存成功")
            messagebox.showinfo("成功", f"文件 '{filename}' 保存成功")
            edit_window.destroy()
            self.refresh_all_views()
        else:
            self.log(f"文件 '{filename}' 保存失败: {error}")
            messagebox.showerror("失败", f"保存失败: {error}")

    def delete_file(self):
        """删除文件"""
        filename = self.filename_entry.get().strip()
        if not filename:
            messagebox.showwarning("警告", "请输入文件名")
            return

        if not messagebox.askyesno("确认", f"确定要删除文件 '{filename}' 吗？"):
            return

        # 创建对应的进程
        def delete_task():
            time.sleep(0.5)
            return f"删除文件: {filename}"

        pcb = self.scheduler.create_process(f"删除:{filename}", delete_task, (), Priority.LOW)
        self.scheduler.submit_process(pcb)
        self.update_process_view()  # 立即更新进程视图

        def callback(request):
            if request.status == IOStatus.COMPLETED and request.result:
                self.root.after(0, lambda: self.on_file_deleted(filename, True))
            else:
                self.root.after(0, lambda: self.on_file_deleted(filename, False, request.error))

        self.async_io.submit_delete(filename, callback=callback)
        self.log(f"提交删除文件请求: {filename}")

    def on_file_deleted(self, filename, success, error=None):
        """文件删除完成回调"""
        if success:
            self.log(f"文件 '{filename}' 删除成功")
            messagebox.showinfo("成功", f"文件 '{filename}' 删除成功")
        else:
            self.log(f"文件 '{filename}' 删除失败: {error}")
            messagebox.showerror("失败", f"删除失败: {error}")
        self.refresh_all_views()

    def on_file_double_click(self, event):
        """双击文件列表项"""
        selection = self.file_tree.selection()
        if selection:
            item = self.file_tree.item(selection[0])
            filename = item['values'][0]
            self.filename_entry.delete(0, tk.END)
            self.filename_entry.insert(0, filename)
            self.read_file()

    def refresh_file_list(self):
        """刷新文件列表"""
        # 清空现有项
        for item in self.file_tree.get_children():
            self.file_tree.delete(item)

        # 获取文件列表
        files = self.fs.list_directory()

        for f in files:
            self.file_tree.insert('', tk.END, values=(
                f['name'],
                f"{f['size']}B",
                f['blocks'],
                f['permission'],
                f['create_time'][-8:] if f['create_time'] != 'N/A' else 'N/A'
            ))

    # ==================== 系统信息显示 ====================

    def show_disk_info(self):
        """显示磁盘信息"""
        info = self.fs.get_disk_info()

        text = f"""磁盘配置:
  块大小: {info['block_size']} 字节
  总块数: {info['total_blocks']}
  数据区起始: 块 {info['data_start']}

使用情况:
  已使用块: {info['used_blocks']}
  空闲块: {info['free_blocks']}
  使用率: {info['used_blocks'] / info['total_blocks'] * 100:.1f}%

iNode:
  总数: {info['total_inodes']}
  空闲: {info['free_inodes']}
"""
        self.update_info_text(text)
        self.log("查看磁盘信息")

    def show_buffer_status(self):
        """显示缓冲区状态"""
        stats = self.buffer_mgr.get_statistics()

        text = f"""缓冲区统计:
  命中次数: {stats['hit_count']}
  未命中次数: {stats['miss_count']}
  命中率: {stats['hit_rate']}
  写回次数: {stats['writeback_count']}
  有效页: {stats['valid_pages']}
  脏页: {stats['dirty_pages']}
"""
        self.update_info_text(text)
        self.log("查看缓冲区状态")

    def show_io_status(self):
        """显示I/O状态"""
        stats = self.async_io.get_statistics()

        text = f"""异步I/O统计:
  总请求数: {stats['total_requests']}
  已完成: {stats['completed']}
  失败: {stats['failed']}
  待处理: {stats['pending']}
  执行中: {stats['running']}
  读取字节: {stats['total_bytes_read']}
  写入字节: {stats['total_bytes_written']}
"""
        self.update_info_text(text)
        self.log("查看I/O状态")

    def show_process_management(self):
        """显示进程管理窗口"""
        window = tk.Toplevel(self.root)
        window.title("进程管理")
        window.geometry("750x550")
        window.transient(self.root)

        # 创建进程区
        create_frame = ttk.LabelFrame(window, text="创建进程", padding=10)
        create_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(create_frame, text="进程名称:").grid(row=0, column=0, sticky=tk.W)
        name_entry = ttk.Entry(create_frame, width=20)
        name_entry.grid(row=0, column=1, padx=5)
        name_entry.insert(0, "测试进程")

        ttk.Label(create_frame, text="优先级:").grid(row=0, column=2, padx=(10, 0))
        priority_var = tk.StringVar(value="MEDIUM")
        priority_combo = ttk.Combobox(create_frame, textvariable=priority_var,
                                      values=["HIGH", "MEDIUM", "LOW"], width=10, state='readonly')
        priority_combo.grid(row=0, column=3, padx=5)

        ttk.Label(create_frame, text="执行时间(秒):").grid(row=0, column=4, padx=(10, 0))
        duration_var = tk.StringVar(value="1.0")
        duration_spin = ttk.Spinbox(create_frame, from_=0.1, to=10.0, increment=0.1,
                                    textvariable=duration_var, width=8)
        duration_spin.grid(row=0, column=5, padx=5)

        def create_process():
            """创建新进程"""
            name = name_entry.get().strip()
            if not name:
                messagebox.showwarning("警告", "请输入进程名称")
                return

            pri_map = {"HIGH": Priority.HIGH, "MEDIUM": Priority.MEDIUM, "LOW": Priority.LOW}
            priority = pri_map.get(priority_var.get(), Priority.MEDIUM)

            try:
                duration = float(duration_var.get())
            except:
                duration = 1.0

            def task():
                time.sleep(duration)
                return f"完成: {name}"

            pcb = self.scheduler.create_process(name, task, (), priority)
            self.scheduler.submit_process(pcb)
            self.log(f"创建进程: {name}, 优先级: {priority.name}")
            refresh_list()
            self.refresh_all_views()

        ttk.Button(create_frame, text="创建进程", command=create_process).grid(row=0, column=6, padx=10)

        # 批量创建
        batch_frame = ttk.Frame(create_frame)
        batch_frame.grid(row=1, column=0, columnspan=7, pady=10)

        def create_batch_processes():
            """批量创建不同优先级的进程"""
            priorities = [
                (Priority.HIGH, "高优先级任务1", 1.5),
                (Priority.HIGH, "高优先级任务2", 1.2),
                (Priority.MEDIUM, "中优先级任务1", 2.0),
                (Priority.MEDIUM, "中优先级任务2", 1.8),
                (Priority.MEDIUM, "中优先级任务3", 1.5),
                (Priority.LOW, "低优先级任务1", 2.5),
                (Priority.LOW, "低优先级任务2", 2.0),
                (Priority.LOW, "低优先级任务3", 1.5),
            ]

            for pri, name, dur in priorities:
                def task(d=dur):
                    time.sleep(d)
                    return f"完成"

                pcb = self.scheduler.create_process(name, task, (), pri)
                self.scheduler.submit_process(pcb)

            self.log(f"批量创建了 {len(priorities)} 个进程")
            refresh_list()
            self.refresh_all_views()

        ttk.Button(batch_frame, text="批量创建测试进程", command=create_batch_processes).pack(side=tk.LEFT, padx=5)
        ttk.Label(batch_frame, text="(创建8个不同优先级的进程用于演示调度)", foreground='gray').pack(side=tk.LEFT)

        # 进程列表区
        list_frame = ttk.LabelFrame(window, text="进程列表", padding=10)
        list_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        columns = ('pid', 'name', 'priority', 'state', 'cpu_time')
        tree = ttk.Treeview(list_frame, columns=columns, show='headings', height=12)

        tree.heading('pid', text='PID')
        tree.heading('name', text='名称')
        tree.heading('priority', text='优先级')
        tree.heading('state', text='状态')
        tree.heading('cpu_time', text='CPU时间')

        tree.column('pid', width=80)
        tree.column('name', width=150)
        tree.column('priority', width=100)
        tree.column('state', width=100)
        tree.column('cpu_time', width=100)

        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=tree.yview)
        tree.configure(yscrollcommand=scrollbar.set)

        tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)

        def refresh_list():
            """刷新进程列表"""
            for item in tree.get_children():
                tree.delete(item)

            processes = self.scheduler.get_process_list()
            for p in processes:
                tree.insert('', tk.END, values=(
                    p['pid'], p['name'], p['priority'], p['state'], p['cpu_time']
                ))

        # 队列状态区
        queue_frame = ttk.LabelFrame(window, text="调度队列状态", padding=10)
        queue_frame.pack(fill=tk.X, padx=10, pady=5)

        queue_info = tk.StringVar(value="")
        queue_label = ttk.Label(queue_frame, textvariable=queue_info, font=('Consolas', 10))
        queue_label.pack(anchor=tk.W)

        def refresh_queue_info():
            """刷新队列信息"""
            status = self.scheduler.get_queue_status()
            info = f"高优先级队列: {status['high']}\n"
            info += f"中优先级队列: {status['medium']}\n"
            info += f"低优先级队列: {status['low']}\n"
            info += f"当前运行: PID {status['running'] if status['running'] else '无'}\n"
            info += f"总调度次数: {status['total_scheduled']}"
            queue_info.set(info)

        # 按钮区
        btn_frame = ttk.Frame(window)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        def refresh_all():
            refresh_list()
            refresh_queue_info()
            self.refresh_all_views()

        ttk.Button(btn_frame, text="刷新", command=refresh_all, width=12).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="关闭", command=window.destroy, width=12).pack(side=tk.RIGHT, padx=5)

        # 自动刷新
        def auto_refresh():
            if window.winfo_exists():
                refresh_list()
                refresh_queue_info()
                window.after(1000, auto_refresh)

        # 初始刷新
        refresh_list()
        refresh_queue_info()
        auto_refresh()

    def show_block_operation(self):
        """显示块操作窗口"""
        window = tk.Toplevel(self.root)
        window.title("缓冲区块操作")
        window.geometry("700x550")
        window.transient(self.root)

        # 文件选择区
        file_frame = ttk.LabelFrame(window, text="文件选择", padding=10)
        file_frame.pack(fill=tk.X, padx=10, pady=5)

        ttk.Label(file_frame, text="文件名:").grid(row=0, column=0, sticky=tk.W)
        file_entry = ttk.Entry(file_frame, width=30)
        file_entry.grid(row=0, column=1, padx=5)

        # 从当前文件名输入框获取默认值
        current_file = self.filename_entry.get().strip()
        if current_file:
            file_entry.insert(0, current_file)

        # 块信息标签
        block_info_var = tk.StringVar(value="请先加载文件信息")
        ttk.Label(file_frame, textvariable=block_info_var).grid(row=0, column=2, padx=10)

        # 块索引选择
        ttk.Label(file_frame, text="块索引:").grid(row=1, column=0, sticky=tk.W, pady=5)
        block_index_var = tk.StringVar(value="0")
        block_spin = ttk.Spinbox(file_frame, from_=0, to=100, width=10, textvariable=block_index_var)
        block_spin.grid(row=1, column=1, sticky=tk.W, pady=5)

        # 存储块列表
        block_list = []

        def load_file_info():
            """加载文件块信息"""
            nonlocal block_list
            filename = file_entry.get().strip()
            if not filename:
                messagebox.showwarning("警告", "请输入文件名")
                return

            inode = self.fs._find_inode(filename)
            if inode is None:
                messagebox.showerror("错误", f"文件 '{filename}' 不存在")
                return

            # 获取所有块
            block_list = [b for b in inode.direct_blocks if b >= 0]
            if inode.indirect_block >= 0:
                from filesystem import IndexBlock
                idx_data = self.fs.disk.read_block(inode.indirect_block)
                idx_block = IndexBlock.from_bytes(idx_data)
                block_list.extend([b for b in idx_block.indices if b >= 0])

            block_info_var.set(f"文件大小: {inode.file_size}B, 共 {len(block_list)} 块")
            block_spin.configure(to=max(0, len(block_list) - 1))
            block_index_var.set("0")
            self.log(f"加载文件 '{filename}' 信息: {len(block_list)} 块")

        ttk.Button(file_frame, text="加载文件", command=load_file_info).grid(row=1, column=2, padx=5)

        # 块内容区
        content_frame = ttk.LabelFrame(window, text="块内容 (64字节/块)", padding=10)
        content_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)

        content_text = scrolledtext.ScrolledText(content_frame, width=80, height=15, font=('Consolas', 10))
        content_text.pack(fill=tk.BOTH, expand=True)

        # 状态标签
        status_var = tk.StringVar(value="")
        status_label = ttk.Label(content_frame, textvariable=status_var, foreground='blue')
        status_label.pack(anchor=tk.W)

        # 操作按钮
        btn_frame = ttk.Frame(window)
        btn_frame.pack(fill=tk.X, padx=10, pady=10)

        def read_block():
            """读取指定块"""
            filename = file_entry.get().strip()
            if not filename:
                messagebox.showwarning("警告", "请输入文件名")
                return

            if not block_list:
                messagebox.showwarning("警告", "请先加载文件信息")
                return

            try:
                block_idx = int(block_index_var.get())
            except:
                messagebox.showerror("错误", "块索引必须是数字")
                return

            if block_idx < 0 or block_idx >= len(block_list):
                messagebox.showerror("错误", f"块索引超出范围 (0-{len(block_list) - 1})")
                return

            block_num = block_list[block_idx]

            def callback(request):
                if request.status == IOStatus.COMPLETED and request.result:
                    data = request.result

                    def update_ui():
                        content_text.delete("1.0", tk.END)
                        try:
                            text = data.rstrip(b'\x00').decode('utf-8')
                            content_text.insert(tk.END, text)
                        except:
                            content_text.insert(tk.END, data.hex())
                        status_var.set(f"读取成功: 块{block_num}, 缓冲区页={self._find_page_for_block(block_num)}")
                        self.refresh_all_views()

                    window.after(0, update_ui)
                else:
                    window.after(0, lambda: status_var.set(f"读取失败: {request.error}"))

            self.async_io.submit_read(filename, block_index=block_idx, callback=callback)
            status_var.set(f"正在读取块 {block_idx} (磁盘块 {block_num})...")
            self.log(f"读取块: 文件={filename}, 块索引={block_idx}")

        def write_block():
            """写入指定块"""
            filename = file_entry.get().strip()
            if not filename:
                messagebox.showwarning("警告", "请输入文件名")
                return

            if not block_list:
                messagebox.showwarning("警告", "请先加载文件信息")
                return

            try:
                block_idx = int(block_index_var.get())
            except:
                messagebox.showerror("错误", "块索引必须是数字")
                return

            if block_idx < 0 or block_idx >= len(block_list):
                messagebox.showerror("错误", f"块索引超出范围 (0-{len(block_list) - 1})")
                return

            block_num = block_list[block_idx]
            content = content_text.get("1.0", tk.END).rstrip('\n')
            content_bytes = content.encode('utf-8')

            if len(content_bytes) > 64:
                if not messagebox.askyesno("警告", f"内容 ({len(content_bytes)} 字节) 超过块大小 (64 字节)，将被截断。继续？"):
                    return

            def callback(request):
                if request.status == IOStatus.COMPLETED and request.result:
                    def update_ui():
                        status_var.set(f"写入成功: 块{block_num}, 已标记为脏页")
                        self.refresh_all_views()

                    window.after(0, update_ui)
                else:
                    window.after(0, lambda: status_var.set(f"写入失败: {request.error}"))

            self.async_io.submit_write(filename, block_index=block_idx, data=content_bytes, callback=callback)
            status_var.set(f"正在写入块 {block_idx} (磁盘块 {block_num})...")
            self.log(f"写入块: 文件={filename}, 块索引={block_idx}, 大小={len(content_bytes)}B")

        def flush_buffer():
            """刷新缓冲区到磁盘"""
            filename = file_entry.get().strip()
            if filename:
                self.buffer_mgr.flush_file(filename)
                status_var.set(f"已刷新文件 '{filename}' 的缓冲区到磁盘")
            else:
                self.buffer_mgr.flush_all()
                status_var.set("已刷新所有缓冲区到磁盘")
            self.refresh_all_views()
            self.log("刷新缓冲区")

        ttk.Button(btn_frame, text="读取块", command=read_block, width=12).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="写入块", command=write_block, width=12).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="刷新到磁盘", command=flush_buffer, width=12).pack(side=tk.LEFT, padx=5)
        ttk.Button(btn_frame, text="关闭", command=window.destroy, width=12).pack(side=tk.RIGHT, padx=5)

        # 提示
        ttk.Label(btn_frame, text="提示: 写入后数据在缓冲区(脏页)，需刷新才写入磁盘", foreground='gray').pack(side=tk.LEFT, padx=10)

    def _find_page_for_block(self, block_num):
        """查找块对应的缓冲页"""
        status = self.buffer_mgr.get_buffer_status()
        for s in status:
            if s['is_valid'] and s['block_num'] == block_num:
                return s['page_id']
        return -1

    def update_info_text(self, text):
        """更新信息文本框"""
        self.info_text.configure(state=tk.NORMAL)
        self.info_text.delete("1.0", tk.END)
        self.info_text.insert(tk.END, text)
        self.info_text.configure(state=tk.DISABLED)

    # ==================== 可视化更新 ====================

    def update_bitmap_view(self):
        """更新位图视图"""
        self.bitmap_fig.clear()
        ax = self.bitmap_fig.add_subplot(111)

        bitmap_data = self.fs.get_bitmap_visual()
        if not bitmap_data:
            ax.text(0.5, 0.5, '无数据', ha='center', va='center')
            self.bitmap_canvas.draw()
            return

        rows = len(bitmap_data)
        cols = len(bitmap_data[0]) if rows > 0 else 32

        # 创建颜色矩阵
        color_matrix = np.zeros((rows, cols, 3))

        for i in range(rows):
            for j in range(cols):
                block_num = i * cols + j
                if block_num < DATA_START:
                    color_matrix[i, j] = [1.0, 0.85, 0.24]  # 黄色 - 系统区
                elif bitmap_data[i][j]:
                    color_matrix[i, j] = [1.0, 0.42, 0.42]  # 红色 - 已使用
                else:
                    color_matrix[i, j] = [0.56, 0.93, 0.56]  # 绿色 - 空闲

        ax.imshow(color_matrix, aspect='equal')
        ax.set_xticks(np.arange(-0.5, cols, 1), minor=True)
        ax.set_yticks(np.arange(-0.5, rows, 1), minor=True)
        ax.grid(which='minor', color='white', linestyle='-', linewidth=0.5)
        ax.set_xlabel('块号 (列)')
        ax.set_ylabel('块号 (行)')
        ax.set_title('磁盘位图可视化 (黄=系统区, 红=已使用, 绿=空闲)')

        self.bitmap_fig.tight_layout()
        self.bitmap_canvas.draw()

    def update_buffer_view(self):
        """更新缓冲区视图"""
        self.buffer_fig.clear()
        ax = self.buffer_fig.add_subplot(111)

        status = self.buffer_mgr.get_buffer_status()
        n_pages = len(status)

        if n_pages == 0:
            ax.text(0.5, 0.5, '无缓冲区数据', ha='center', va='center')
            self.buffer_canvas.draw()
            return

        cols = 8
        rows = (n_pages + cols - 1) // cols

        for idx, page in enumerate(status):
            row = idx // cols
            col = idx % cols

            x = col * 1.5
            y = (rows - 1 - row) * 1.2

            if not page['is_valid']:
                color = '#E8E8E8'
                status_text = '空'
            elif page['is_dirty']:
                color = '#FF6B6B'
                status_text = '脏'
            else:
                color = '#6BCB77'
                status_text = '有效'

            rect = matplotlib.patches.FancyBboxPatch((x, y), 1.2, 0.9,
                                                     boxstyle="round,pad=0.02",
                                                     facecolor=color, edgecolor='black')
            ax.add_patch(rect)

            ax.text(x + 0.6, y + 0.7, f"页 {page['page_id']}", ha='center', va='center', fontsize=9, fontweight='bold')

            if page['is_valid']:
                ax.text(x + 0.6, y + 0.45, f"块:{page['block_num']}", ha='center', va='center', fontsize=8)
                filename = page['filename'][:6] + '..' if len(page['filename']) > 8 else page['filename']
                ax.text(x + 0.6, y + 0.25, filename, ha='center', va='center', fontsize=7)

            ax.text(x + 0.6, y + 0.08, status_text, ha='center', va='center', fontsize=8)

        ax.set_xlim(-0.2, cols * 1.5 + 0.2)
        ax.set_ylim(-0.2, rows * 1.2 + 0.2)
        ax.set_aspect('equal')
        ax.axis('off')
        ax.set_title('内存缓冲区状态 (灰=空闲, 绿=有效, 红=脏页)')

        self.buffer_fig.tight_layout()
        self.buffer_canvas.draw()

    def update_disk_view(self):
        """更新磁盘使用视图"""
        self.disk_fig.clear()

        disk_info = self.fs.get_disk_info()
        file_list = self.fs.list_directory()

        # 左侧饼图
        ax1 = self.disk_fig.add_subplot(121)
        used = disk_info.get('used_blocks', 0)
        free = disk_info.get('free_blocks', 0)

        sizes = [used, free]
        labels = [f'已使用\n{used}块', f'空闲\n{free}块']
        colors = ['#FF6B6B', '#90EE90']

        ax1.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)
        ax1.set_title('磁盘使用率')

        # 右侧文件列表
        ax2 = self.disk_fig.add_subplot(122)
        ax2.axis('off')

        if file_list:
            headers = ['文件名', '大小', '块数']
            cell_text = [[f['name'][:12], f"{f['size']}B", str(f['blocks'])] for f in file_list[:10]]

            table = ax2.table(cellText=cell_text, colLabels=headers, loc='center', cellLoc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.scale(1.2, 1.5)

            for i in range(len(headers)):
                table[(0, i)].set_facecolor('#6BCB77')
        else:
            ax2.text(0.5, 0.5, '无文件', ha='center', va='center')

        ax2.set_title('文件目录')

        self.disk_fig.tight_layout()
        self.disk_canvas.draw()

    def update_process_view(self):
        """更新进程调度视图"""
        self.process_fig.clear()

        processes = self.scheduler.get_process_list()
        queue_status = self.scheduler.get_queue_status()

        ax = self.process_fig.add_subplot(111)
        ax.set_xlim(0, 14)
        ax.set_ylim(0, 6)

        priorities = ['high', 'medium', 'low']
        labels = ['高优先级', '中优先级', '低优先级']
        colors = ['#E74C3C', '#F39C12', '#27AE60']

        # 绘制三个优先级队列
        for i, (pri, label, color) in enumerate(zip(priorities, labels, colors)):
            y = 5 - i

            ax.text(0.5, y + 0.3, label, ha='center', va='center', fontsize=10, fontweight='bold')

            rect = matplotlib.patches.Rectangle((1, y), 8, 0.6, fill=False, edgecolor='black')
            ax.add_patch(rect)

            pids = queue_status.get(pri, [])
            for j, pid in enumerate(pids[:8]):
                proc_rect = matplotlib.patches.Rectangle((1.2 + j * 0.9, y + 0.1), 0.7, 0.4,
                                                         facecolor=color, edgecolor='black')
                ax.add_patch(proc_rect)
                ax.text(1.55 + j * 0.9, y + 0.3, str(pid), ha='center', va='center', fontsize=8)

            if not pids:
                ax.text(5, y + 0.3, '(空)', ha='center', va='center', fontsize=9, color='gray')

        # 显示正在运行的进程
        running = queue_status.get('running')
        if running:
            ax.text(11, 4.3, f"▶ 运行中", ha='center', va='center', fontsize=10, fontweight='bold')
            ax.add_patch(matplotlib.patches.FancyBboxPatch((10, 3.6), 2, 0.6,
                                                           boxstyle="round,pad=0.05", facecolor='#45B7D1',
                                                           edgecolor='black'))
            ax.text(11, 3.9, f"PID {running}", ha='center', va='center', fontsize=10, fontweight='bold')
        else:
            ax.text(11, 4.3, "▶ 运行中", ha='center', va='center', fontsize=10, fontweight='bold')
            ax.text(11, 3.9, "(空闲)", ha='center', va='center', fontsize=9, color='gray')

        # 显示最近完成的进程（最多5个）
        completed = [p for p in processes if p['state'] == '终止']
        recent_completed = completed[-5:] if completed else []

        ax.text(11, 2.8, f"✓ 已完成 ({len(completed)})", ha='center', va='center', fontsize=10, fontweight='bold')

        if recent_completed:
            for j, proc in enumerate(recent_completed):
                y_pos = 2.2 - j * 0.4
                # 根据优先级选择颜色
                pri_colors = {'HIGH': '#E74C3C', 'MEDIUM': '#F39C12', 'LOW': '#27AE60'}
                color = pri_colors.get(proc['priority'], '#888888')
                ax.add_patch(matplotlib.patches.FancyBboxPatch((9.5, y_pos - 0.15), 3, 0.3,
                                                               boxstyle="round,pad=0.02", facecolor=color, alpha=0.5,
                                                               edgecolor='gray'))
                # 显示进程名称（截断过长的名称）
                name = proc['name'][:8] + '..' if len(proc['name']) > 10 else proc['name']
                ax.text(11, y_pos, f"{proc['pid']}: {name}", ha='center', va='center', fontsize=7)
        else:
            ax.text(11, 1.8, "(无)", ha='center', va='center', fontsize=9, color='gray')

        ax.axis('off')
        ax.set_title(f'优先级调度队列 (总调度次数: {queue_status.get("total_scheduled", 0)})')

        self.process_fig.tight_layout()
        self.process_canvas.draw()

    def refresh_all_views(self):
        """刷新所有视图"""
        self.refresh_file_list()
        self.update_bitmap_view()
        self.update_buffer_view()
        self.update_disk_view()
        self.update_process_view()

    def auto_refresh(self):
        """自动刷新"""
        self.refresh_file_list()
        self.update_process_view()  # 更频繁刷新进程视图
        # 每1秒刷新一次
        self.root.after(1000, self.auto_refresh)

    def on_closing(self):
        """关闭窗口"""
        if messagebox.askokcancel("退出", "确定要退出吗？"):
            self.log("系统关闭中...")
            self.buffer_mgr.flush_all()
            self.async_io.stop()
            self.scheduler.stop()
            self.root.destroy()


def main():
    """主函数"""
    root = tk.Tk()
    app = FileSystemGUI(root)
    root.mainloop()


if __name__ == "__main__":
    main()