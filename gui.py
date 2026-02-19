#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
操作系统课程设计 - 图形用户界面
增强版 - 支持进程调度可视化
"""

import tkinter as tk
from tkinter import ttk, messagebox, scrolledtext
import threading
import time
import os
import sys

# 添加当前目录到路径
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from os_simulator import OSSimulator, BLOCK_SIZE, TOTAL_BLOCKS, BUFFER_PAGES


class OSGUI:
    """操作系统模拟器图形界面 - 增强版"""
    
    def __init__(self):
        self.os_sim = OSSimulator()
        self.os_sim.start()
        
        self.root = tk.Tk()
        self.root.title("操作系统模拟器 - 进程调度与文件系统")
        self.root.geometry("1500x950")
        self.root.configure(bg='#f0f0f0')
        
        # 样式配置
        self.style = ttk.Style()
        self.style.configure('Title.TLabel', font=('Arial', 14, 'bold'))
        self.style.configure('Info.TLabel', font=('Arial', 10))
        self.style.configure('Small.TLabel', font=('Arial', 8))
        
        # 颜色配置
        self.colors = {
            'READY': '#2ECC71',
            'RUNNING': '#3498DB',
            'BLOCKED': '#E74C3C',
            'SLEEPING': '#9B59B6',
            'TERMINATED': '#95A5A6',
            'SYSTEM': '#E74C3C',
            'DAEMON': '#F39C12',
            'USER': '#3498DB',
            'COMMAND': '#95A5A6'
        }
        
        self._create_widgets()
        self._start_refresh_thread()
    
    def _create_widgets(self):
        """创建界面组件"""
        # 主框架
        main_frame = ttk.Frame(self.root, padding="10")
        main_frame.pack(fill=tk.BOTH, expand=True)
        
        # 顶部标题栏
        self._create_title_bar(main_frame)
        
        # 内容区域
        content_frame = ttk.Frame(main_frame)
        content_frame.pack(fill=tk.BOTH, expand=True, pady=(10, 0))
        
        # 左侧面板 - 命令和进程管理
        left_frame = ttk.Frame(content_frame)
        left_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=False, padx=(0, 10))
        
        self._create_command_panel(left_frame)
        self._create_process_control_panel(left_frame)
        
        # 中间面板 - 进程状态
        middle_frame = ttk.Frame(content_frame)
        middle_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 10))
        
        self._create_process_panel(middle_frame)
        self._create_schedule_log_panel(middle_frame)
        
        # 右侧面板 - 磁盘和缓冲区
        right_frame = ttk.Frame(content_frame)
        right_frame.pack(side=tk.RIGHT, fill=tk.BOTH, expand=True)
        
        self._create_disk_buffer_panel(right_frame)
        self._create_fat_directory_panel(right_frame)
    
    def _create_title_bar(self, parent):
        """创建标题栏"""
        title_frame = ttk.Frame(parent)
        title_frame.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(title_frame, text="操作系统模拟器 v2.0", 
                  style='Title.TLabel').pack(side=tk.LEFT)
        
        algo_info = "FAT文件系统 | LRU缓冲管理 | 时间片轮转调度 | 常驻进程 | 非阻塞I/O"
        ttk.Label(title_frame, text=algo_info, 
                  style='Info.TLabel').pack(side=tk.RIGHT)
    
    def _create_command_panel(self, parent):
        """创建命令面板"""
        frame = ttk.LabelFrame(parent, text="命令控制台", padding="5")
        frame.pack(fill=tk.X, pady=(0, 10))
        
        # 文件操作区
        ops_frame = ttk.LabelFrame(frame, text="文件操作", padding="5")
        ops_frame.pack(fill=tk.X, pady=(0, 5))
        
        # 文件名输入
        ttk.Label(ops_frame, text="文件名:").grid(row=0, column=0, sticky=tk.W)
        self.filename_var = tk.StringVar()
        ttk.Entry(ops_frame, textvariable=self.filename_var, width=20).grid(
            row=0, column=1, columnspan=2, padx=5, pady=2)
        
        # 内容输入
        ttk.Label(ops_frame, text="内容:").grid(row=1, column=0, sticky=tk.W)
        self.content_text = scrolledtext.ScrolledText(ops_frame, height=3, width=20)
        self.content_text.grid(row=1, column=1, columnspan=2, padx=5, pady=2)
        
        # 块号输入
        ttk.Label(ops_frame, text="块号:").grid(row=2, column=0, sticky=tk.W)
        self.block_var = tk.StringVar(value="0")
        ttk.Entry(ops_frame, textvariable=self.block_var, width=8).grid(
            row=2, column=1, sticky=tk.W, padx=5, pady=2)
        
        # 操作按钮
        btn_frame = ttk.Frame(ops_frame)
        btn_frame.grid(row=3, column=0, columnspan=3, pady=5)
        
        ttk.Button(btn_frame, text="创建", width=6, 
                   command=self._create_file).pack(side=tk.LEFT, padx=1)
        ttk.Button(btn_frame, text="删除", width=6,
                   command=self._delete_file).pack(side=tk.LEFT, padx=1)
        ttk.Button(btn_frame, text="读取", width=6,
                   command=self._read_file).pack(side=tk.LEFT, padx=1)
        ttk.Button(btn_frame, text="修改", width=6,
                   command=self._modify_file).pack(side=tk.LEFT, padx=1)
        
        # 命令行区
        cmd_frame = ttk.LabelFrame(frame, text="命令行", padding="5")
        cmd_frame.pack(fill=tk.X)
        
        self.cmd_var = tk.StringVar()
        cmd_entry = ttk.Entry(cmd_frame, textvariable=self.cmd_var, width=28)
        cmd_entry.pack(fill=tk.X, pady=(0, 3))
        cmd_entry.bind('<Return>', lambda e: self._execute_command())
        
        ttk.Button(cmd_frame, text="执行命令", command=self._execute_command).pack(fill=tk.X)
        
        ttk.Label(cmd_frame, text="命令: ps/spawn/kill/log/sched/status/help",
                  style='Small.TLabel').pack(pady=3)
        
        # 输出区
        self.output_text = scrolledtext.ScrolledText(frame, height=10, width=32)
        self.output_text.pack(fill=tk.BOTH, expand=True, pady=5)
    
    def _create_process_control_panel(self, parent):
        """创建进程控制面板"""
        frame = ttk.LabelFrame(parent, text="进程管理", padding="5")
        frame.pack(fill=tk.X)
        
        # 进程名输入
        input_frame = ttk.Frame(frame)
        input_frame.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(input_frame, text="进程名:").pack(side=tk.LEFT)
        self.proc_name_var = tk.StringVar()
        ttk.Entry(input_frame, textvariable=self.proc_name_var, width=12).pack(side=tk.LEFT, padx=5)
        
        # 按钮
        btn_frame = ttk.Frame(frame)
        btn_frame.pack(fill=tk.X)
        
        ttk.Button(btn_frame, text="创建进程", 
                   command=self._spawn_process).pack(side=tk.LEFT, padx=2)
        
        ttk.Label(btn_frame, text="PID:").pack(side=tk.LEFT, padx=(10, 2))
        self.kill_pid_var = tk.StringVar()
        ttk.Entry(btn_frame, textvariable=self.kill_pid_var, width=5).pack(side=tk.LEFT)
        ttk.Button(btn_frame, text="终止", 
                   command=self._kill_process).pack(side=tk.LEFT, padx=2)
    
    def _create_process_panel(self, parent):
        """创建进程状态面板"""
        frame = ttk.LabelFrame(parent, text="进程状态可视化", padding="5")
        frame.pack(fill=tk.BOTH, expand=True, pady=(0, 10))
        
        # 进程状态画布
        canvas_frame = ttk.Frame(frame)
        canvas_frame.pack(fill=tk.X, pady=(0, 5))
        
        self.proc_canvas = tk.Canvas(canvas_frame, width=500, height=150, bg='white')
        self.proc_canvas.pack(fill=tk.X)
        
        # 图例
        legend_frame = ttk.Frame(frame)
        legend_frame.pack(fill=tk.X, pady=(0, 5))
        
        ttk.Label(legend_frame, text="状态:", style='Small.TLabel').pack(side=tk.LEFT)
        for state, color in [('RUNNING', '#3498DB'), ('READY', '#2ECC71'), 
                             ('BLOCKED', '#E74C3C'), ('SLEEPING', '#9B59B6')]:
            canvas = tk.Canvas(legend_frame, width=12, height=12, bg=color, 
                              highlightthickness=1, highlightbackground='black')
            canvas.pack(side=tk.LEFT, padx=2)
            ttk.Label(legend_frame, text=state, style='Small.TLabel').pack(side=tk.LEFT, padx=(0, 8))
        
        ttk.Label(legend_frame, text=" | 类型:", style='Small.TLabel').pack(side=tk.LEFT)
        for ptype, color in [('SYSTEM', '#E74C3C'), ('DAEMON', '#F39C12'), ('USER', '#3498DB')]:
            canvas = tk.Canvas(legend_frame, width=12, height=12, bg='white', 
                              highlightthickness=2, highlightbackground=color)
            canvas.pack(side=tk.LEFT, padx=2)
            ttk.Label(legend_frame, text=ptype, style='Small.TLabel').pack(side=tk.LEFT, padx=(0, 8))
        
        # 进程列表
        list_frame = ttk.Frame(frame)
        list_frame.pack(fill=tk.BOTH, expand=True)
        
        columns = ('pid', 'name', 'state', 'type', 'cpu', 'runs')
        self.proc_tree = ttk.Treeview(list_frame, columns=columns, height=8, show='headings')
        
        self.proc_tree.heading('pid', text='PID')
        self.proc_tree.heading('name', text='名称')
        self.proc_tree.heading('state', text='状态')
        self.proc_tree.heading('type', text='类型')
        self.proc_tree.heading('cpu', text='CPU时间')
        self.proc_tree.heading('runs', text='运行次数')
        
        self.proc_tree.column('pid', width=50)
        self.proc_tree.column('name', width=100)
        self.proc_tree.column('state', width=80)
        self.proc_tree.column('type', width=80)
        self.proc_tree.column('cpu', width=80)
        self.proc_tree.column('runs', width=80)
        
        scrollbar = ttk.Scrollbar(list_frame, orient=tk.VERTICAL, command=self.proc_tree.yview)
        self.proc_tree.configure(yscrollcommand=scrollbar.set)
        
        self.proc_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        
        # 调度信息
        info_frame = ttk.Frame(frame)
        info_frame.pack(fill=tk.X, pady=(5, 0))
        
        self.sched_labels = {}
        for key, text in [('current', '当前进程:'), ('ready', '就绪数:'), 
                          ('switches', '切换次数:')]:
            ttk.Label(info_frame, text=text).pack(side=tk.LEFT, padx=(0, 2))
            self.sched_labels[key] = ttk.Label(info_frame, text="-", width=10)
            self.sched_labels[key].pack(side=tk.LEFT, padx=(0, 15))
    
    def _create_schedule_log_panel(self, parent):
        """创建调度日志面板"""
        frame = ttk.LabelFrame(parent, text="调度日志 (最近15条)", padding="5")
        frame.pack(fill=tk.BOTH, expand=True)
        
        self.sched_log_text = scrolledtext.ScrolledText(frame, height=8, width=50,
                                                         font=('Consolas', 9))
        self.sched_log_text.pack(fill=tk.BOTH, expand=True)
    
    def _create_disk_buffer_panel(self, parent):
        """创建磁盘和缓冲区面板"""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.X, pady=(0, 10))
        
        # 磁盘状态
        disk_frame = ttk.LabelFrame(frame, text="磁盘状态", padding="5")
        disk_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=(0, 5))
        
        self.disk_labels = {}
        for key, text in [('total', '总块数:'), ('used', '已用块:'), 
                          ('free', '空闲块:'), ('usage', '使用率:')]:
            row_frame = ttk.Frame(disk_frame)
            row_frame.pack(fill=tk.X, pady=1)
            ttk.Label(row_frame, text=text, width=8).pack(side=tk.LEFT)
            self.disk_labels[key] = ttk.Label(row_frame, text="-", width=10)
            self.disk_labels[key].pack(side=tk.LEFT)
        
        self.disk_progress = ttk.Progressbar(disk_frame, length=120, mode='determinate')
        self.disk_progress.pack(pady=5)
        
        # 缓冲区状态
        buf_frame = ttk.LabelFrame(frame, text="缓冲区 (LRU)", padding="5")
        buf_frame.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        self.buffer_canvas = tk.Canvas(buf_frame, width=200, height=80, bg='white')
        self.buffer_canvas.pack(pady=2)
        
        self.buffer_labels = {}
        info_frame = ttk.Frame(buf_frame)
        info_frame.pack(fill=tk.X)
        
        for key, text in [('used', '已用:'), ('dirty', '脏页:'), ('hit', '命中率:')]:
            ttk.Label(info_frame, text=text, style='Small.TLabel').pack(side=tk.LEFT)
            self.buffer_labels[key] = ttk.Label(info_frame, text="-", 
                                                 style='Small.TLabel', width=6)
            self.buffer_labels[key].pack(side=tk.LEFT, padx=(0, 5))
    
    def _create_fat_directory_panel(self, parent):
        """创建FAT表和目录面板"""
        frame = ttk.Frame(parent)
        frame.pack(fill=tk.BOTH, expand=True)
        
        # FAT表
        fat_frame = ttk.LabelFrame(frame, text="FAT表 (前64块)", padding="5")
        fat_frame.pack(fill=tk.X, pady=(0, 10))
        
        self.fat_canvas = tk.Canvas(fat_frame, width=350, height=160, bg='white')
        self.fat_canvas.pack()
        
        # 目录
        dir_frame = ttk.LabelFrame(frame, text="文件目录", padding="5")
        dir_frame.pack(fill=tk.BOTH, expand=True)
        
        columns = ('size', 'blocks', 'time')
        self.dir_tree = ttk.Treeview(dir_frame, columns=columns, height=6, show='tree headings')
        
        self.dir_tree.heading('#0', text='文件名')
        self.dir_tree.heading('size', text='大小')
        self.dir_tree.heading('blocks', text='块数')
        self.dir_tree.heading('time', text='创建时间')
        
        self.dir_tree.column('#0', width=100)
        self.dir_tree.column('size', width=60)
        self.dir_tree.column('blocks', width=50)
        self.dir_tree.column('time', width=100)
        
        self.dir_tree.pack(fill=tk.BOTH, expand=True)
        self.dir_tree.bind('<Double-1>', self._on_file_double_click)
    
    def _update_display(self):
        """更新显示"""
        try:
            status = self.os_sim.get_status()
            
            # 更新磁盘状态
            disk = status['disk']
            self.disk_labels['total'].config(text=str(disk['total_blocks']))
            self.disk_labels['used'].config(text=str(disk['used_blocks']))
            self.disk_labels['free'].config(text=str(disk['free_blocks']))
            self.disk_labels['usage'].config(text=f"{disk['usage_percent']:.1f}%")
            self.disk_progress['value'] = disk['usage_percent']
            
            # 更新缓冲区
            buf = status['buffer']
            self._draw_buffer(buf['pages'])
            self.buffer_labels['used'].config(text=str(buf['used_pages']))
            self.buffer_labels['dirty'].config(text=str(buf['dirty_pages']))
            self.buffer_labels['hit'].config(text=f"{buf['hit_rate']:.1f}%")
            
            # 更新FAT表
            self._draw_fat(status['fat_table'])
            
            # 更新目录
            self._update_directory(status['files'])
            
            # 更新调度器
            sched = status['scheduler']
            self._draw_processes(sched['processes'], sched.get('current_pid', 0))
            self._update_process_tree(sched['processes'])
            
            self.sched_labels['current'].config(text=sched['current'])
            self.sched_labels['ready'].config(text=str(sched['ready_count']))
            self.sched_labels['switches'].config(text=str(sched['context_switches']))
            
            # 更新调度日志
            self._update_schedule_log(sched.get('schedule_log', []))
            
        except Exception as e:
            print(f"Update error: {e}")
    
    def _draw_buffer(self, pages):
        """绘制缓冲区"""
        self.buffer_canvas.delete('all')
        
        cols = 8
        cell_w = 24
        cell_h = 35
        
        for i, page in enumerate(pages):
            row = i // cols
            col = i % cols
            x = col * (cell_w + 2) + 3
            y = row * (cell_h + 2) + 3
            
            if page['block_id'] < 0:
                color = '#E8E8E8'
            elif page['is_dirty']:
                color = '#FF6B6B'
            else:
                color = '#4ECDC4'
            
            self.buffer_canvas.create_rectangle(x, y, x + cell_w, y + cell_h, 
                                                 fill=color, outline='black')
            self.buffer_canvas.create_text(x + cell_w//2, y + 10, 
                                            text=f"P{page['page_id']}", font=('Arial', 7))
            if page['block_id'] >= 0:
                self.buffer_canvas.create_text(x + cell_w//2, y + 24, 
                                                text=f"B{page['block_id']}", font=('Arial', 6))
    
    def _draw_fat(self, fat_table):
        """绘制FAT表"""
        self.fat_canvas.delete('all')
        
        cols = 16
        cell_w = 20
        cell_h = 38
        
        colors = {
            'free': '#90EE90',
            'used': '#87CEEB',
            'reserved': '#FFB6C1',
            'eof': '#FFD700',
        }
        
        for i in range(min(64, len(fat_table))):
            row = i // cols
            col = i % cols
            x = col * (cell_w + 1) + 5
            y = row * (cell_h + 1) + 5
            
            val = fat_table[i]
            if val == 0:
                color = colors['free']
                text = ''
            elif val == -1:
                color = colors['eof']
                text = 'E'
            elif val < 0:
                color = colors['reserved']
                text = 'R'
            else:
                color = colors['used']
                text = str(val)
            
            self.fat_canvas.create_rectangle(x, y, x + cell_w, y + cell_h,
                                              fill=color, outline='black')
            self.fat_canvas.create_text(x + cell_w//2, y + 10,
                                         text=str(i), font=('Arial', 6))
            if text:
                self.fat_canvas.create_text(x + cell_w//2, y + 26,
                                             text=text, font=('Arial', 7, 'bold'))
    
    def _draw_processes(self, processes, current_pid):
        """绘制进程状态"""
        self.proc_canvas.delete('all')
        
        # 过滤掉已终止的进程
        active_procs = [p for p in processes if p['state'] != 'TERMINATED']
        
        x = 10
        cell_w = 75
        cell_h = 55
        
        for i, proc in enumerate(active_procs[:12]):  # 最多显示12个
            # 根据进程类型决定边框颜色
            border_color = self.colors.get(proc['type'], '#666666')
            fill_color = self.colors.get(proc['state'], '#CCCCCC')
            
            # 当前运行进程边框加粗
            border_width = 3 if proc['pid'] == current_pid else 1
            
            # 绘制矩形
            self.proc_canvas.create_rectangle(x, 10, x + cell_w, 10 + cell_h,
                                               fill=fill_color, outline=border_color,
                                               width=border_width)
            
            # 进程名和PID
            self.proc_canvas.create_text(x + cell_w//2, 25,
                                          text=proc['name'][:8], font=('Arial', 9, 'bold'))
            self.proc_canvas.create_text(x + cell_w//2, 40,
                                          text=f"PID:{proc['pid']}", font=('Arial', 8))
            self.proc_canvas.create_text(x + cell_w//2, 52,
                                          text=f"R:{proc['run_count']}", font=('Arial', 7))
            
            x += cell_w + 5
            
            if x > 480:
                break
        
        # 显示时间片信息
        self.proc_canvas.create_text(250, 80,
                                      text=f"活动进程: {len(active_procs)} | 时间片: 100ms",
                                      font=('Arial', 9))
    
    def _update_process_tree(self, processes):
        """更新进程列表"""
        # 清空现有项
        for item in self.proc_tree.get_children():
            self.proc_tree.delete(item)
        
        for proc in sorted(processes, key=lambda x: x['pid']):
            if proc['state'] != 'TERMINATED':
                self.proc_tree.insert('', 'end', values=(
                    proc['pid'],
                    proc['name'],
                    proc['state'],
                    proc['type'],
                    f"{proc['cpu_time']:.2f}s",
                    proc['run_count']
                ))
    
    def _update_schedule_log(self, logs):
        """更新调度日志"""
        self.sched_log_text.delete('1.0', tk.END)
        for log in logs[-15:]:
            self.sched_log_text.insert(tk.END, log + "\n")
        self.sched_log_text.see(tk.END)
    
    def _update_directory(self, files):
        """更新目录显示"""
        for item in self.dir_tree.get_children():
            self.dir_tree.delete(item)
        
        for f in files:
            self.dir_tree.insert('', 'end', text=f['filename'],
                                values=(f'{f["size"]}B', f['blocks'], f['create_time']))
    
    def _on_file_double_click(self, event):
        """双击文件项"""
        item = self.dir_tree.selection()
        if item:
            filename = self.dir_tree.item(item[0])['text']
            self.filename_var.set(filename)
            self._read_file()
    
    def _create_file(self):
        """创建文件"""
        filename = self.filename_var.get().strip()
        content = self.content_text.get('1.0', tk.END).strip()
        
        if not filename:
            messagebox.showerror("错误", "请输入文件名")
            return
        
        result = self.os_sim.execute_command('create', filename, content)
        self._log_output(f"> create {filename}\n{result}")
    
    def _delete_file(self):
        """删除文件"""
        filename = self.filename_var.get().strip()
        
        if not filename:
            messagebox.showerror("错误", "请输入文件名")
            return
        
        if messagebox.askyesno("确认", f"确定删除文件 '{filename}'?"):
            result = self.os_sim.execute_command('delete', filename)
            self._log_output(f"> delete {filename}\n{result}")
    
    def _read_file(self):
        """读取文件"""
        filename = self.filename_var.get().strip()
        block_idx = self.block_var.get().strip()
        
        if not filename:
            messagebox.showerror("错误", "请输入文件名")
            return
        
        if block_idx:
            result = self.os_sim.execute_command('read', filename, block_idx)
        else:
            result = self.os_sim.execute_command('read', filename)
        
        self._log_output(f"> read {filename}\n{result}")
        
        if result and not result.startswith("文件") and not result.startswith("命令"):
            self.content_text.delete('1.0', tk.END)
            self.content_text.insert('1.0', result)
    
    def _modify_file(self):
        """修改文件"""
        filename = self.filename_var.get().strip()
        block_idx = self.block_var.get().strip()
        content = self.content_text.get('1.0', tk.END).strip()
        
        if not filename:
            messagebox.showerror("错误", "请输入文件名")
            return
        
        if not block_idx:
            block_idx = "0"
        
        result = self.os_sim.execute_command('modify', filename, block_idx, content)
        self._log_output(f"> modify {filename}\n{result}")
    
    def _spawn_process(self):
        """创建用户进程"""
        name = self.proc_name_var.get().strip()
        if not name:
            name = f"worker_{int(time.time()) % 1000}"
        
        result = self.os_sim.execute_command('spawn', name)
        self._log_output(f"> spawn {name}\n{result}")
        self.proc_name_var.set("")
    
    def _kill_process(self):
        """终止进程"""
        pid = self.kill_pid_var.get().strip()
        if not pid:
            messagebox.showerror("错误", "请输入PID")
            return
        
        result = self.os_sim.execute_command('kill', pid)
        self._log_output(f"> kill {pid}\n{result}")
        self.kill_pid_var.set("")
    
    def _execute_command(self):
        """执行命令"""
        cmd_line = self.cmd_var.get().strip()
        if not cmd_line:
            return
        
        parts = cmd_line.split(maxsplit=3)
        command = parts[0]
        args = parts[1:] if len(parts) > 1 else []
        
        result = self.os_sim.execute_command(command, *args)
        self._log_output(f"> {cmd_line}\n{result}")
        self.cmd_var.set("")
    
    def _log_output(self, text):
        """记录输出"""
        self.output_text.insert(tk.END, text + "\n\n")
        self.output_text.see(tk.END)
    
    def _start_refresh_thread(self):
        """启动刷新线程"""
        def refresh():
            while True:
                try:
                    self.root.after(0, self._update_display)
                except:
                    break
                time.sleep(0.3)  # 更快刷新以观察调度
        
        thread = threading.Thread(target=refresh, daemon=True)
        thread.start()
    
    def run(self):
        """运行GUI"""
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.mainloop()
    
    def _on_close(self):
        """关闭窗口"""
        self.os_sim.stop()
        self.root.destroy()


def main():
    """主函数"""
    gui = OSGUI()
    gui.run()


if __name__ == '__main__':
    main()
