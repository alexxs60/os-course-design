"""
操作系统课程设计 - 可视化模块
功能：磁盘位图可视化、缓冲区状态显示、进程调度可视化
"""

import matplotlib
matplotlib.use('Agg')  # 非交互式后端
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle, FancyBboxPatch
from matplotlib.figure import Figure
import matplotlib.gridspec as gridspec
import numpy as np
import time
from typing import Dict, List, Optional
from io import BytesIO

# ========== 修复中文显示 ==========
# 使用 Noto Sans CJK SC (思源黑体) 作为主要字体
plt.rcParams['font.sans-serif'] = ['Noto Sans CJK SC', 'WenQuanYi Zen Hei', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False  # 解决负号显示问题
plt.rcParams['font.family'] = 'sans-serif'
# ==================================

# 颜色配置
COLORS = {
    'free': '#90EE90',       # 浅绿色 - 空闲块
    'used': '#FF6B6B',       # 红色 - 已使用块
    'system': '#FFD93D',     # 黄色 - 系统保留块
    'buffer_empty': '#E8E8E8',   # 灰色 - 空缓冲页
    'buffer_valid': '#6BCB77',   # 绿色 - 有效缓冲页
    'buffer_dirty': '#FF6B6B',   # 红色 - 脏页
    'process_ready': '#4ECDC4',  # 青色 - 就绪
    'process_running': '#45B7D1', # 蓝色 - 运行
    'process_waiting': '#FFA07A', # 橙色 - 等待
    'process_terminated': '#95A5A6', # 灰色 - 终止
    'priority_high': '#E74C3C',   # 红色 - 高优先级
    'priority_medium': '#F39C12', # 橙色 - 中优先级
    'priority_low': '#27AE60',    # 绿色 - 低优先级
}

class Visualizer:
    """可视化器 - 生成各种可视化图表"""

    def __init__(self):
        self.fig = None

    def create_bitmap_figure(self, bitmap_data: List[List[bool]],
                            data_start: int = 35,
                            title: str = "磁盘位图可视化") -> Figure:
        """
        创建磁盘位图可视化图
        bitmap_data: 32x32的布尔数组，True表示已使用
        """
        fig, ax = plt.subplots(figsize=(10, 10))

        rows = len(bitmap_data)
        cols = len(bitmap_data[0]) if rows > 0 else 32

        # 创建颜色矩阵
        color_matrix = np.zeros((rows, cols, 3))

        for i in range(rows):
            for j in range(cols):
                block_num = i * cols + j
                if block_num < data_start:
                    # 系统保留区
                    color_matrix[i, j] = [1.0, 0.85, 0.24]  # 黄色
                elif bitmap_data[i][j]:
                    # 已使用
                    color_matrix[i, j] = [1.0, 0.42, 0.42]  # 红色
                else:
                    # 空闲
                    color_matrix[i, j] = [0.56, 0.93, 0.56]  # 绿色

        ax.imshow(color_matrix, aspect='equal')

        # 添加网格线
        ax.set_xticks(np.arange(-0.5, cols, 1), minor=True)
        ax.set_yticks(np.arange(-0.5, rows, 1), minor=True)
        ax.grid(which='minor', color='white', linestyle='-', linewidth=0.5)

        # 设置标签
        ax.set_xticks(np.arange(0, cols, 4))
        ax.set_yticks(np.arange(0, rows, 4))
        ax.set_xticklabels(np.arange(0, cols, 4))
        ax.set_yticklabels(np.arange(0, rows, 4))

        ax.set_xlabel('块号 (列)', fontsize=12)
        ax.set_ylabel('块号 (行)', fontsize=12)
        ax.set_title(title, fontsize=14, fontweight='bold')

        # 添加图例
        legend_elements = [
            plt.Rectangle((0, 0), 1, 1, facecolor=COLORS['system'], label='系统保留'),
            plt.Rectangle((0, 0), 1, 1, facecolor=COLORS['used'], label='已使用'),
            plt.Rectangle((0, 0), 1, 1, facecolor=COLORS['free'], label='空闲'),
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

        plt.tight_layout()
        return fig

    def create_buffer_figure(self, buffer_status: List[Dict],
                            title: str = "内存缓冲区状态") -> Figure:
        """
        创建缓冲区状态可视化图
        """
        fig, ax = plt.subplots(figsize=(14, 6))

        n_pages = len(buffer_status)
        if n_pages == 0:
            ax.text(0.5, 0.5, '无缓冲区数据', ha='center', va='center', fontsize=14)
            return fig

        # 每行显示8个页
        cols = 8
        rows = (n_pages + cols - 1) // cols

        for idx, page in enumerate(buffer_status):
            row = idx // cols
            col = idx % cols

            x = col * 1.5
            y = (rows - 1 - row) * 1.2

            # 确定颜色
            if not page['is_valid']:
                color = COLORS['buffer_empty']
                status = '空'
            elif page['is_dirty']:
                color = COLORS['buffer_dirty']
                status = '脏'
            else:
                color = COLORS['buffer_valid']
                status = '有效'

            # 绘制矩形
            rect = FancyBboxPatch((x, y), 1.2, 0.9,
                                  boxstyle="round,pad=0.02",
                                  facecolor=color, edgecolor='black', linewidth=1.5)
            ax.add_patch(rect)

            # 添加文字
            ax.text(x + 0.6, y + 0.7, f"页 {page['page_id']}",
                   ha='center', va='center', fontsize=9, fontweight='bold')

            if page['is_valid']:
                ax.text(x + 0.6, y + 0.45, f"块:{page['block_num']}",
                       ha='center', va='center', fontsize=8)
                filename = page['filename'][:8] + '..' if len(page['filename']) > 10 else page['filename']
                ax.text(x + 0.6, y + 0.25, filename,
                       ha='center', va='center', fontsize=7)

            ax.text(x + 0.6, y + 0.08, status,
                   ha='center', va='center', fontsize=8)

        ax.set_xlim(-0.2, cols * 1.5 + 0.2)
        ax.set_ylim(-0.2, rows * 1.2 + 0.2)
        ax.set_aspect('equal')
        ax.axis('off')
        ax.set_title(title, fontsize=14, fontweight='bold', pad=20)

        # 添加图例
        legend_elements = [
            plt.Rectangle((0, 0), 1, 1, facecolor=COLORS['buffer_empty'], label='空闲'),
            plt.Rectangle((0, 0), 1, 1, facecolor=COLORS['buffer_valid'], label='有效'),
            plt.Rectangle((0, 0), 1, 1, facecolor=COLORS['buffer_dirty'], label='脏页'),
        ]
        ax.legend(handles=legend_elements, loc='upper right', fontsize=10)

        plt.tight_layout()
        return fig

    def create_process_figure(self, process_list: List[Dict],
                             queue_status: Dict,
                             title: str = "进程调度状态") -> Figure:
        """
        创建进程调度可视化图
        """
        fig = plt.figure(figsize=(14, 8))
        gs = gridspec.GridSpec(2, 2, height_ratios=[1, 1.5])

        # 1. 优先级队列可视化
        ax1 = fig.add_subplot(gs[0, :])
        self._draw_priority_queues(ax1, queue_status)

        # 2. 进程状态表
        ax2 = fig.add_subplot(gs[1, 0])
        self._draw_process_table(ax2, process_list)

        # 3. 进程状态统计
        ax3 = fig.add_subplot(gs[1, 1])
        self._draw_process_stats(ax3, process_list)

        fig.suptitle(title, fontsize=14, fontweight='bold')
        plt.tight_layout()
        return fig

    def _draw_priority_queues(self, ax, queue_status: Dict):
        """绘制优先级队列"""
        ax.set_xlim(0, 12)
        ax.set_ylim(0, 4)

        priorities = ['high', 'medium', 'low']
        labels = ['高优先级', '中优先级', '低优先级']
        colors = [COLORS['priority_high'], COLORS['priority_medium'], COLORS['priority_low']]

        for i, (pri, label, color) in enumerate(zip(priorities, labels, colors)):
            y = 3 - i

            # 队列标签
            ax.text(0.5, y + 0.3, label, ha='center', va='center',
                   fontsize=10, fontweight='bold')

            # 队列框
            rect = plt.Rectangle((1, y), 10, 0.6, fill=False,
                                edgecolor='black', linewidth=1)
            ax.add_patch(rect)

            # 队列中的进程
            pids = queue_status.get(pri, [])
            for j, pid in enumerate(pids[:10]):  # 最多显示10个
                proc_rect = plt.Rectangle((1.2 + j * 0.9, y + 0.1), 0.7, 0.4,
                                         facecolor=color, edgecolor='black')
                ax.add_patch(proc_rect)
                ax.text(1.55 + j * 0.9, y + 0.3, str(pid),
                       ha='center', va='center', fontsize=8)

        # 当前运行进程
        running = queue_status.get('running')
        if running:
            ax.text(6, 0.3, f"运行中: PID {running}", ha='center', va='center',
                   fontsize=11, fontweight='bold', color='#E74C3C')

        ax.axis('off')
        ax.set_title('多级反馈优先队列', fontsize=12)

    def _draw_process_table(self, ax, process_list: List[Dict]):
        """绘制进程状态表格"""
        ax.axis('off')

        if not process_list:
            ax.text(0.5, 0.5, '无进程', ha='center', va='center', fontsize=12)
            return

        headers = ['PID', '名称', '优先级', '状态', 'CPU时间']
        cell_text = []

        for p in process_list[:6]:  # 最多显示6个进程
            cell_text.append([
                str(p['pid']),
                p['name'][:10],
                p['priority'],
                p['state'],
                p['cpu_time']
            ])

        if cell_text:
            table = ax.table(cellText=cell_text, colLabels=headers,
                            loc='center', cellLoc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.scale(1.2, 1.5)

            # 设置表头样式
            for i in range(len(headers)):
                table[(0, i)].set_facecolor('#4ECDC4')
                table[(0, i)].set_text_props(fontweight='bold')

        ax.set_title('进程列表', fontsize=12)

    def _draw_process_stats(self, ax, process_list: List[Dict]):
        """绘制进程状态统计饼图"""
        if not process_list:
            ax.text(0.5, 0.5, '无数据', ha='center', va='center', fontsize=12)
            ax.axis('off')
            return

        # 统计各状态数量
        states = {}
        for p in process_list:
            state = p['state']
            states[state] = states.get(state, 0) + 1

        if not states:
            ax.text(0.5, 0.5, '无数据', ha='center', va='center', fontsize=12)
            ax.axis('off')
            return

        labels = list(states.keys())
        sizes = list(states.values())
        colors_list = ['#4ECDC4', '#45B7D1', '#FFA07A', '#95A5A6', '#FF6B6B']

        ax.pie(sizes, labels=labels, colors=colors_list[:len(labels)],
              autopct='%1.1f%%', startangle=90)
        ax.set_title('状态分布', fontsize=12)

    def create_disk_info_figure(self, disk_info: Dict,
                               file_list: List[Dict],
                               title: str = "磁盘与文件信息") -> Figure:
        """创建磁盘信息和文件列表可视化"""
        fig = plt.figure(figsize=(14, 6))
        gs = gridspec.GridSpec(1, 2, width_ratios=[1, 1.5])

        # 1. 磁盘使用率
        ax1 = fig.add_subplot(gs[0, 0])
        self._draw_disk_usage(ax1, disk_info)

        # 2. 文件列表
        ax2 = fig.add_subplot(gs[0, 1])
        self._draw_file_list(ax2, file_list)

        fig.suptitle(title, fontsize=14, fontweight='bold')
        plt.tight_layout()
        return fig

    def _draw_disk_usage(self, ax, disk_info: Dict):
        """绘制磁盘使用率饼图"""
        used = disk_info.get('used_blocks', 0)
        free = disk_info.get('free_blocks', 0)

        sizes = [used, free]
        labels = [f'已使用\n{used}块', f'空闲\n{free}块']
        colors = [COLORS['used'], COLORS['free']]
        explode = (0.05, 0)

        ax.pie(sizes, explode=explode, labels=labels, colors=colors,
              autopct='%1.1f%%', startangle=90, textprops={'fontsize': 10})

        # 添加中心文字
        total = disk_info.get('total_blocks', 1024)
        ax.text(0, 0, f'总计\n{total}块', ha='center', va='center',
               fontsize=12, fontweight='bold')

        ax.set_title('磁盘使用率', fontsize=12)

    def _draw_file_list(self, ax, file_list: List[Dict]):
        """绘制文件列表表格"""
        ax.axis('off')

        if not file_list:
            ax.text(0.5, 0.5, '无文件', ha='center', va='center', fontsize=12)
            return

        headers = ['文件名', '大小', '块数', '权限', '创建时间']
        cell_text = []

        for f in file_list[:8]:  # 最多显示8个文件
            cell_text.append([
                f['name'][:15],
                f"{f['size']}B",
                str(f['blocks']),
                f['permission'],
                f['create_time'][-8:] if f['create_time'] != 'N/A' else 'N/A'
            ])

        if cell_text:
            table = ax.table(cellText=cell_text, colLabels=headers,
                            loc='center', cellLoc='center')
            table.auto_set_font_size(False)
            table.set_fontsize(9)
            table.scale(1.2, 1.5)

            for i in range(len(headers)):
                table[(0, i)].set_facecolor('#6BCB77')
                table[(0, i)].set_text_props(fontweight='bold')

        ax.set_title('文件目录', fontsize=12)

    def create_buffer_stats_figure(self, stats: Dict,
                                   title: str = "缓冲区统计") -> Figure:
        """创建缓冲区统计图"""
        fig, axes = plt.subplots(1, 2, figsize=(12, 5))

        # 1. 命中率条形图
        ax1 = axes[0]
        categories = ['命中', '未命中']
        values = [stats.get('hit_count', 0), stats.get('miss_count', 0)]
        colors = ['#6BCB77', '#FF6B6B']

        bars = ax1.bar(categories, values, color=colors, edgecolor='black')
        ax1.set_ylabel('次数')
        ax1.set_title('缓冲命中统计')

        # 添加数值标签
        for bar, val in zip(bars, values):
            ax1.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    str(val), ha='center', va='bottom', fontsize=10)

        # 添加命中率文字
        hit_rate = stats.get('hit_rate', '0%')
        ax1.text(0.5, 0.95, f'命中率: {hit_rate}', transform=ax1.transAxes,
                ha='center', fontsize=12, fontweight='bold')

        # 2. 页面状态饼图
        ax2 = axes[1]
        valid = stats.get('valid_pages', 0)
        dirty = stats.get('dirty_pages', 0)
        empty = 16 - valid  # 假设总共16页

        sizes = [empty, valid - dirty, dirty]
        labels = ['空闲', '有效', '脏页']
        colors = [COLORS['buffer_empty'], COLORS['buffer_valid'], COLORS['buffer_dirty']]

        # 过滤掉0值
        non_zero = [(s, l, c) for s, l, c in zip(sizes, labels, colors) if s > 0]
        if non_zero:
            sizes, labels, colors = zip(*non_zero)
            ax2.pie(sizes, labels=labels, colors=colors, autopct='%1.1f%%', startangle=90)

        ax2.set_title('缓冲页状态')

        fig.suptitle(title, fontsize=14, fontweight='bold')
        plt.tight_layout()
        return fig

    def save_figure(self, fig: Figure, filename: str, dpi: int = 150):
        """保存图表到文件"""
        fig.savefig(filename, dpi=dpi, bbox_inches='tight',
                   facecolor='white', edgecolor='none')
        plt.close(fig)

    def figure_to_bytes(self, fig: Figure, dpi: int = 150) -> bytes:
        """将图表转换为字节数据"""
        buf = BytesIO()
        fig.savefig(buf, format='png', dpi=dpi, bbox_inches='tight',
                   facecolor='white', edgecolor='none')
        buf.seek(0)
        data = buf.getvalue()
        plt.close(fig)
        return data


# ==================== 测试代码 ====================
if __name__ == "__main__":
    viz = Visualizer()

    # 测试位图可视化
    print("生成位图可视化...")
    bitmap_data = []
    for i in range(32):
        row = []
        for j in range(32):
            block_num = i * 32 + j
            if block_num < 35:  # 系统区
                row.append(True)
            elif block_num < 100:  # 部分已使用
                row.append(True)
            else:
                row.append(False)
        bitmap_data.append(row)

    fig = viz.create_bitmap_figure(bitmap_data)
    viz.save_figure(fig, 'test_bitmap.png')
    print("  保存到 test_bitmap.png")

    # 测试缓冲区可视化
    print("生成缓冲区可视化...")
    buffer_status = [
        {'page_id': i, 'block_num': 35 + i if i < 8 else -1,
         'filename': f'test{i}.txt' if i < 8 else '',
         'is_valid': i < 8, 'is_dirty': i % 3 == 0 and i < 8,
         'owner_pid': 1000 + i, 'access_count': i * 10}
        for i in range(16)
    ]

    fig = viz.create_buffer_figure(buffer_status)
    viz.save_figure(fig, 'test_buffer.png')
    print("  保存到 test_buffer.png")

    # 测试进程可视化
    print("生成进程调度可视化...")
    process_list = [
        {'pid': 1001, 'name': '计算任务', 'priority': 'HIGH', 'state': '运行', 'cpu_time': '0.5s'},
        {'pid': 1002, 'name': 'IO任务', 'priority': 'MEDIUM', 'state': '就绪', 'cpu_time': '0.2s'},
        {'pid': 1003, 'name': '后台任务', 'priority': 'LOW', 'state': '等待', 'cpu_time': '0.1s'},
    ]
    queue_status = {
        'high': [1004, 1005],
        'medium': [1006, 1007, 1008],
        'low': [1009],
        'running': 1001
    }

    fig = viz.create_process_figure(process_list, queue_status)
    viz.save_figure(fig, 'test_process.png')
    print("  保存到 test_process.png")

    print("可视化测试完成!")