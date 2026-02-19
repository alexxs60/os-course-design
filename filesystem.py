"""
操作系统课程设计 - 文件系统模块
功能：模拟磁盘、位图管理、索引结构、文件操作
选题：异步I/O、管道、条件变量、优先级调度、位图+索引
"""

import os
import struct
import time
import threading
import asyncio
from datetime import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple
from enum import Enum
import json

# ==================== 常量定义 ====================
BLOCK_SIZE = 64          # 每个盘块大小 64B
TOTAL_BLOCKS = 1024      # 总盘块数
DISK_FILE = "virtual_disk.dat"

# 磁盘布局
SUPERBLOCK_START = 0     # 超级块：1个块
BITMAP_START = 1         # 位图区：2个块 (1024位 / 8 / 64 = 2块)
INODE_START = 3          # iNode区：32个块 (每个iNode 32B，共64个)
DATA_START = 35          # 数据区：从第35块开始

MAX_INODES = 32          # 修改：每个iNode 64字节，32个块存32个iNode
MAX_FILENAME = 20        # 修改：缩短文件名以容纳块索引
DIRECT_BLOCKS = 10       # 直接索引数
INDIRECT_BLOCKS = 1      # 一级间接索引数

# 文件权限
class Permission(Enum):
    READ = 0b100
    WRITE = 0b010
    EXECUTE = 0b001

# ==================== 数据结构定义 ====================

@dataclass
class SuperBlock:
    """超级块 - 存储文件系统元信息"""
    magic: int = 0x4F534653           # 魔数 "OSFS"
    total_blocks: int = TOTAL_BLOCKS
    block_size: int = BLOCK_SIZE
    free_blocks: int = TOTAL_BLOCKS - DATA_START
    free_inodes: int = MAX_INODES
    inode_count: int = MAX_INODES
    data_start: int = DATA_START
    
    def to_bytes(self) -> bytes:
        data = struct.pack('<IIIIIII', 
            self.magic, self.total_blocks, self.block_size,
            self.free_blocks, self.free_inodes, self.inode_count, self.data_start)
        return data.ljust(BLOCK_SIZE, b'\x00')
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'SuperBlock':
        values = struct.unpack('<IIIIIII', data[:28])
        return cls(*values)

@dataclass  
class INode:
    """iNode节点 - 文件控制块"""
    inode_id: int = 0
    filename: str = ""
    file_size: int = 0
    create_time: float = 0.0
    modify_time: float = 0.0
    permission: int = 0b110          # rw-
    is_used: bool = False
    is_directory: bool = False
    ref_count: int = 0               # 引用计数，用于文件保护
    direct_blocks: List[int] = field(default_factory=lambda: [-1] * DIRECT_BLOCKS)
    indirect_block: int = -1         # 一级间接索引块号
    
    def to_bytes(self) -> bytes:
        """
        序列化iNode为64字节
        格式:
        - id (1B)
        - filename (20B)
        - file_size (4B)
        - flags (1B): is_used(1) + is_directory(1) + permission(3) + ref_count低3位
        - direct_blocks (10 * 2B = 20B) - 使用short存储块号
        - indirect_block (2B)
        - padding (16B)
        总计: 1 + 20 + 4 + 1 + 20 + 2 + 16 = 64B
        """
        # 文件名
        name_bytes = self.filename.encode('utf-8')[:MAX_FILENAME].ljust(MAX_FILENAME, b'\x00')
        
        # flags: is_used(1) | is_directory(1) | permission(3) | ref_count低3位
        flags = 0
        if self.is_used:
            flags |= 0x80
        if self.is_directory:
            flags |= 0x40
        flags |= (self.permission & 0x07) << 3
        flags |= (self.ref_count & 0x07)
        
        # 构建数据
        data = struct.pack('<B', self.inode_id)  # 1B
        data += name_bytes                        # 20B
        data += struct.pack('<I', self.file_size) # 4B
        data += struct.pack('<B', flags)          # 1B
        
        # direct_blocks - 每个用2字节short存储 (可存储-1到32767)
        for i in range(DIRECT_BLOCKS):
            block_num = self.direct_blocks[i] if i < len(self.direct_blocks) else -1
            data += struct.pack('<h', block_num)  # 20B total
        
        # indirect_block
        data += struct.pack('<h', self.indirect_block)  # 2B
        
        # padding到64字节
        data = data.ljust(64, b'\x00')
        
        return data[:64]
    
    @classmethod
    def from_bytes(cls, data: bytes, inode_id: int) -> 'INode':
        """从字节反序列化 - 64字节格式"""
        if len(data) < 64:
            return cls(inode_id=inode_id)
        
        try:
            # 解析各字段
            id_byte = data[0]
            filename = data[1:21].rstrip(b'\x00').decode('utf-8', errors='ignore')
            file_size = struct.unpack('<I', data[21:25])[0]
            flags = data[25]
            
            is_used = bool(flags & 0x80)
            is_directory = bool(flags & 0x40)
            permission = (flags >> 3) & 0x07
            ref_count = flags & 0x07
            
            # 解析direct_blocks
            direct_blocks = []
            offset = 26
            for i in range(DIRECT_BLOCKS):
                block_num = struct.unpack('<h', data[offset:offset+2])[0]
                direct_blocks.append(block_num)
                offset += 2
            
            # 解析indirect_block
            indirect_block = struct.unpack('<h', data[offset:offset+2])[0]
            
            return cls(
                inode_id=id_byte if id_byte == inode_id else inode_id,
                filename=filename,
                file_size=file_size,
                is_used=is_used,
                is_directory=is_directory,
                permission=permission,
                ref_count=ref_count,
                direct_blocks=direct_blocks,
                indirect_block=indirect_block
            )
        except Exception as e:
            print(f"[INode] 解析错误: {e}")
            return cls(inode_id=inode_id)

class Bitmap:
    """位示图 - 空闲块管理"""
    
    def __init__(self, total_blocks: int = TOTAL_BLOCKS):
        self.total_blocks = total_blocks
        # 使用位数组，每位代表一个块的使用状态
        self.bits = bytearray((total_blocks + 7) // 8)
        # 预留系统区域
        for i in range(DATA_START):
            self.set_used(i)
    
    def set_used(self, block_num: int):
        """标记块为已使用"""
        if 0 <= block_num < self.total_blocks:
            byte_idx = block_num // 8
            bit_idx = block_num % 8
            self.bits[byte_idx] |= (1 << bit_idx)
    
    def set_free(self, block_num: int):
        """标记块为空闲"""
        if 0 <= block_num < self.total_blocks:
            byte_idx = block_num // 8
            bit_idx = block_num % 8
            self.bits[byte_idx] &= ~(1 << bit_idx)
    
    def is_free(self, block_num: int) -> bool:
        """检查块是否空闲"""
        if 0 <= block_num < self.total_blocks:
            byte_idx = block_num // 8
            bit_idx = block_num % 8
            return not (self.bits[byte_idx] & (1 << bit_idx))
        return False
    
    def allocate_block(self) -> int:
        """分配一个空闲块，返回块号，-1表示无空闲块"""
        for i in range(DATA_START, self.total_blocks):
            if self.is_free(i):
                self.set_used(i)
                return i
        return -1
    
    def allocate_blocks(self, count: int) -> List[int]:
        """分配多个连续或不连续的空闲块"""
        blocks = []
        for _ in range(count):
            block = self.allocate_block()
            if block == -1:
                # 回滚已分配的块
                for b in blocks:
                    self.set_free(b)
                return []
            blocks.append(block)
        return blocks
    
    def free_blocks_count(self) -> int:
        """返回空闲块数量"""
        count = 0
        for i in range(DATA_START, self.total_blocks):
            if self.is_free(i):
                count += 1
        return count
    
    def get_bitmap_status(self) -> List[bool]:
        """获取位图状态列表，用于可视化"""
        return [not self.is_free(i) for i in range(self.total_blocks)]
    
    def to_bytes(self) -> bytes:
        """序列化位图"""
        return bytes(self.bits)
    
    @classmethod
    def from_bytes(cls, data: bytes, total_blocks: int = TOTAL_BLOCKS) -> 'Bitmap':
        """从字节反序列化"""
        bitmap = cls(total_blocks)
        bitmap.bits = bytearray(data[:len(bitmap.bits)])
        return bitmap

class IndexBlock:
    """索引块 - 用于存储文件数据块索引"""
    
    def __init__(self):
        # 每个索引项4字节(int)，64B可存储16个索引
        self.indices: List[int] = [-1] * 16
    
    def to_bytes(self) -> bytes:
        return struct.pack('<' + 'i' * 16, *self.indices)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> 'IndexBlock':
        idx = cls()
        idx.indices = list(struct.unpack('<' + 'i' * 16, data[:64]))
        return idx

# ==================== 虚拟磁盘类 ====================

class VirtualDisk:
    """虚拟磁盘 - 模拟物理磁盘操作"""
    
    def __init__(self, disk_path: str = DISK_FILE):
        self.disk_path = disk_path
        self.lock = threading.RLock()
        
    def create_disk(self):
        """创建新的虚拟磁盘文件"""
        with self.lock:
            with open(self.disk_path, 'wb') as f:
                # 写入全0数据
                f.write(b'\x00' * BLOCK_SIZE * TOTAL_BLOCKS)
    
    def read_block(self, block_num: int) -> bytes:
        """读取指定块"""
        with self.lock:
            with open(self.disk_path, 'rb') as f:
                f.seek(block_num * BLOCK_SIZE)
                return f.read(BLOCK_SIZE)
    
    def write_block(self, block_num: int, data: bytes):
        """写入指定块"""
        with self.lock:
            # 确保数据长度正确
            data = data[:BLOCK_SIZE].ljust(BLOCK_SIZE, b'\x00')
            with open(self.disk_path, 'r+b') as f:
                f.seek(block_num * BLOCK_SIZE)
                f.write(data)
    
    def read_blocks(self, block_nums: List[int]) -> bytes:
        """读取多个块"""
        data = b''
        for num in block_nums:
            if num >= 0:
                data += self.read_block(num)
        return data
    
    def exists(self) -> bool:
        """检查磁盘文件是否存在"""
        return os.path.exists(self.disk_path)

# ==================== 文件系统类 ====================

class FileSystem:
    """文件系统 - 管理文件的创建、读写、删除等操作"""
    
    def __init__(self, disk_path: str = DISK_FILE):
        self.disk = VirtualDisk(disk_path)
        self.superblock: Optional[SuperBlock] = None
        self.bitmap: Optional[Bitmap] = None
        self.inodes: Dict[int, INode] = {}
        self.lock = threading.RLock()
        self.file_locks: Dict[str, threading.RLock] = {}  # 文件级锁
        
    def format_disk(self):
        """格式化磁盘，初始化文件系统"""
        with self.lock:
            # 创建新磁盘
            self.disk.create_disk()
            
            # 初始化超级块
            self.superblock = SuperBlock()
            self.disk.write_block(SUPERBLOCK_START, self.superblock.to_bytes())
            
            # 初始化位图
            self.bitmap = Bitmap()
            bitmap_data = self.bitmap.to_bytes()
            for i in range(2):  # 位图占2个块
                start = i * BLOCK_SIZE
                end = start + BLOCK_SIZE
                self.disk.write_block(BITMAP_START + i, bitmap_data[start:end])
            
            # 初始化iNode区
            self.inodes = {}
            for i in range(MAX_INODES):
                inode = INode(inode_id=i)
                self.inodes[i] = inode
            self._save_inodes()
            
            print(f"[FileSystem] 磁盘格式化完成，总块数: {TOTAL_BLOCKS}, 可用数据块: {self.bitmap.free_blocks_count()}")
    
    def mount(self) -> bool:
        """挂载文件系统"""
        with self.lock:
            if not self.disk.exists():
                print("[FileSystem] 磁盘文件不存在，正在创建...")
                self.format_disk()
                return True
            
            try:
                # 读取超级块
                sb_data = self.disk.read_block(SUPERBLOCK_START)
                self.superblock = SuperBlock.from_bytes(sb_data)
                
                if self.superblock.magic != 0x4F534653:
                    print("[FileSystem] 无效的文件系统，正在格式化...")
                    self.format_disk()
                    return True
                
                # 读取位图
                bitmap_data = b''
                for i in range(2):
                    bitmap_data += self.disk.read_block(BITMAP_START + i)
                self.bitmap = Bitmap.from_bytes(bitmap_data)
                
                # 读取iNode
                self._load_inodes()
                
                print(f"[FileSystem] 文件系统挂载成功，空闲块: {self.bitmap.free_blocks_count()}")
                return True
            except Exception as e:
                print(f"[FileSystem] 挂载失败: {e}")
                return False
    
    def _save_inodes(self):
        """保存所有iNode到磁盘 - 每个iNode 64字节，占用一个完整块"""
        for i in range(MAX_INODES):
            if i in self.inodes:
                inode = self.inodes[i]
                # 每个iNode占用一个完整的块
                block_idx = INODE_START + i
                inode_bytes = inode.to_bytes()
                self.disk.write_block(block_idx, inode_bytes)
    
    def _load_inodes(self):
        """从磁盘加载所有iNode - 每个iNode占用一个完整块"""
        self.inodes = {}
        for i in range(MAX_INODES):
            block_idx = INODE_START + i
            block_data = self.disk.read_block(block_idx)
            inode = INode.from_bytes(block_data, i)
            self.inodes[i] = inode
    
    def _save_bitmap(self):
        """保存位图到磁盘"""
        bitmap_data = self.bitmap.to_bytes()
        for i in range(2):
            start = i * BLOCK_SIZE
            end = start + BLOCK_SIZE
            self.disk.write_block(BITMAP_START + i, bitmap_data[start:end])
    
    def _allocate_inode(self) -> Optional[INode]:
        """分配一个空闲iNode"""
        for i in range(MAX_INODES):
            if not self.inodes[i].is_used:
                self.inodes[i].is_used = True
                self.inodes[i].inode_id = i
                self.inodes[i].create_time = time.time()
                self.inodes[i].modify_time = time.time()
                self.superblock.free_inodes -= 1
                return self.inodes[i]
        return None
    
    def create_file(self, filename: str, content: bytes, permission: int = 0b110) -> Tuple[bool, str]:
        """
        创建新文件
        返回: (成功标志, 消息)
        """
        with self.lock:
            # 检查文件名是否已存在
            for inode in self.inodes.values():
                if inode.is_used and inode.filename == filename:
                    return False, f"文件 '{filename}' 已存在"
            
            # 分配iNode
            inode = self._allocate_inode()
            if inode is None:
                return False, "iNode已满，无法创建文件"
            
            # 计算需要的块数
            blocks_needed = (len(content) + BLOCK_SIZE - 1) // BLOCK_SIZE
            if blocks_needed == 0:
                blocks_needed = 1
            
            # 检查是否需要间接索引
            if blocks_needed > DIRECT_BLOCKS:
                blocks_needed += 1  # 额外的间接索引块
            
            # 分配数据块
            if blocks_needed > DIRECT_BLOCKS + 1:
                # 需要间接索引
                data_blocks = self.bitmap.allocate_blocks(DIRECT_BLOCKS)
                if len(data_blocks) < DIRECT_BLOCKS:
                    return False, "空间不足"
                
                indirect_block = self.bitmap.allocate_block()
                if indirect_block == -1:
                    for b in data_blocks:
                        self.bitmap.set_free(b)
                    return False, "空间不足"
                
                extra_blocks = self.bitmap.allocate_blocks(blocks_needed - DIRECT_BLOCKS - 1)
                if len(extra_blocks) < blocks_needed - DIRECT_BLOCKS - 1:
                    for b in data_blocks + [indirect_block]:
                        self.bitmap.set_free(b)
                    return False, "空间不足"
                
                # 设置直接索引
                inode.direct_blocks = data_blocks
                inode.indirect_block = indirect_block
                
                # 写入间接索引块
                idx_block = IndexBlock()
                idx_block.indices[:len(extra_blocks)] = extra_blocks
                self.disk.write_block(indirect_block, idx_block.to_bytes())
                
                all_blocks = data_blocks + extra_blocks
            else:
                # 只需要直接索引
                data_blocks = self.bitmap.allocate_blocks(blocks_needed)
                if len(data_blocks) < blocks_needed:
                    return False, "空间不足"
                
                inode.direct_blocks = data_blocks + [-1] * (DIRECT_BLOCKS - len(data_blocks))
                all_blocks = data_blocks
            
            # 写入文件内容
            for i, block_num in enumerate(all_blocks):
                start = i * BLOCK_SIZE
                end = start + BLOCK_SIZE
                block_data = content[start:end]
                self.disk.write_block(block_num, block_data)
            
            # 更新iNode
            inode.filename = filename[:MAX_FILENAME]
            inode.file_size = len(content)
            inode.permission = permission
            inode.ref_count = 0
            
            # 保存元数据
            self._save_inodes()
            self._save_bitmap()
            self.superblock.free_blocks = self.bitmap.free_blocks_count()
            self.disk.write_block(SUPERBLOCK_START, self.superblock.to_bytes())
            
            return True, f"文件 '{filename}' 创建成功，占用 {len(all_blocks)} 个块"
    
    def read_file(self, filename: str) -> Tuple[Optional[bytes], str]:
        """读取文件内容"""
        with self.lock:
            inode = self._find_inode(filename)
            if inode is None:
                return None, f"文件 '{filename}' 不存在"
            
            # 收集所有数据块
            all_blocks = []
            for b in inode.direct_blocks:
                if b >= 0:
                    all_blocks.append(b)
            
            # 处理间接索引
            if inode.indirect_block >= 0:
                idx_data = self.disk.read_block(inode.indirect_block)
                idx_block = IndexBlock.from_bytes(idx_data)
                for b in idx_block.indices:
                    if b >= 0:
                        all_blocks.append(b)
            
            # 读取内容
            content = self.disk.read_blocks(all_blocks)
            return content[:inode.file_size], "读取成功"
    
    def read_block_content(self, filename: str, block_index: int) -> Tuple[Optional[bytes], str]:
        """读取文件指定块的内容"""
        with self.lock:
            inode = self._find_inode(filename)
            if inode is None:
                return None, f"文件 '{filename}' 不存在"
            
            # 获取所有数据块列表
            all_blocks = [b for b in inode.direct_blocks if b >= 0]
            if inode.indirect_block >= 0:
                idx_data = self.disk.read_block(inode.indirect_block)
                idx_block = IndexBlock.from_bytes(idx_data)
                all_blocks.extend([b for b in idx_block.indices if b >= 0])
            
            if block_index < 0 or block_index >= len(all_blocks):
                return None, f"块索引 {block_index} 超出范围 (0-{len(all_blocks)-1})"
            
            block_num = all_blocks[block_index]
            content = self.disk.read_block(block_num)
            return content, f"成功读取块 {block_num}"
    
    def modify_block_content(self, filename: str, block_index: int, new_content: bytes) -> Tuple[bool, str]:
        """修改文件指定块的内容（同时更新文件大小）"""
        with self.lock:
            inode = self._find_inode(filename)
            if inode is None:
                return False, f"文件 '{filename}' 不存在"

            # 检查写权限
            if not (inode.permission & Permission.WRITE.value):
                return False, "没有写权限"

            # 获取所有数据块列表
            all_blocks = [b for b in inode.direct_blocks if b >= 0]
            if inode.indirect_block >= 0:
                idx_data = self.disk.read_block(inode.indirect_block)
                idx_block = IndexBlock.from_bytes(idx_data)
                all_blocks.extend([b for b in idx_block.indices if b >= 0])

            if block_index < 0 or block_index >= len(all_blocks):
                return False, f"块索引 {block_index} 超出范围"

            block_num = all_blocks[block_index]
            # 写入时填充到BLOCK_SIZE
            padded_content = new_content[:BLOCK_SIZE].ljust(BLOCK_SIZE, b'\x00')
            self.disk.write_block(block_num, padded_content)

            # 计算新的文件大小（如果写入使文件变大）
            new_potential_size = block_index * BLOCK_SIZE + len(new_content)
            if new_potential_size > inode.file_size:
                inode.file_size = new_potential_size

            # 更新修改时间
            inode.modify_time = time.time()
            self._save_inodes()

            return True, f"成功修改块 {block_num}，文件大小: {inode.file_size} 字节"

    def write_file(self, filename: str, content: bytes) -> Tuple[bool, str]:
        """
        写入/覆盖文件内容（会更新文件大小）
        如果新内容需要更多块，会自动分配；如果需要更少块，会释放多余的块
        """
        with self.lock:
            inode = self._find_inode(filename)
            if inode is None:
                return False, f"文件 '{filename}' 不存在"

            # 检查写权限
            if not (inode.permission & Permission.WRITE.value):
                return False, "没有写权限"

            # 检查引用计数
            if inode.ref_count > 0:
                return False, f"文件 '{filename}' 正在被使用"

            # 计算需要的块数
            new_blocks_needed = (len(content) + BLOCK_SIZE - 1) // BLOCK_SIZE
            if new_blocks_needed == 0:
                new_blocks_needed = 1

            # 获取当前占用的所有块
            old_blocks = [b for b in inode.direct_blocks if b >= 0]
            old_indirect_block = inode.indirect_block
            if old_indirect_block >= 0:
                idx_data = self.disk.read_block(old_indirect_block)
                idx_block = IndexBlock.from_bytes(idx_data)
                old_blocks.extend([b for b in idx_block.indices if b >= 0])

            old_blocks_count = len(old_blocks)

            # 判断是否需要扩展或收缩
            if new_blocks_needed <= old_blocks_count:
                # 收缩或保持不变：释放多余的块
                blocks_to_use = old_blocks[:new_blocks_needed]
                blocks_to_free = old_blocks[new_blocks_needed:]

                # 释放多余的数据块
                for b in blocks_to_free:
                    self.bitmap.set_free(b)

                # 如果不再需要间接索引块，释放它
                if new_blocks_needed <= DIRECT_BLOCKS and old_indirect_block >= 0:
                    self.bitmap.set_free(old_indirect_block)
                    inode.indirect_block = -1

                # 更新直接索引
                inode.direct_blocks = blocks_to_use[:DIRECT_BLOCKS] + [-1] * max(0, DIRECT_BLOCKS - len(blocks_to_use))

                # 如果还需要间接索引
                if new_blocks_needed > DIRECT_BLOCKS:
                    idx_block = IndexBlock()
                    idx_block.indices[:len(blocks_to_use) - DIRECT_BLOCKS] = blocks_to_use[DIRECT_BLOCKS:]
                    self.disk.write_block(inode.indirect_block, idx_block.to_bytes())

                all_blocks = blocks_to_use

            else:
                # 扩展：需要分配新块
                extra_needed = new_blocks_needed - old_blocks_count

                # 检查是否需要新的间接索引块
                need_new_indirect = new_blocks_needed > DIRECT_BLOCKS and old_indirect_block < 0

                if need_new_indirect:
                    extra_needed += 1  # 为间接索引块预留

                new_blocks = self.bitmap.allocate_blocks(extra_needed)
                if len(new_blocks) < extra_needed:
                    # 回滚
                    for b in new_blocks:
                        self.bitmap.set_free(b)
                    return False, "磁盘空间不足"

                # 分配间接索引块（如果需要）
                if need_new_indirect:
                    inode.indirect_block = new_blocks.pop(0)

                all_blocks = old_blocks + new_blocks

                # 更新直接索引
                inode.direct_blocks = all_blocks[:DIRECT_BLOCKS] + [-1] * max(0, DIRECT_BLOCKS - len(all_blocks))

                # 更新间接索引
                if new_blocks_needed > DIRECT_BLOCKS:
                    idx_block = IndexBlock()
                    extra_indices = all_blocks[DIRECT_BLOCKS:]
                    idx_block.indices[:len(extra_indices)] = extra_indices
                    self.disk.write_block(inode.indirect_block, idx_block.to_bytes())

            # 写入新内容
            for i, block_num in enumerate(all_blocks):
                start = i * BLOCK_SIZE
                end = start + BLOCK_SIZE
                block_data = content[start:end].ljust(BLOCK_SIZE, b'\x00')
                self.disk.write_block(block_num, block_data)

            # 更新iNode - 关键：更新文件大小！
            inode.file_size = len(content)
            inode.modify_time = time.time()

            # 保存元数据
            self._save_inodes()
            self._save_bitmap()
            self.superblock.free_blocks = self.bitmap.free_blocks_count()
            self.disk.write_block(SUPERBLOCK_START, self.superblock.to_bytes())

            return True, f"文件 '{filename}' 写入成功，大小: {len(content)} 字节，占用 {len(all_blocks)} 块"

    def delete_file(self, filename: str) -> Tuple[bool, str]:
        """删除文件"""
        with self.lock:
            inode = self._find_inode(filename)
            if inode is None:
                return False, f"文件 '{filename}' 不存在"

            # 检查引用计数（文件保护）
            if inode.ref_count > 0:
                return False, f"文件 '{filename}' 正在被使用，无法删除"

            # 释放数据块
            for b in inode.direct_blocks:
                if b >= 0:
                    self.bitmap.set_free(b)

            # 释放间接索引块
            if inode.indirect_block >= 0:
                idx_data = self.disk.read_block(inode.indirect_block)
                idx_block = IndexBlock.from_bytes(idx_data)
                for b in idx_block.indices:
                    if b >= 0:
                        self.bitmap.set_free(b)
                self.bitmap.set_free(inode.indirect_block)

            # 重置iNode
            inode.is_used = False
            inode.filename = ""
            inode.file_size = 0
            inode.direct_blocks = [-1] * DIRECT_BLOCKS
            inode.indirect_block = -1
            inode.ref_count = 0

            self.superblock.free_inodes += 1

            # 保存
            self._save_inodes()
            self._save_bitmap()
            self.superblock.free_blocks = self.bitmap.free_blocks_count()
            self.disk.write_block(SUPERBLOCK_START, self.superblock.to_bytes())

            return True, f"文件 '{filename}' 删除成功"

    def list_directory(self) -> List[Dict]:
        """列出目录中所有文件"""
        files = []
        for inode in self.inodes.values():
            if inode.is_used and not inode.is_directory:
                # 计算占用块数
                blocks = sum(1 for b in inode.direct_blocks if b >= 0)
                if inode.indirect_block >= 0:
                    idx_data = self.disk.read_block(inode.indirect_block)
                    idx_block = IndexBlock.from_bytes(idx_data)
                    blocks += sum(1 for b in idx_block.indices if b >= 0)
                    blocks += 1  # 间接索引块本身

                files.append({
                    'name': inode.filename,
                    'size': inode.file_size,
                    'blocks': blocks,
                    'create_time': datetime.fromtimestamp(inode.create_time).strftime('%Y-%m-%d %H:%M:%S') if inode.create_time else 'N/A',
                    'modify_time': datetime.fromtimestamp(inode.modify_time).strftime('%Y-%m-%d %H:%M:%S') if inode.modify_time else 'N/A',
                    'permission': f"{'r' if inode.permission & 4 else '-'}{'w' if inode.permission & 2 else '-'}{'x' if inode.permission & 1 else '-'}",
                    'ref_count': inode.ref_count,
                    'inode_id': inode.inode_id
                })
        return files

    def _find_inode(self, filename: str) -> Optional[INode]:
        """根据文件名查找iNode"""
        for inode in self.inodes.values():
            if inode.is_used and inode.filename == filename:
                return inode
        return None

    def acquire_file(self, filename: str) -> bool:
        """获取文件引用（增加引用计数）"""
        with self.lock:
            inode = self._find_inode(filename)
            if inode:
                inode.ref_count += 1
                self._save_inodes()
                return True
            return False

    def release_file(self, filename: str) -> bool:
        """释放文件引用（减少引用计数）"""
        with self.lock:
            inode = self._find_inode(filename)
            if inode and inode.ref_count > 0:
                inode.ref_count -= 1
                self._save_inodes()
                return True
            return False

    def get_disk_info(self) -> Dict:
        """获取磁盘信息"""
        with self.lock:
            return {
                'total_blocks': TOTAL_BLOCKS,
                'block_size': BLOCK_SIZE,
                'free_blocks': self.bitmap.free_blocks_count() if self.bitmap else 0,
                'used_blocks': TOTAL_BLOCKS - (self.bitmap.free_blocks_count() if self.bitmap else 0),
                'total_inodes': MAX_INODES,
                'free_inodes': self.superblock.free_inodes if self.superblock else 0,
                'data_start': DATA_START
            }

    def get_bitmap_visual(self) -> List[List[bool]]:
        """获取位图可视化数据（32x32网格）"""
        if not self.bitmap:
            return []
        status = self.bitmap.get_bitmap_status()
        # 转换为32x32网格
        grid = []
        for i in range(32):
            row = status[i*32:(i+1)*32]
            grid.append(row)
        return grid


# ==================== 测试代码 ====================
if __name__ == "__main__":
    fs = FileSystem()
    fs.mount()

    # 测试创建文件
    content = b"Hello, World! This is a test file content." * 10
    success, msg = fs.create_file("test.txt", content)
    print(msg)

    # 测试列出目录
    files = fs.list_directory()
    print("\n目录列表:")
    for f in files:
        print(f"  {f['name']} - {f['size']}B - {f['blocks']}块 - {f['create_time']}")

    # 测试读取文件
    data, msg = fs.read_file("test.txt")
    print(f"\n读取文件: {msg}")
    print(f"内容前50字节: {data[:50] if data else 'N/A'}")

    # 测试磁盘信息
    info = fs.get_disk_info()
    print(f"\n磁盘信息: {info}")