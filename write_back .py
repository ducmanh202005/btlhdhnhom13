import time
import os
import sys
import random

# Ghi hoãn lại (Write-Back) + LRU Eviction
# Dirty Bit: Đánh dấu khi ghi, flush khi thay thế
# ============================================================================

# ============================================================================
# 1. THAM SỐ CẤU HÌNH HỆ THỐNG
# ============================================================================
BLOCK_SIZE = 4096  # Kích thước một block (bytes)
HDD_CAPACITY = 10000  # Tổng số block HDD
HDD_READ_LATENCY = 8  # Trễ đọc HDD (ms)
HDD_WRITE_LATENCY = 10  # Trễ ghi HDD (ms)
CACHE_SIZE = 128  # Số slot cache (SSD)
SSD_READ_LATENCY = 0.1  # Trễ đọc SSD cache (ms)
SSD_WRITE_LATENCY = 0.2  # Trễ ghi SSD cache (ms)


# ============================================================================
# 2. CẤU TRÚC DỮ LIỆU
# ============================================================================

class CacheEntry:
    def __init__(self):
        self.blockID = -1
        self.data = 0
        self.timestamp = 0
        self.valid = False
        self.dirty = False  # [WRITE-BACK CORE]


class HDDEntry:
    def __init__(self, blockID):
        self.blockID = blockID
        self.data = 0


class StorageSystem:
    def __init__(self):
        # Cấu trúc lưu trữ
        self.ssdCache = [CacheEntry() for _ in range(CACHE_SIZE)]
        self.hdd = [HDDEntry(i) for i in range(HDD_CAPACITY)]

        # Các biến đếm để tính toán chỉ số
        self.cacheHits = 0
        self.cacheMisses = 0
        self.totalReads = 0
        self.totalWrites = 0

        # Các chỉ số yêu cầu trong ảnh
        self.totalReadLatency = 0.0  # Thời gian read
        self.totalWriteLatency = 0.0  # Thời gian write
        self.hddReadCount = 0  # Số lần truy cập HDD khi read
        self.hddWriteCount = 0  # Số lần truy cập HDD khi write (Flush)

        self.currentTime = 0  # Clock cho LRU timestamp

    def tick(self):
        """Tăng thời gian hệ thống (cho LRU)"""
        self.currentTime += 1


# ============================================================================
# 3. HÀM TÌM KIẾM VÀ QUẢN LÝ CACHE
# ============================================================================

def find_in_cache(system, blockID):
    """Tìm block trong cache, trả về index hoặc -1 nếu không tìm thấy"""
    for i in range(CACHE_SIZE):
        if system.ssdCache[i].valid and system.ssdCache[i].blockID == blockID:
            return i
    return -1


def find_lru_victim(system):
    """Tìm entry có timestamp nhỏ nhất (LRU) hoặc slot trống"""
    min_timestamp = float('inf')
    victim_index = 0

    # Bước 1: Tìm slot trống
    for i in range(CACHE_SIZE):
        if not system.ssdCache[i].valid:
            return i

    # Bước 2: Cache đầy → Tìm LRU (timestamp nhỏ nhất)
    for i in range(CACHE_SIZE):
        if system.ssdCache[i].timestamp < min_timestamp:
            min_timestamp = system.ssdCache[i].timestamp
            victim_index = i

    return victim_index


def flush_entry(system, index):
    """Ghi một entry dirty xuống HDD"""
    entry = system.ssdCache[index]

    if entry.valid and entry.dirty:
        # Ghi xuống HDD
        system.hdd[entry.blockID].data = entry.data

        # Cộng latency (mô phỏng thời gian ghi HDD)
        system.totalWriteLatency += HDD_WRITE_LATENCY

        # Tăng counter truy cập HDD khi write
        system.hddWriteCount += 1

        # Đánh dấu sạch
        entry.dirty = False


def load_to_cache(system, blockID, cache_index):
    """Load block từ HDD vào cache"""
    data = system.hdd[blockID].data

    system.ssdCache[cache_index].blockID = blockID
    system.ssdCache[cache_index].data = data
    system.ssdCache[cache_index].timestamp = system.currentTime
    system.ssdCache[cache_index].valid = True
    system.ssdCache[cache_index].dirty = False  # Vừa load từ HDD nên sạch


# ============================================================================
# 4. HÀM ĐỌC/GHI WRITE-BACK
# ============================================================================

def cache_read(system, blockID):
    """Đọc block từ cache (Write-Back policy)"""
    system.totalReads += 1
    system.tick()

    cache_index = find_in_cache(system, blockID)

    if cache_index != -1:
        # ===== CACHE HIT =====
        system.cacheHits += 1
        system.ssdCache[cache_index].timestamp = system.currentTime

        latency = SSD_READ_LATENCY  # 0.1ms
        system.totalReadLatency += latency

        return system.ssdCache[cache_index].data, latency

    else:
        # ===== CACHE MISS =====
        system.cacheMisses += 1

        # Miss khi đọc -> Phải đọc từ HDD -> Tăng HDD Read Count
        system.hddReadCount += 1

        latency = HDD_READ_LATENCY  # 8ms

        # Tìm victim
        victim_index = find_lru_victim(system)

        # [WRITE-BACK KEY] Nếu victim bẩn → FLUSH trước khi ghi đè
        if system.ssdCache[victim_index].valid and system.ssdCache[victim_index].dirty:
            flush_entry(system, victim_index)
            # flush_entry() đã cộng HDD_WRITE_LATENCY vào totalWriteLatency

        # Load block mới từ HDD
        load_to_cache(system, blockID, victim_index)

        system.totalReadLatency += latency
        return system.ssdCache[victim_index].data, latency


def cache_write(system, blockID, new_data):
    """
    Ghi block vào cache (Write-Back policy)
    
    """
    system.totalWrites += 1
    system.tick()

    cache_index = find_in_cache(system, blockID)

    if cache_index != -1:
        # ===== WRITE HIT =====
        # Chỉ tốn SSD latency (0.2ms)
        system.ssdCache[cache_index].data = new_data
        system.ssdCache[cache_index].timestamp = system.currentTime
        system.ssdCache[cache_index].dirty = True  # [KEY] Đánh dấu bẩn

        current_latency = SSD_WRITE_LATENCY  # 0.2ms

    else:
        # ===== WRITE MISS =====
        # Write Allocate: Phải load block lên cache trước
        system.hddReadCount += 1  # Load từ HDD tính là 1 lần đọc HDD

        # Bước 1: Tìm victim
        victim_index = find_lru_victim(system)

        # Bước 2: Nếu victim bẩn → FLUSH
        if system.ssdCache[victim_index].valid and system.ssdCache[victim_index].dirty:
            flush_entry(system, victim_index)
            # flush_entry() đã cộng HDD_WRITE_LATENCY vào totalWriteLatency

        # Bước 3: Load block cần ghi từ HDD (Write-Allocate)
        load_to_cache(system, blockID, victim_index)

        # Bước 4: Ghi dữ liệu mới vào cache
        system.ssdCache[victim_index].data = new_data
        system.ssdCache[victim_index].dirty = True  # [KEY] Đánh dấu bẩn

        current_latency = SSD_WRITE_LATENCY  # 0.2ms

    system.totalWriteLatency += current_latency
    return current_latency


def flush_all_cache(system):
    """Flush tất cả dirty blocks xuống HDD"""
    count = 0
    for i in range(CACHE_SIZE):
        if system.ssdCache[i].valid and system.ssdCache[i].dirty:
            flush_entry(system, i)
            count += 1
    # print(f"   [System] Flush All: {count} dirty blocks written to HDD")


# ============================================================================
# 5. ĐỌC VÀ THỰC THI WORKLOAD
# ============================================================================

def parse_workload(filename):
    """Đọc file workload và parse thành list operations"""
    operations = []
    try:
        try:
            f = open(filename, 'r', encoding='utf-8')
            content = f.readlines()
            f.close()
        except UnicodeDecodeError:
            f = open(filename, 'r', encoding='cp1252')
            content = f.readlines()
            f.close()

        for line in content:
            line = line.strip()
            if not line or line.startswith('#'):
                continue

            parts = line.split()
            op = parts[0].upper()

            if op == 'R':
                operations.append(('R', int(parts[1]), None))
            elif op == 'W':
                val = int(parts[2]) if len(parts) > 2 else 0
                operations.append(('W', int(parts[1]), val))
            elif op == 'F':
                operations.append(('F', None, None))
            elif op == 'S':
                operations.append(('S', None, None))

        print(f"✓ Đọc {len(operations)} operations từ {filename}")
        return operations

    except FileNotFoundError:
        print(f"✗ Không tìm thấy file: {filename}")
        return []


def execute_workload(system, operations):
    """Thực thi từng operation trong workload"""
    for op, blockID, value in operations:
        if op == 'R':
            cache_read(system, blockID)
        elif op == 'W':
            cache_write(system, blockID, value)
        elif op == 'F':
            flush_all_cache(system)


# ============================================================================
# 6. HIỂN THỊ THỐNG KÊ
# ============================================================================

def print_statistics(system, name):
    """In thống kê chi tiết cho một workload"""
    total_access = system.cacheHits + system.cacheMisses
    hit_rate = (system.cacheHits / total_access * 100) if total_access > 0 else 0
    miss_rate = (system.cacheMisses / total_access * 100) if total_access > 0 else 0
    total_time = system.totalReadLatency + system.totalWriteLatency

    print(f"\n{'=' * 70}")
    print(f"THỐNG KÊ WRITE-BACK: {name}")
    print(f"{'=' * 70}")

    # 7 Chỉ số theo yêu cầu
    print(f"  Hit rate:                      {hit_rate:.2f}%")
    print(f"  Miss rate:                     {miss_rate:.2f}%")
    print(f"  Số lần truy cập HDD khi read:  {system.hddReadCount:,}")
    print(f"  Số lần truy cập HDD khi write: {system.hddWriteCount:,}")
    print(f"  Thời gian read:                {system.totalReadLatency:.2f} ms")
    print(f"  Thời gian write:               {system.totalWriteLatency:.2f} ms")
    print(f"  Tổng thời gian xử lý:          {total_time:.2f} ms")
    print(f"{'=' * 70}")


def compare_workloads(results):
    """So sánh kết quả của 4 workloads"""
    print(f"\n{'=' * 110}")
    print("BẢNG SO SÁNH 4 LOẠI WORKLOAD (WRITE-BACK)")
    print(f"{'=' * 110}")
    
    print(f"\n{'Chỉ số':<35} {'Random':<18} {'Sequential':<18} {'Locality':<18} {'Write-Heavy':<18}")
    print("-" * 110)

    systems = [r[1] for r in results]

    # 1. Hit Rate
    hit_rates = [
        (s.cacheHits / (s.cacheHits + s.cacheMisses) * 100) if (s.cacheHits + s.cacheMisses) > 0 else 0
        for s in systems
    ]
    print(f"{'Hit Rate (%)':<35} {hit_rates[0]:>6.2f}%          {hit_rates[1]:>6.2f}%          {hit_rates[2]:>6.2f}%          {hit_rates[3]:>6.2f}%")

    # 2. Miss Rate
    miss_rates = [
        (s.cacheMisses / (s.cacheHits + s.cacheMisses) * 100) if (s.cacheHits + s.cacheMisses) > 0 else 0
        for s in systems
    ]
    print(f"{'Miss Rate (%)':<35} {miss_rates[0]:>6.2f}%          {miss_rates[1]:>6.2f}%          {miss_rates[2]:>6.2f}%          {miss_rates[3]:>6.2f}%")

    # 3. Số lần truy cập HDD khi read
    hdd_reads = [s.hddReadCount for s in systems]
    print(f"{'Số lần truy cập HDD (Read)':<35} {hdd_reads[0]:>7}          {hdd_reads[1]:>7}          {hdd_reads[2]:>7}          {hdd_reads[3]:>7}")

    # 4. Số lần truy cập HDD khi write
    hdd_writes = [s.hddWriteCount for s in systems]
    print(f"{'Số lần truy cập HDD (Write)':<35} {hdd_writes[0]:>7}          {hdd_writes[1]:>7}          {hdd_writes[2]:>7}          {hdd_writes[3]:>7}")

    # 5. Thời gian read
    read_times = [s.totalReadLatency for s in systems]
    print(f"{'Thời gian Read (ms)':<35} {read_times[0]:>10.2f}      {read_times[1]:>10.2f}      {read_times[2]:>10.2f}      {read_times[3]:>10.2f}")

    # 6. Thời gian write
    write_times = [s.totalWriteLatency for s in systems]
    print(f"{'Thời gian Write (ms)':<35} {write_times[0]:>10.2f}      {write_times[1]:>10.2f}      {write_times[2]:>10.2f}      {write_times[3]:>10.2f}")

    # 7. Tổng thời gian xử lý
    total_times = [s.totalReadLatency + s.totalWriteLatency for s in systems]
    print(f"{'Tổng thời gian xử lý (ms)':<35} {total_times[0]:>10.2f}      {total_times[1]:>10.2f}      {total_times[2]:>10.2f}      {total_times[3]:>10.2f}")

    print(f"\n{'=' * 110}")


# ============================================================================
# 7. CHƯƠNG TRÌNH CHÍNH
# ============================================================================

def main():
    print("=" * 70)
    print("MÔ PHỎNG WRITE-BACK CACHE")
    print("Chính sách: WRITE-BACK | Thay thế: LRU")
    print("=" * 70)

    # File workload cần chạy
    configs = [
        ("Random", "workload_random.txt"),
        ("Sequential", "workload_sequential.txt"),
        ("Locality", "workload_locality.txt"),
        ("Write-Heavy", "workload_write_heavy.txt")
    ]

    results = []

    for name, filename in configs:
        print(f"\n>>> Đang chạy: {name} ({filename})")

        if not os.path.exists(filename):
            print(f"Không tìm thấy file {filename}. Vui lòng tạo file workload trước.")
            continue

        sys_sim = StorageSystem()
        ops = parse_workload(filename)

        if ops:
            print(f"Executing {len(ops)} operations...")
            execute_workload(sys_sim, ops)

            print_statistics(sys_sim, name)
            results.append((name, sys_sim))

    # So sánh các workload
    if len(results) == 4:
        compare_workloads(results)

    print("\n✓ HOÀN THÀNH MÔ PHỎNG WRITE-BACK")


if __name__ == "__main__":
    main()