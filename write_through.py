import time
import random
import os

# Ghi trực tiếp (Write-Through) + LRU Eviction
# Ghi đồng thời vào cache và HDD
# ============================================================================

# ============================================================================
# 1. THAM SỐ CẤU HÌNH HỆ THỐNG
# ============================================================================
BLOCK_SIZE = 4096
HDD_CAPACITY = 10000
HDD_READ_LATENCY = 8
HDD_WRITE_LATENCY = 10
CACHE_SIZE = 128
SSD_READ_LATENCY = 0.1
SSD_WRITE_LATENCY = 0.2


# ============================================================================
# 2. CẤU TRÚC DỮ LIỆU
# ============================================================================
class CacheEntry:
    def __init__(self):
        self.blockID = -1
        self.data = 0
        self.timestamp = 0
        self.valid = False


class HDDEntry:
    def __init__(self, blockID):
        self.blockID = blockID
        self.data = 0


class StorageSystem:
    def __init__(self):
        self.ssdCache = [CacheEntry() for _ in range(CACHE_SIZE)]
        self.hdd = [HDDEntry(i) for i in range(HDD_CAPACITY)]
        self.cacheHits = 0
        self.cacheMisses = 0
        self.hddReadCount = 0  # Số lần truy cập HDD khi read
        self.hddWriteCount = 0  # Số lần truy cập HDD khi write
        self.totalReadTime = 0.0  # Tổng thời gian read
        self.totalWriteTime = 0.0  # Tổng thời gian write
        self.currentTime = 0

    def tick(self):
        self.currentTime += 1


# ============================================================================
# 3. HÀM TÌM KIẾM VÀ QUẢN LÝ CACHE
# ============================================================================
def find_in_cache(system, blockID):
    """Tìm block trong cache, trả về index hoặc -1"""
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
    
    # Bước 2: Cache đầy → Tìm LRU
    for i in range(CACHE_SIZE):
        if system.ssdCache[i].timestamp < min_timestamp:
            min_timestamp = system.ssdCache[i].timestamp
            victim_index = i
    
    return victim_index


def load_to_cache(system, blockID, cache_index):
    """Load block từ HDD vào cache"""
    data = system.hdd[blockID].data
    system.ssdCache[cache_index].blockID = blockID
    system.ssdCache[cache_index].data = data
    system.ssdCache[cache_index].timestamp = system.currentTime
    system.ssdCache[cache_index].valid = True


# ============================================================================
# 4. HÀM ĐỌC/GHI WRITE-THROUGH
# ============================================================================
def cache_read(system, blockID):
    """Đọc block từ cache (Write-Through policy)"""
    system.tick()
    cache_index = find_in_cache(system, blockID)
    
    if cache_index != -1:
        # ===== CACHE HIT =====
        system.cacheHits += 1
        system.ssdCache[cache_index].timestamp = system.currentTime
        
        latency = SSD_READ_LATENCY  # 0.1ms
        system.totalReadTime += latency
        
        return system.ssdCache[cache_index].data, latency
    
    else:
        # ===== CACHE MISS =====
        system.cacheMisses += 1
        system.hddReadCount += 1  # Đếm truy cập HDD
        
        latency = HDD_READ_LATENCY  # 8ms
        system.totalReadTime += latency
        
        data = system.hdd[blockID].data
        victim_index = find_lru_victim(system)
        load_to_cache(system, blockID, victim_index)
        
        return data, latency


def cache_write_through(system, blockID, new_data):
    """
    Ghi block vào cache và HDD (Write-Through policy)
    
    """
    system.tick()
    total_latency = 0.0
    cache_index = find_in_cache(system, blockID)

    if cache_index != -1:
        # ===== WRITE HIT =====
        # Cập nhật cache
        system.ssdCache[cache_index].data = new_data
        system.ssdCache[cache_index].timestamp = system.currentTime
        
        ssd_latency = SSD_WRITE_LATENCY  # 0.2ms
        total_latency += ssd_latency
        
    else:
        # ===== WRITE MISS =====
        system.hddReadCount += 1
        
        # Load block vào cache
        victim_index = find_lru_victim(system)
        load_to_cache(system, blockID, victim_index)
        
        # Cập nhật data mới
        system.ssdCache[victim_index].data = new_data
        system.ssdCache[victim_index].timestamp = system.currentTime
        
        ssd_latency = SSD_WRITE_LATENCY  # 0.2ms
        total_latency += ssd_latency

    # [WRITE-THROUGH KEY] Ghi xuống HDD ngay lập tức
    system.hdd[blockID].data = new_data
    hdd_latency = HDD_WRITE_LATENCY  # 10ms
    total_latency += hdd_latency
    system.hddWriteCount += 1  # Đếm truy cập HDD khi write
    
    system.totalWriteTime += total_latency
    return total_latency


# ============================================================================
# 5. ĐỌC VÀ THỰC THI WORKLOAD
# ============================================================================
def parse_workload(filename):
    """Đọc file workload và parse thành list operations"""
    operations = []
    try:
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                content = f.readlines()
        except UnicodeDecodeError:
            with open(filename, 'r', encoding='cp1252') as f:
                content = f.readlines()
        
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
            cache_write_through(system, blockID, value)
        elif op == 'F':
            # Write-Through không cần flush (đã ghi HDD ngay)
            pass


# ============================================================================
# 6. SINH WORKLOAD
# ============================================================================
def generate_random_workload(filename, num_ops=100):
    """Tạo workload random access"""
    ops = []
    for _ in range(num_ops):
        if random.choice(['R', 'W']) == 'R':
            ops.append(f"R {random.randint(0, 200)}")
        else:
            ops.append(f"W {random.randint(0, 200)} {random.randint(1, 1000)}")
    ops.extend(["F", "S"])
    
    with open(filename, 'w') as f:
        f.write("# Random Workload\n" + '\n'.join(ops))
    print(f"✓ Tạo workload random: {filename}")


def generate_sequential_workload(filename, num_ops=150):
    """Tạo workload sequential access"""
    ops = []
    for i in range(num_ops):
        if random.random() < 0.75:
            ops.append(f"R {i}")
        else:
            ops.append(f"W {i} {random.randint(1, 1000)}")
    ops.extend(["F", "S"])
    
    with open(filename, 'w') as f:
        f.write("# Sequential Workload\n" + '\n'.join(ops))
    print(f"✓ Tạo workload sequential: {filename}")


def generate_locality_workload(filename, num_ops=100):
    """Tạo workload với tính cục bộ cao"""
    hot_blocks = list(range(10, 110, 5))
    ops = []
    
    for _ in range(num_ops):
        block = random.choice(hot_blocks) if random.random() < 0.8 else random.randint(0, 200)
        if random.random() < 0.67:
            ops.append(f"R {block}")
        else:
            ops.append(f"W {block} {random.randint(1, 1000)}")
    ops.extend(["F", "S"])
    
    with open(filename, 'w') as f:
        f.write("# Locality Workload\n" + '\n'.join(ops))
    print(f"✓ Tạo workload locality: {filename}")


def generate_write_heavy_workload(filename, num_ops=100):
    """Tạo workload write-heavy"""
    hot_blocks = list(range(10, 110, 5))
    ops = []
    
    for _ in range(num_ops):
        block = random.choice(hot_blocks) if random.random() < 0.8 else random.randint(0, 200)
        if random.random() < 0.3:  # 30% read, 70% write
            ops.append(f"R {block}")
        else:
            ops.append(f"W {block} {random.randint(1, 1000)}")
    ops.extend(["F", "S"])
    
    with open(filename, 'w') as f:
        f.write("# Write-Heavy Workload\n" + '\n'.join(ops))
    print(f"✓ Tạo workload write-heavy: {filename}")


# ============================================================================
# 7. HIỂN THỊ THỐNG KÊ
# ============================================================================
def print_statistics(system, name):
    """In thống kê chi tiết cho một workload"""
    total_access = system.cacheHits + system.cacheMisses
    hit_rate = (system.cacheHits / total_access * 100) if total_access > 0 else 0
    miss_rate = 100 - hit_rate
    total_time = system.totalReadTime + system.totalWriteTime

    print(f"\n{'=' * 70}")
    print(f"THỐNG KÊ WRITE-THROUGH: {name}")
    print(f"{'=' * 70}")
    print(f"  Hit rate:                    {hit_rate:.2f}%")
    print(f"  Miss rate:                   {miss_rate:.2f}%")
    print(f"  Số lần truy cập HDD (read):  {system.hddReadCount:,}")
    print(f"  Số lần truy cập HDD (write): {system.hddWriteCount:,}")
    print(f"  Thời gian read:              {system.totalReadTime:.2f} ms")
    print(f"  Thời gian write:             {system.totalWriteTime:.2f} ms")
    print(f"  Tổng thời gian xử lý:        {total_time:.2f} ms")
    print(f"{'=' * 70}")


def compare_four_workloads(results):
    """So sánh kết quả của 4 workloads"""
    print(f"\n{'=' * 110}")
    print("BẢNG SO SÁNH 4 LOẠI WORKLOAD (WRITE-THROUGH)")
    print(f"{'=' * 110}")
    
    print(f"\n{'Chỉ số':<35} {'Random':<18} {'Sequential':<18} {'Locality':<18} {'Write-Heavy':<18}")
    print("-" * 110)

    systems = [r[1] for r in results]

    # Hit Rate
    hrs = [(s.cacheHits / (s.cacheHits + s.cacheMisses) * 100) if (s.cacheHits + s.cacheMisses) > 0 else 0 
           for s in systems]
    print(f"{'Hit Rate (%)':<35} {hrs[0]:>6.2f}%          {hrs[1]:>6.2f}%          {hrs[2]:>6.2f}%          {hrs[3]:>6.2f}%")

    # Miss Rate
    mrs = [100 - hr for hr in hrs]
    print(f"{'Miss Rate (%)':<35} {mrs[0]:>6.2f}%          {mrs[1]:>6.2f}%          {mrs[2]:>6.2f}%          {mrs[3]:>6.2f}%")

    # HDD Read Count
    hdd_reads = [s.hddReadCount for s in systems]
    print(f"{'Số lần truy cập HDD (Read)':<35} {hdd_reads[0]:>7}          {hdd_reads[1]:>7}          {hdd_reads[2]:>7}          {hdd_reads[3]:>7}")

    # HDD Write Count
    hdd_writes = [s.hddWriteCount for s in systems]
    print(f"{'Số lần truy cập HDD (Write)':<35} {hdd_writes[0]:>7}          {hdd_writes[1]:>7}          {hdd_writes[2]:>7}          {hdd_writes[3]:>7}")

    # Read Time
    read_times = [s.totalReadTime for s in systems]
    print(f"{'Thời gian Read (ms)':<35} {read_times[0]:>10.2f}      {read_times[1]:>10.2f}      {read_times[2]:>10.2f}      {read_times[3]:>10.2f}")

    # Write Time
    write_times = [s.totalWriteTime for s in systems]
    print(f"{'Thời gian Write (ms)':<35} {write_times[0]:>10.2f}      {write_times[1]:>10.2f}      {write_times[2]:>10.2f}      {write_times[3]:>10.2f}")

    # Total Time
    total_times = [s.totalReadTime + s.totalWriteTime for s in systems]
    print(f"{'Tổng thời gian xử lý (ms)':<35} {total_times[0]:>10.2f}      {total_times[1]:>10.2f}      {total_times[2]:>10.2f}      {total_times[3]:>10.2f}")

    print(f"\n{'=' * 110}")


# ============================================================================
# 8. CHƯƠNG TRÌNH CHÍNH
# ============================================================================
def main():
    random.seed(42)  # Đảm bảo kết quả lặp lại được
    
    print("=" * 70)
    print("MÔ PHỎNG HỆ THỐNG SSD CACHE + HDD")
    print("Chính sách: WRITE-THROUGH | Thay thế: LRU")
    print("=" * 70)
    print(f"\nCẤU HÌNH:")
    print(f"  • Cache: {CACHE_SIZE} blocks ({CACHE_SIZE * BLOCK_SIZE // 1024} KB)")
    print(f"  • HDD: {HDD_CAPACITY:,} blocks ({HDD_CAPACITY * BLOCK_SIZE // 1024 // 1024} MB)")
    print(f"  • SSD Read/Write: {SSD_READ_LATENCY}/{SSD_WRITE_LATENCY} ms")
    print(f"  • HDD Read/Write: {HDD_READ_LATENCY}/{HDD_WRITE_LATENCY} ms")

    # Sinh 4 loại workload
    print("\n" + "=" * 70)
    print("SINH 4 LOẠI WORKLOAD")
    print("=" * 70)
    generate_random_workload("workload_random.txt", 100)
    generate_sequential_workload("workload_sequential.txt", 150)
    generate_locality_workload("workload_locality.txt", 100)
    generate_write_heavy_workload("workload_write_heavy.txt", 100)

    # Chạy 4 test cases
    results = []

    configs = [
        ("Random", "workload_random.txt"),
        ("Sequential", "workload_sequential.txt"),
        ("Locality", "workload_locality.txt"),
        ("Write-Heavy", "workload_write_heavy.txt")
    ]

    for name, filename in configs:
        print(f"\n{'=' * 70}")
        print(f"CHẠY TEST: {name.upper()}")
        print("=" * 70)
        
        if not os.path.exists(filename):
            print(f"Không tìm thấy file {filename}")
            continue
        
        sys = StorageSystem()
        ops = parse_workload(filename)
        
        if ops:
            execute_workload(sys, ops)
            print_statistics(sys, name)
            results.append((name, sys))

    # So sánh 4 workloads
    if len(results) == 4:
        compare_four_workloads(results)

    print("\n✓ HOÀN THÀNH MÔ PHỎNG WRITE-THROUGH")


if __name__ == "__main__":
    main()