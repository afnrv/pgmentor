from typing import List, Dict
import os
import re
from pgmentor.db import Pg

KB = 1024
MB = 1024 * KB
GB = 1024 * MB


def to_unit(mb: int, unit: str) -> int:
    if unit == "kB":
        return mb * 1024
    if unit == "8kB":
        # В pg_settings unit="8kB" означает блоки по 8 KiB
        return mb * 1024 // 8
    return mb


def fmt_bytes(n: int) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    while n > 1024 and i < len(units) - 1:
        n //= 1024
        i += 1
    return f"{n}{units[i]}"


def read_first(path: str, default: str = "n/a") -> str:
    try:
        with open(path, "r") as f:
            return f.read().strip()
    except Exception:
        return default


def get_os() -> str:
    try:
        data = {}
        with open("/etc/os-release", "r") as f:
            for line in f:
                if "=" in line:
                    k, v = line.strip().split("=", 1)
                    data[k] = v.strip('"')
        return data.get("PRETTY_NAME", "")
    except Exception:
        return ""


def get_governor() -> str:
    vals: List[str] = []
    base = "/sys/devices/system/cpu"
    if not os.path.isdir(base):
        return ""
    for name in os.listdir(base):
        if not name.startswith("cpu"):
            continue
        path = os.path.join(base, name, "cpufreq", "scaling_governor")
        if os.path.isfile(path):
            vals.append(read_first(path))
    return " ".join(sorted(set(vals)))


def hp_info() -> str:
    info: Dict[str, str] = {}
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith(("HugePages_Total", "HugePages_Free", "Hugepagesize")):
                    k, v = line.split(":", 1)
                    info[k] = v.strip().split()[0]
    except Exception:
        pass
    return " ".join(f"{k}:{v}" for k, v in info.items())


def parse_meminfo_kb(key: str) -> int:
    try:
        with open("/proc/meminfo", "r") as f:
            for line in f:
                if line.startswith(key):
                    return int(line.split()[1])
    except Exception:
        pass
    return 0


def calc_need_hp(pg: Pg) -> int:
    hp_kb = parse_meminfo_kb("Hugepagesize:")  # in kB
    if not hp_kb:
        return 0
    sb = pg.qval("SHOW shared_buffers;") or "128MB"
    # convert shared_buffers to kB
    m = re.match(r"(\d+)(kB|MB|GB)", str(sb))
    if m:
        num = int(m.group(1))
        unit = m.group(2)
        if unit == "kB":
            sb_kb = num
        elif unit == "MB":
            sb_kb = num * 1024
        else:
            sb_kb = num * 1024 * 1024
    else:
        sb_kb = 128 * 1024
    # +8192kB как в оригинале
    need = (sb_kb + 8192 + hp_kb - 1) // hp_kb
    return int(need)


def numa_nodes() -> int:
    base = "/sys/devices/system/node"
    if not os.path.isdir(base):
        return 0
    cnt = 0
    for name in os.listdir(base):
        if re.match(r"node\d+", name):
            cnt += 1
    return cnt


def numa_bal() -> int:
    val = read_first("/proc/sys/kernel/numa_balancing", "0")
    try:
        return int(val)
    except Exception:
        return 0