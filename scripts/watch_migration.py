#!/usr/bin/env python3
"""OneDrive → GDrive 이전 진행상황 모니터링"""
import subprocess, sys, time, json, os
from datetime import datetime

HOME = os.path.expanduser("~")
CACHE = f"{HOME}/.cache/migration_watch_cache.json"

# 고정값 (OneDrive는 변하지 않으므로 캐시)
OD_TOTAL_COUNT = 133029
OD_TOTAL_BYTES = 136610716236  # 127.2 GB

def rclone_size(remote: str, timeout_sec: int = 60) -> dict:
    try:
        proc = subprocess.run(
            ["rclone", "size", remote, "--json"],
            capture_output=True, text=True, timeout=timeout_sec
        )
        if proc.returncode != 0:
            return {"count": -1, "bytes": -1}
        return json.loads(proc.stdout)
    except (subprocess.TimeoutExpired, Exception):
        return {"count": -1, "bytes": -1}

def fmt_bytes(b: int) -> str:
    if b < 0:
        return "조회불가"
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if b < 1024:
            return f"{b:.1f} {unit}"
        b /= 1024
    return f"{b:.1f} PB"

def fmt_pct(part: int, total: int) -> str:
    if total <= 0 or part < 0:
        return "   -   "
    return f"{part/total*100:.1f}%"

def get_log_tail(path: str, n: int = 5) -> list[str]:
    try:
        proc = subprocess.run(["tail", f"-{n}", path], capture_output=True, text=True, timeout=3)
        return [l.strip() for l in proc.stdout.strip().split("\n") if l.strip()]
    except Exception:
        return []

def main():
    print("\033[2J\033[H", end="")
    print(f"\033[1;36m{'='*65}\033[0m")
    print(f"\033[1;36m  OneDrive → Google Drive 이전 모니터\033[0m")
    print(f"\033[1;36m{'='*65}\033[0m")
    print()

    # rclone 프로세스 확인
    proc = subprocess.run(["pgrep", "-a", "rclone"], capture_output=True, text=True)
    running = [l for l in proc.stdout.strip().split("\n") if "copy onedrive" in l] if proc.stdout.strip() else []
    if running:
        print(f"\033[1;32m  ● rclone copy 실행 중\033[0m")
    else:
        print(f"\033[1;31m  ○ rclone copy 중지됨\033[0m")
    print()

    # GDrive 크기만 조회 (빠름)
    print("  \033[1;37mGDrive 크기 조회 중...\033[0m", end="\r")
    gd = rclone_size("gdrive:/archive/pdf", timeout_sec=30)
    print(" " * 30, end="\r")

    gd_count = gd.get("count", -1)
    gd_bytes = gd.get("bytes", -1)

    od_count = OD_TOTAL_COUNT
    od_bytes = OD_TOTAL_BYTES
    remaining_count = max(0, od_count - gd_count) if gd_count >= 0 else -1
    remaining_bytes = max(0, od_bytes - gd_bytes) if gd_bytes >= 0 else -1

    # 진행률 바
    bar_width = 30
    pct = gd_bytes / od_bytes * 100 if gd_bytes > 0 else 0
    filled = max(1, int(bar_width * pct / 100)) if pct > 0 else 0
    bar = "█" * filled + "░" * (bar_width - filled)

    # 테이블
    gd_count_str = f"{gd_count:>9,}" if gd_count >= 0 else "  조회중..."
    gd_bytes_str = f"{fmt_bytes(gd_bytes):>10}" if gd_bytes >= 0 else "  조회중..."
    rem_count_str = f"{remaining_count:>9,}" if remaining_count >= 0 else "     -"
    rem_bytes_str = f"{fmt_bytes(remaining_bytes):>10}" if remaining_bytes >= 0 else "     -"
    pct_count_str = fmt_pct(gd_count, od_count)
    pct_bytes_str = fmt_pct(gd_bytes, od_bytes)
    pct_rem_str = fmt_pct(remaining_count, od_count) if remaining_count >= 0 else "   -   "

    print(f"  ┌─────────────────────┬──────────────┬──────────────┬──────────────┐")
    print(f"  │                     │   OneDrive   │   GDrive     │   남은 양    │")
    print(f"  ├─────────────────────┼──────────────┼──────────────┼──────────────┤")
    print(f"  │ 파일 수             │  {od_count:>9,}   │  {gd_count_str}   │  {rem_count_str}   │")
    print(f"  │ 용량                │  {fmt_bytes(od_bytes):>10}   │  {gd_bytes_str}   │  {rem_bytes_str}   │")
    print(f"  │ 진행률              │              │  {pct_bytes_str:>6}     │  {pct_rem_str:>6}     │")
    print(f"  └─────────────────────┴──────────────┴──────────────┴──────────────┘")
    print()
    if gd_bytes > 0:
        print(f"  \033[1;37m[{bar}] {pct_bytes_str}\033[0m")
    print()

    # 최근 전송 로그
    for logfile in [f"{HOME}/logs/rclone_sync.log", f"{HOME}/logs/rclone_onedrive_to_gdrive.log"]:
        logs = get_log_tail(logfile, 3)
        for line in logs:
            if "Copied" in line:
                # 파일명만 추출
                parts = line.split(": ")
                if len(parts) >= 2:
                    fname = parts[-1].replace("Copied (new)", "").strip()
                    print(f"  \033[0;32m  ✓ {fname[-80:]}\033[0m")
            elif "ERROR" in line:
                print(f"  \033[0;31m  ✗ {line[-90:]}\033[0m")

    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"\n  \033[0;37m업데이트: {timestamp}  │  watch -n10 'python3 scripts/watch_migration.py'\033[0m")
    print(f"\033[1;36m{'='*65}\033[0m")

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--watch":
        try:
            while True:
                main()
                time.sleep(10)
        except KeyboardInterrupt:
            print("\n종료")
    else:
        main()
