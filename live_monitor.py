import psutil
import time
import sys

# CONFIGURATION
TARGET_PROCESS_NAME = "sftp_puller_main"  # The name we saw in htop
LOG_FILE = "live_metrics.log"

def bytes_to_mb(b):
    return b / (1024 * 1024)

def find_target_process():
    """Finds the running process with the highest RAM usage matching the name."""
    candidates = []
    for proc in psutil.process_iter(['pid', 'name', 'cmdline', 'memory_info']):
        try:
            # Check if target name is in the command line or process name
            if proc.info['name'] and TARGET_PROCESS_NAME in proc.info['name']:
                candidates.append(proc)
            elif proc.info['cmdline'] and any(TARGET_PROCESS_NAME in arg for arg in proc.info['cmdline']):
                candidates.append(proc)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            continue

    if not candidates:
        return None

    # Return the candidate using the most RAM (The real worker, not a wrapper)
    return max(candidates, key=lambda p: p.memory_info().rss)

def monitor_live():
    print(f"🔍 Searching for process: '{TARGET_PROCESS_NAME}'...")
    proc = find_target_process()

    if not proc:
        print(f"❌ Could not find any running process named '{TARGET_PROCESS_NAME}'")
        print("   Make sure the pulling job is actually running.")
        sys.exit(1)

    print(f"✅ Attached to PID: {proc.pid} ({proc.name()})")
    print(f"📝 Logging to: {LOG_FILE}")
    print("-" * 60)

    # Initialize Counters
    net_start = psutil.net_io_counters()
    start_time = time.time()
    
    # We use system-wide network counters because checking per-process 
    # network traffic requires root access/complex kernel calls.
    # Since this server is dedicated to this job, system-wide is accurate.

    try:
        with open(LOG_FILE, "a") as f:
            f.write(f"\n=== NEW MONITORING SESSION AT {time.strftime('%Y-%m-%d %H:%M:%S')} ===\n")

        while proc.is_running():
            # 1. Get CPU & RAM (Specific to the Process)
            with proc.oneshot():
                cpu_percent = proc.cpu_percent(interval=None)
                # Get Real Memory (RSS)
                ram_mb = bytes_to_mb(proc.memory_info().rss)

            # 2. Get Network Speed (System Wide)
            net_current = psutil.net_io_counters()
            bytes_sent = net_current.bytes_sent - net_start.bytes_sent
            bytes_recv = net_current.bytes_recv - net_start.bytes_recv
            # total_traffic_mb = bytes_to_mb(bytes_sent + bytes_recv)
            total_traffic_mb = bytes_to_mb(bytes_recv)


            # Calculate Speed
            elapsed = time.time() - start_time
            if elapsed > 0:
                speed_mb_s = total_traffic_mb / elapsed
            else:
                speed_mb_s = 0.0

            # 3. Formatting
            now = time.strftime("%H:%M:%S")
            output = (
                f"[{now}] PID={proc.pid} | "
                f"CPU={cpu_percent:.1f}% | "
                f"RAM={ram_mb:.2f} MB | "
                f"Net={total_traffic_mb:.2f} MB | "
                f"Speed={speed_mb_s:.2f} MB/s"
            )

            # 4. Print & Log
            print(output)
            with open(LOG_FILE, "a") as f:
                f.write(output + "\n")

            time.sleep(1)

    except psutil.NoSuchProcess:
        print(f"\n❌ Process {proc.pid} has finished or crashed.")
    except KeyboardInterrupt:
        print(f"\n🛑 Monitoring stopped by user.")

if __name__ == "__main__":
    monitor_live()