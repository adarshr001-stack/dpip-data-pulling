import psutil
import subprocess
import time
import threading

# ========================
# CONFIG
# ========================
MILESTONE_MB = 100
MILESTONE_FILE = "milestone.log"
METRIC_FILE = "metrics.log"
# ========================

def bytes_to_mb(b):
    return b / (1024 * 1024)

def log_metric(text):
    with open(METRIC_FILE, "a") as f:
        f.write(text + "\n")

def log_milestone(text):
    with open(MILESTONE_FILE, "a") as f:
        f.write(text + "\n")

def monitor(pid, results):
    """Monitor process metrics + save milestones."""
    try:
        proc = psutil.Process(pid)
    except psutil.NoSuchProcess:
        return

    net_start = psutil.net_io_counters()
    start_time = time.time()

    # Tracking
    cpu_peak = 0
    mem_peak = 0
    cpu_sum = 0
    mem_sum = 0
    count = 0
    net_total = 0

    next_milestone = MILESTONE_MB

    try:
        while proc.is_running():

            # CPU + RAM
            cpu = proc.cpu_percent(interval=1)
            mem = proc.memory_info().rss / (1024 * 1024)

            cpu_peak = max(cpu_peak, cpu)
            mem_peak = max(mem_peak, mem)

            cpu_sum += cpu
            mem_sum += mem
            count += 1

            # Network usage
            net_current = psutil.net_io_counters()
            net_sent = net_current.bytes_sent - net_start.bytes_sent
            net_recv = net_current.bytes_recv - net_start.bytes_recv
            net_total = bytes_to_mb(net_sent + net_recv)

            now = time.strftime("%Y-%m-%d %H:%M:%S")
            elapsed = time.time() - start_time
            speed = net_total / elapsed if elapsed > 0 else 0

            # ==========================
            # Log sample
            # ==========================
            line = (
                f"[{now}] CPU={cpu:.2f}% | RAM={mem:.2f} MB | "
                f"Net={net_total:.2f} MB | Speed={speed:.2f} MB/s"
            )
            print(line)
            log_metric(line)

            # ==========================
            # RAM Milestone Logic
            # ==========================
            if mem >= next_milestone:
                avg_cpu = cpu_sum / count
                avg_mem = mem_sum / count

                milestone_line = (
                    f"[{now}] RAM={mem:.2f} MB reached milestone {next_milestone} MB | "
                    f"PeakCPU={cpu_peak:.2f}% | AvgCPU={avg_cpu:.2f}% | "
                    f"PeakRAM={mem_peak:.2f} MB | AvgRAM={avg_mem:.2f} MB | "
                    f"Network={net_total:.2f} MB | Speed={speed:.2f} MB/s"
                )

                print("📌 Milestone:", milestone_line)
                log_milestone(milestone_line)

                next_milestone += MILESTONE_MB

    except psutil.NoSuchProcess:
        pass

    finally:
        elapsed = time.time() - start_time
        results["cpu_peak"] = cpu_peak
        results["mem_peak"] = mem_peak
        results["cpu_avg"] = cpu_sum / count if count else 0
        results["mem_avg"] = mem_sum / count if count else 0
        results["net_total"] = net_total
        results["duration"] = elapsed
        results["speed"] = net_total / elapsed if elapsed > 0 else 0


def run_and_monitor(command):
    process = subprocess.Popen(
        command,
        shell=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        bufsize=1
    )

    results = {}
    t = threading.Thread(target=monitor, args=(process.pid, results))
    t.start()

    # Print cargo program output (DO NOT save to metrics.log)
    for line in process.stdout:
        print(line, end="")

    for line in process.stderr:
        print(line, end="")

    process.wait()
    t.join()

    summary = (
        "\n=== FINAL SUMMARY ===\n"
        f"Total Time:        {results['duration']:.2f} sec\n"
        f"Peak CPU Usage:    {results['cpu_peak']:.2f}%\n"
        f"Avg CPU Usage:     {results['cpu_avg']:.2f}%\n"
        f"Peak RAM Usage:    {results['mem_peak']:.2f} MB\n"
        f"Avg RAM Usage:     {results['mem_avg']:.2f} MB\n"
        f"Total Network I/O: {results['net_total']:.2f} MB\n"
        f"Avg Speed:         {results['speed']:.2f} MB/s\n"
    )

    print(summary)

    # Save summary only to metric + milestone logs
    log_metric(summary)
    log_milestone(summary)


if __name__ == "__main__":
    cmd = "caffeinate -dims cargo run --release"
    run_and_monitor(cmd)


# import psutil
# import subprocess
# import time
# import threading

# # ========================
# # CONFIG
# # ========================
# MILESTONE_MB = 100
# MILESTONE_FILE = "milestone.log"
# lo = "metrics.log"
# # ========================

# def bytes_to_mb(b):
#     return b / (1024 * 1024)

# def log_metric(text):
#     with open(METRIC_FILE, "a") as f:
#         f.write(text + "\n")

# def log_milestone(text):
#     with open(MILESTONE_FILE, "a") as f:
#         f.write(text + "\n")

# def monitor(pid, results):
#     """Monitor process metrics and log each sample + milestone."""
#     try:
#         proc = psutil.Process(pid)
#     except psutil.NoSuchProcess:
#         return

#     net_start = psutil.net_io_counters()
#     start_time = time.time()

#     # Peak + averages
#     cpu_peak = 0
#     mem_peak = 0
#     cpu_sum = 0
#     mem_sum = 0
#     count = 0
#     net_total = 0

#     # Milestone
#     next_milestone = MILESTONE_MB

#     try:
#         while proc.is_running():

#             # CPU + memory
#             cpu = proc.cpu_percent(interval=1)
#             mem = proc.memory_info().rss / (1024 * 1024)

#             cpu_peak = max(cpu_peak, cpu)
#             mem_peak = max(mem_peak, mem)

#             cpu_sum += cpu
#             mem_sum += mem
#             count += 1

#             # Network
#             net_current = psutil.net_io_counters()
#             net_sent = net_current.bytes_sent - net_start.bytes_sent
#             net_recv = net_current.bytes_recv - net_start.bytes_recv
#             net_total = bytes_to_mb(net_sent + net_recv)

#             now = time.strftime("%Y-%m-%d %H:%M:%S")
#             elapsed = time.time() - start_time
#             speed = net_total / elapsed if elapsed > 0 else 0

#             # ==============================
#             # SAVE EVERY METRIC SAMPLE
#             # ==============================
#             line = (
#                 f"[{now}] CPU={cpu:.2f}% | RAM={mem:.2f} MB | "
#                 f"Net={net_total:.2f} MB | Speed={speed:.2f} MB/s"
#             )
#             print(line)
#             log_metric(line)

#             # ==============================
#             # MEMORY MILESTONE
#             # ==============================
#             if mem >= next_milestone:
#                 avg_cpu = cpu_sum / count
#                 avg_mem = mem_sum / count

#                 milestone_line = (
#                     f"[{now}] RAM={mem:.2f} MB reached milestone {next_milestone} MB | "
#                     f"PeakCPU={cpu_peak:.2f}% | AvgCPU={avg_cpu:.2f}% | "
#                     f"PeakRAM={mem_peak:.2f} MB | AvgRAM={avg_mem:.2f} MB | "
#                     f"Network={net_total:.2f} MB | Speed={speed:.2f} MB/s"
#                 )

#                 print("📌 Milestone:", milestone_line)
#                 log_milestone(milestone_line)

#                 next_milestone += MILESTONE_MB

#     except psutil.NoSuchProcess:
#         pass
#     finally:
#         elapsed = time.time() - start_time
#         results["cpu_peak"] = cpu_peak
#         results["mem_peak"] = mem_peak
#         results["cpu_avg"] = cpu_sum / count if count else 0
#         results["mem_avg"] = mem_sum / count if count else 0
#         results["net_total"] = net_total
#         results["duration"] = elapsed
#         results["speed"] = net_total / elapsed if elapsed > 0 else 0

# def run_and_monitor(command):
#     process = subprocess.Popen(
#         command,
#         shell=True,
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#         text=True,
#         bufsize=1
#     )

#     results = {}
#     t = threading.Thread(target=monitor, args=(process.pid, results))
#     t.start()

#     # Print program output
#     for line in process.stdout:
#         print(line, end="")
#     for line in process.stderr:
#         print(line, end="")

#     process.wait()
#     t.join()

#     summary = (
#         "\n=== FINAL SUMMARY ===\n"
#         f"Total Time:        {results['duration']:.2f} sec\n"
#         f"Peak CPU Usage:    {results['cpu_peak']:.2f}%\n"
#         f"Avg CPU Usage:     {results['cpu_avg']:.2f}%\n"
#         f"Peak RAM Usage:    {results['mem_peak']:.2f} MB\n"
#         f"Avg RAM Usage:     {results['mem_avg']:.2f} MB\n"
#         f"Total Network I/O: {results['net_total']:.2f} MB\n"
#         f"Avg Speed:         {results['speed']:.2f} MB/s\n"
#     )

#     print(summary)

#     # Save summary to both files
#     log_metric(summary)
#     log_milestone(summary)

# if __name__ == "__main__":
#     cmd = "cargo run --release"
#     run_and_monitor(cmd)



# import psutil
# import subprocess
# import time
# import threading

# def bytes_to_mb(b):
#     return b / (1024 * 1024)

# def monitor(pid, results):
#     """Monitor CPU, memory, and network usage for the given PID."""
#     try:
#         proc = psutil.Process(pid)
#     except psutil.NoSuchProcess:
#         return

#     net_start = psutil.net_io_counters()
#     start_time = time.time()

#     cpu_peak = 0
#     mem_peak = 0
#     net_total = 0

#     cpu_sum = 0
#     mem_sum = 0
#     count = 0

#     try:
#         while proc.is_running():
#             # Measure CPU & memory
#             cpu = proc.cpu_percent(interval=1)
#             mem = proc.memory_info().rss / (1024 * 1024)  # MB

#             # Update peaks and sums
#             cpu_peak = max(cpu_peak, cpu)
#             mem_peak = max(mem_peak, mem)
#             cpu_sum += cpu
#             mem_sum += mem
#             count += 1

#             # Network (system-wide)
#             net_current = psutil.net_io_counters()
#             net_sent = net_current.bytes_sent - net_start.bytes_sent
#             net_recv = net_current.bytes_recv - net_start.bytes_recv
#             net_total = bytes_to_mb(net_sent + net_recv)

#             print(f"CPU: {cpu:5.2f}% | RAM: {mem:8.2f} MB | Network: {net_total:8.2f} MB")

#     except psutil.NoSuchProcess:
#         pass
#     finally:
#         end_time = time.time()
#         total_time = end_time - start_time

#         results["cpu_peak"] = cpu_peak
#         results["mem_peak"] = mem_peak
#         results["cpu_avg"] = cpu_sum / count if count else 0
#         results["mem_avg"] = mem_sum / count if count else 0
#         results["net_total"] = net_total
#         results["duration"] = total_time
#         results["speed"] = net_total / total_time if total_time > 0 else 0

# def run_and_monitor(command):
#     """Run a process and monitor its performance metrics."""
#     process = subprocess.Popen(
#         command,
#         shell=True,
#         stdout=subprocess.PIPE,
#         stderr=subprocess.PIPE,
#         text=True,
#         bufsize=1
#     )

#     results = {}
#     monitor_thread = threading.Thread(target=monitor, args=(process.pid, results))
#     monitor_thread.start()

#     # Optional: print live stdout/stderr
#     for line in process.stdout:
#         print(line, end="")
#     for line in process.stderr:
#         print(line, end="")

#     process.wait()
#     monitor_thread.join()

#     print("\n=== 📊 Performance Summary ===")
#     print(f"Total Time:        {results['duration']:.2f} sec")
#     print(f"Peak CPU Usage:    {results['cpu_peak']:.2f}%")
#     print(f"Avg CPU Usage:     {results['cpu_avg']:.2f}%")
#     print(f"Peak RAM Usage:    {results['mem_peak']:.2f} MB")
#     print(f"Avg RAM Usage:     {results['mem_avg']:.2f} MB")
#     print(f"Total Network I/O: {results['net_total']:.2f} MB")
#     print(f"Avg Transfer Speed:{results['speed']:.2f} MB/s")

# if __name__ == "__main__":
#     # Replace this with your command
#     cmd = "caffeinate -dims cargo run --release"
#     print(f"Starting process: {cmd}")
#     run_and_monitor(cmd)
