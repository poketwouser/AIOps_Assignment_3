# Log docker stats (CPU %, memory) of the cluster containers every second to a CSV
# python monitor_resources.py --cluster spark --out results/resources/spark_run1.csv

import argparse
import csv
import os
import re
import signal
import subprocess
import sys
import time

CONTAINERS = {
    "spark": ["spark_master", "spark_worker_a", "spark_worker_b"],
    "ray": ["ray_head", "ray_worker_a", "ray_worker_b"],
}

UNIT_TO_MIB = {
    "B": 1 / (1024 * 1024),
    "KiB": 1 / 1024,
    "kB": 1000 / (1024 * 1024),
    "MiB": 1,
    "MB": 1000 * 1000 / (1024 * 1024),
    "GiB": 1024,
    "GB": 1000 * 1000 * 1000 / (1024 * 1024),
}

ANSI_ESCAPE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")

stop_requested = False


def handle_stop(signum, frame):
    global stop_requested
    stop_requested = True


def to_mib(text):
    match = re.match(r"([\d.]+)\s*([A-Za-z]+)", text.strip())
    number = float(match.group(1))
    unit = match.group(2)
    return number * UNIT_TO_MIB[unit]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--cluster", choices=["spark", "ray"], required=True)
    parser.add_argument("--out", required=True, help="CSV file to write")
    args = parser.parse_args()

    containers = CONTAINERS[args.cluster]
    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)

    signal.signal(signal.SIGINT, handle_stop)
    signal.signal(signal.SIGTERM, handle_stop)

    command = [
        "docker",
        "stats",
        "--format",
        "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}",
    ] + containers
    stats = subprocess.Popen(command, stdout=subprocess.PIPE, text=True, bufsize=1)

    peak_cpu = {name: 0.0 for name in containers}
    peak_mem = {name: 0.0 for name in containers}
    last_written = {name: 0.0 for name in containers}
    start = time.time()
    n_samples = 0

    with open(args.out, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "elapsed_s", "container", "cpu_percent", "mem_used_mib", "mem_limit_mib"])

        for line in stats.stdout:
            if stop_requested:
                break
            line = ANSI_ESCAPE.sub("", line).strip()
            if line.count("|") != 2:
                continue

            name, cpu_text, mem_text = line.split("|")
            if name not in peak_cpu or "--" in cpu_text:
                continue

            # keep at most one sample per container per second
            now = time.time()
            if now - last_written[name] < 0.95:
                continue
            last_written[name] = now

            cpu = float(cpu_text.rstrip("%"))
            used_text, limit_text = mem_text.split("/")
            mem_used = to_mib(used_text)
            mem_limit = to_mib(limit_text)

            writer.writerow([
                time.strftime("%H:%M:%S", time.localtime(now)),
                round(now - start, 1),
                name,
                cpu,
                round(mem_used, 1),
                round(mem_limit, 1),
            ])
            f.flush()
            n_samples += 1

            peak_cpu[name] = max(peak_cpu[name], cpu)
            peak_mem[name] = max(peak_mem[name], mem_used)

    stats.terminate()

    print(f"\nSaved {n_samples} samples to {args.out}")
    print(f"{'container':<16} {'peak CPU %':>10} {'peak mem MiB':>13}")
    for name in containers:
        print(f"{name:<16} {peak_cpu[name]:>10.1f} {peak_mem[name]:>13.1f}")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
