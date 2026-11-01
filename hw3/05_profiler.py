import subprocess
import time
import tracemalloc
import os


def profile_script(script_name, runs=3):
    print(f"\n--- Профилирование: {script_name} ({runs} запуска) ---")
    times = []
    mem_peaks = []

    for i in range(runs):
        tracemalloc.start()
        start_time = time.perf_counter()

        subprocess.run(["python", script_name], capture_output=True, text=True)

        end_time = time.perf_counter()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()

        times.append(end_time - start_time)
        mem_peaks.append(peak / (1024 * 1024))

    avg_time = sum(times) / len(times)
    max_mem = max(mem_peaks)

    print(f"Среднее время: {avg_time:.4f} сек")
    print(f"Пиковая память: {max_mem:.2f} MB")
    return avg_time, max_mem


if __name__ == "__main__":
    print("Запуск профилирования...")
    mr_time, mr_mem = profile_script("02_mapreduce.py")
    db_time, db_mem = profile_script("03_duckdb.py")

    print("\n=== ИТОГОВАЯ ТАБЛИЦА ДЛЯ ОТЧЕТА ===")
    print(f"{'Подход':<15} | {'Среднее время (сек)':<20} | {'Пик памяти (MB)':<15}")
    print("-" * 55)
    print(f"{'MapReduce':<15} | {mr_time:<20.4f} | {mr_mem:<15.2f}")
    print(f"{'DuckDB':<15} | {db_time:<20.4f} | {db_mem:<15.2f}")