import os
import json

p = r'C:\Users\aayus\Downloads\nyc_taxi_full_project\logs\dag_id=nyc_taxi_daily_pipeline'
dirs = os.listdir(p)
manual_dirs = [d for d in dirs if 'manual' in d]

for d in manual_dirs:
    print(f"\n{'='*80}")
    print(f"Run: {d}")
    print('='*80)
    full_path = os.path.join(p, d)
    tasks = sorted(os.listdir(full_path))
    print(f"Tasks ({len(tasks)}):")
    for task in tasks:
        print(f"  - {task}")
        task_path = os.path.join(full_path, task)
        if os.path.isdir(task_path):
            log_files = os.listdir(task_path)
            print(f"    Log files: {log_files}")
