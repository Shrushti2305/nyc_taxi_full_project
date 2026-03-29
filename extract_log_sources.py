import os
import re
import sys

# Handle Unicode output on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

p = r'C:\Users\aayus\Downloads\nyc_taxi_full_project\logs\dag_id=nyc_taxi_daily_pipeline'
dirs = os.listdir(p)
manual_dirs = sorted([d for d in dirs if 'manual' in d])

# Focus on the most recent run
target_run = manual_dirs[-1]  # Last manual run
# Clean display name
display_name = target_run.replace('\uf03a', ':')
print(f"\nAnalyzing run: {display_name}\n")
print("="*100)

full_path = os.path.join(p, target_run)
tasks = sorted(os.listdir(full_path))

for task in tasks:
    task_name = task.replace('task_id=', '')
    print(f"\n{'='*100}")
    print(f"TASK: {task_name}")
    print('='*100)
    
    task_path = os.path.join(full_path, task)
    log_files = sorted(os.listdir(task_path))
    
    for log_file in log_files:
        log_path = os.path.join(task_path, log_file)
        print(f"\n{log_file}:")
        print("-" * 100)
        
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
            # Find all source= patterns
            sources = re.findall(r'source[s]?\s*=\s*\[([^\]]+)\]', content)
            
            if sources:
                for i, source in enumerate(sources, 1):
                    print(f"  Source #{i}: {source}")
            else:
                # Look for any source-related information  
                info_lines = [line for line in content.split('\n') if 'source' in line.lower()]
                if info_lines:
                    for line in info_lines[:10]:  # First 10 matches
                        if line.strip():
                            print(f"  {line.strip()[:150]}")
                else:
                    # Show key log entries
                    lines = content.split('\n')
                    key_lines = [l for l in lines if any(x in l for x in ['ERROR', 'INFO', 'WARNING', 'FAILED', 'SUCCESS', 'Bash', 'dbt'])]
                    if key_lines:
                        for line in key_lines[:10]:
                            if line.strip():
                                print(f"  {line.strip()[:150]}")
        except Exception as e:
            print(f"  Error reading log: {e}")
