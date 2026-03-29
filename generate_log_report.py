import os
import json
import sys
from datetime import datetime

# Handle Unicode output on Windows
if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')

p = r'C:\Users\aayus\Downloads\nyc_taxi_full_project\logs\dag_id=nyc_taxi_daily_pipeline'
dirs = os.listdir(p)
manual_dirs = sorted([d for d in dirs if 'manual' in d])

# Focus on the most recent run
target_run = manual_dirs[-1]
display_name = target_run.replace('\uf03a', ':')

output = []
output.append(f"{'='*120}")
output.append(f"LOG MESSAGE SOURCE DETAILS - NYC TAXI DAG")
output.append(f"{'='*120}")
output.append(f"Run: {display_name}")
output.append(f"Generated: {datetime.now().isoformat()}")
output.append("")

full_path = os.path.join(p, target_run)
tasks = sorted(os.listdir(full_path))

for task in tasks:
    task_name = task.replace('task_id=', '')
    output.append(f"\n{'='*120}")
    output.append(f"TASK: {task_name}")
    output.append(f"{'='*120}")
    
    task_path = os.path.join(full_path, task)
    log_files = sorted(os.listdir(task_path))
    
    for log_file in log_files:
        log_path = os.path.join(task_path, log_file)
        
        output.append(f"\n  [{log_file}]")
        output.append(f"  {'-'*110}")
        
        try:
            with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()
            
            # Parse JSON logs
            for line_num, line in enumerate(lines, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    log_entry = json.loads(line)
                    
                    # Extract key information
                    timestamp = log_entry.get('timestamp', 'N/A')
                    level = log_entry.get('level', 'INFO').upper()
                    event = log_entry.get('event', '')
                    logger = log_entry.get('logger', '')
                    filename = log_entry.get('filename', '')
                    lineno = log_entry.get('lineno', '')
                    
                    # Build source info
                    source_parts = []
                    if filename:
                        source_parts.append(f"file={filename}")
                    if lineno:
                        source_parts.append(f"line={lineno}")
                    if logger:
                        source_parts.append(f"logger={logger}")
                    
                    source_info = f"[{', '.join(source_parts)}]" if source_parts else ""
                    
                    # Format output
                    msg = f"  {timestamp} | {level:<7} | {source_info}"
                    if event:
                        msg += f" | {event[:95]}"
                    output.append(msg)
                    
                except json.JSONDecodeError:
                    # Fallback for non-JSON lines
                    output.append(f"  {line[:120]}")
                    
        except Exception as e:
            output.append(f"  Error reading log: {e}")

# Write to both stdout and file
output_text = '\n'.join(output)
print(output_text)

# Also save to file
with open('LOG_SOURCES_REPORT.txt', 'w', encoding='utf-8') as f:
    f.write(output_text)

print(f"\n{'='*120}")
print("Report saved to: LOG_SOURCES_REPORT.txt")
