import argparse
import os
import time
import uuid
import importlib

def slice_log_file(raw_file_path, batch_size, temp_dir="batches"):
    """
    Splits the raw log file into sequential batch files without pre-cleaning.
    """
    os.makedirs(temp_dir, exist_ok=True)
    batch_files = []
    
    current_batch_lines = []
    batch_id = 1
    
    print(f"[ORCHESTRATOR] Slicing raw log file '{raw_file_path}' into batches of size {batch_size}...")
    
    with open(raw_file_path, 'r', encoding='latin-1') as f:
        for line in f:
            current_batch_lines.append(line)
            if len(current_batch_lines) == batch_size:
                batch_path = os.path.join(temp_dir, f"batch_{batch_id:03d}.txt")
                with open(batch_path, 'w', encoding='latin-1') as bf:
                    bf.writelines(current_batch_lines)
                batch_files.append((batch_id, batch_path))
                batch_id += 1
                current_batch_lines = []
                
        # Handle final partial batch
        if current_batch_lines:
            batch_path = os.path.join(temp_dir, f"batch_{batch_id:03d}.txt")
            with open(batch_path, 'w', encoding='latin-1') as bf:
                bf.writelines(current_batch_lines)
            batch_files.append((batch_id, batch_path))
            
    print(f"[ORCHESTRATOR] Created {len(batch_files)} batches successfully.")
    return batch_files

def main():
    parser = argparse.ArgumentParser(description="Master CLI Orchestrator for Multi-Pipeline ETL Framework")
    parser.add_argument("--backend", required=True, choices=["pig", "mapreduce", "hive", "mongo"], help="Execution pipeline backend")
    parser.add_argument("--input", required=True, help="Path to raw NASA log file")
    parser.add_argument("--batch-size", type=int, default=10000, help="Number of records per batch")
    
    args = parser.parse_args()
    
    run_id = str(uuid.uuid4())[:8]
    print(f"==================================================")
    print(f" Starting ETL Run | Pipeline: {args.backend.upper()} | Run ID: {run_id}")
    print(f"==================================================")
    
    # Runtime Rule Compliance: Start timer immediately before opening input files
    t0 = time.perf_counter()
    
    # 1. Slice raw log file into sequential batches (starting from Batch ID 1)
    batch_files = slice_log_file(args.input, args.batch_size)
    
    # 2. Dynamically load the selected backend runner module
    runner_module_map = {
        "pig": "pipelines.pig_runner",
        # "mapreduce": "pipelines.mr_runner",
        # "hive": "pipelines.hive_runner",
        # "mongo": "pipelines.mongo_runner"
    }
    
    if args.backend not in runner_module_map:
        raise NotImplementedError(f"Backend '{args.backend}' runner module not registered yet.")
        
    backend_module = importlib.import_module(runner_module_map[args.backend])
    execute_batch_func = getattr(backend_module, "execute_batch")
    
    total_records_processed = 0
    non_empty_batches = 0
    
    # 3. Iterate through sequential batches and invoke execute_batch(...)
    for batch_id, batch_path in batch_files:
        print(f"\n[ORCHESTRATOR] Processing Batch ID: {batch_id} -> {batch_path}")
        batch_stats = execute_batch_func(batch_path, batch_id, run_id)
        
        if batch_stats and batch_stats.get("total_records", 0) > 0:
            total_records_processed += batch_stats["total_records"]
            non_empty_batches += 1

    # Stop timer right after final database commit/batch execution
    t1 = time.perf_counter()
    total_runtime = t1 - t0
    
    # 4. Compute average batch size
    avg_batch_size = (total_records_processed / non_empty_batches) if non_empty_batches > 0 else 0
    
    print(f"\n==================================================")
    print(f" ETL Run Completed Successfully!")
    print(f" Pipeline Name         : {args.backend}")
    print(f" Run ID                : {run_id}")
    print(f" Total Records Processed: {total_records_processed}")
    print(f" Non-Empty Batches     : {non_empty_batches}")
    print(f" Average Batch Size    : {avg_batch_size:.2f}")
    print(f" Total Execution Runtime: {total_runtime:.4f} seconds")
    print(f"==================================================")

if __name__ == "__main__":
    main()