import os
import pandas as pd
import numpy as np
import multiprocessing
from tqdm import tqdm

def process_file(file_path, output_dir):
    """Processes a single HDF5 file and saves the output as a .npy file."""
    try:
        with pd.HDFStore(file_path, mode='r') as store:
            hits = store["hits"]['record_id']
            records = store["records"][['record_id', 'energy']]
            
            record_counts = hits.value_counts().reset_index()
            record_counts.columns = ["record_id", "count"]
            
            merged_df = records.merge(record_counts, on="record_id", how="inner")
            result_array = merged_df[["energy", "count"]].to_numpy()
            
            output_file = os.path.join(output_dir, os.path.basename(file_path).replace(".h5", ".npy"))
            np.save(output_file, result_array)
    except Exception as e:
        print(f"Error processing {file_path}: {e}")

def process_wrapper(args):
    """Wrapper function for multiprocessing."""
    file_path, output_dir = args
    process_file(file_path, output_dir)

def process_directory(input_dir, output_dir, num_workers=8):
    """Processes all .h5 files and saves individual .npy files."""
    os.makedirs(output_dir, exist_ok=True)
    h5_files = [os.path.join(input_dir, f) for f in os.listdir(input_dir) if f.endswith(".h5")]
    
    h5_files.sort(key=lambda f: os.path.getsize(f))  # Sort files by size (smallest first)
    
    with multiprocessing.Pool(processes=min(num_workers, len(h5_files))) as pool:
        with tqdm(total=len(h5_files), desc="Processing Files") as pbar:
            for _ in pool.imap_unordered(process_wrapper, [(f, output_dir) for f in h5_files]):
                pbar.update(1)

def merge_results(output_dir, merged_file):
    """Loads all .npy files in the output directory and merges them."""
    npy_files = [os.path.join(output_dir, f) for f in os.listdir(output_dir) if f.endswith(".npy")]
    
    results = [np.load(f) for f in npy_files]
    merged_array = np.vstack(results) if results else np.empty((0, 2))
    
    np.save(merged_file, merged_array)
    print("Final Merged Shape:", merged_array.shape)

if __name__ == "__main__":
    input_dir = "/viper/ptmp/arego/R1T4K/"
    output_dir = "/u/arego/Project/Thesis/plot/TD/R1T4K"
    merged_file = "EvHRC.npy"
    
    process_directory(input_dir, output_dir, num_workers=16)
    merge_results(output_dir, merged_file)