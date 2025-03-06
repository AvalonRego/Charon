import sys
import os
sys.path.append('/u/arego/Project/Experimenting')
import gc

import Trigger_Improve as ti
import pandas as pd
import numpy as np
from itertools import combinations
from joblib import Parallel, delayed
from tqdm import tqdm

OUTPUT_DIR = "/u/arego/Project/Thesis/plot/CD"  # Base directory for storing results
COMPRESSION = "snappy"  # Parquet compression type

def process_file(file_path):
    """Processes a single HDF5 file and saves results as Parquet files in structured directories."""
    hits_data, timer_bins, _ = ti.initialize_and_load_data(file_path)
    del _
    records = pd.read_hdf(file_path, key='records', columns=['record_id', 'energy'])

    types = hits_data['type'].unique()
    all_subsets = [subset for r in range(1, len(types) + 1) for subset in combinations(types, r)]

    for subset in all_subsets:
        subset_df = hits_data[hits_data["type"].isin(subset)]
        aggregated_data = ti.process_hits(subset_df, timer_bins)
        del subset_df
        trigger_data = ti.create_trigger_data(aggregated_data)
        del aggregated_data
        plot_df = ti.aggregate_for_plotting(trigger_data)
        del trigger_data

        cols = [col for col in plot_df.columns if 'Mod' in col]
        plot_df = plot_df[['record_id'] + cols].applymap(np.sum)
        plot_df = plot_df.merge(records, on='record_id')

        # Define output directory based on subset key
        subset_name = "_".join(map(str, subset))# Convert tuple ('A', 'B') → "A_B"
        subset_dir = os.path.join(OUTPUT_DIR, subset_name)
        os.makedirs(subset_dir, exist_ok=True)  # Create directory if not exists

        # Define output file path
        file_name = os.path.basename(file_path).replace(".h5", ".parquet")
        output_path = os.path.join(subset_dir, file_name)

        # Save as Parquet with compression
        plot_df.to_parquet(output_path, index=False, compression=COMPRESSION)
        del plot_df
        gc.collect()


def process_files_in_parallel(file_paths, num_workers=4):
    """Processes multiple HDF5 files in parallel using Joblib and tqdm."""
    Parallel(n_jobs=num_workers)(
        delayed(process_file)(file) for file in tqdm(file_paths, desc="Processing Files", unit="file")
    )

if __name__=='__main__':
    # Example usage
    path='/viper/ptmp/arego/RC4K1/'
    file_paths = [os.path.join(path, file) for file in os.listdir(path) if file.endswith('.h5')]

    # Filter files by size < 1GB
    file_paths = [(file, os.path.getsize(file)) for file in file_paths if os.path.getsize(file) < 10**9]

    # Sort files by size (small to big)
    file_paths = [file for file, size in sorted(file_paths, key=lambda x: x[1])]

    results = process_files_in_parallel(file_paths, num_workers=8)  # Adjust workers as needed
