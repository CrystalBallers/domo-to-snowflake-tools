import os
from pathlib import Path
import pandas as pd

def ensure_output_dir(file_path: str, create_dirs: bool = True) -> Path:
    """
    Ensure the output directory exists for a given file path.
    
    Args:
        file_path: Path to the file
        create_dirs: Whether to create directories if they don't exist
        
    Returns:
        Path object pointing to the file
    """
    path = Path(file_path)
    
    if create_dirs:
        path.parent.mkdir(parents=True, exist_ok=True)
    
    return path

def save_csv(df: pd.DataFrame, output_file: str) -> str:
    """
    Save a pandas DataFrame to a CSV file.
    
    Args:
        df: DataFrame to save
        output_file: Output file path
        
    Returns:
        Path to the saved file
    """
    # Ensure output directory exists
    output_path = ensure_output_dir(output_file, create_dirs=True)
    
    # Save the DataFrame
    df.to_csv(output_path, index=False)
    
    return str(output_path)



