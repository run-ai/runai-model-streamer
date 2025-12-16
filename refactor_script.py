#!/usr/bin/env python3
import os
import shutil
import sys

def rename_directories():
    """Rename directories from s3 to obj_store"""
    base_path = "/home/omdayan/Desktop/work/code/runai-model-streamer"
    
    dirs_to_rename = [
        ("cpp/s3", "cpp/obj_store"),
        ("cpp/streamer/impl/s3", "cpp/streamer/impl/obj_store"),
        ("py/runai_model_streamer_s3", "py/runai_model_streamer_obj_store"),
        ("py/runai_model_streamer/runai_model_streamer/s3_utils", "py/runai_model_streamer/runai_model_streamer/obj_store_utils"),
        ("tests/s3", "tests/obj_store"),
    ]
    
    for old_path, new_path in dirs_to_rename:
        old_full = os.path.join(base_path, old_path)
        new_full = os.path.join(base_path, new_path)
        
        if os.path.exists(old_full):
            print(f"Renaming {old_path} -> {new_path}")
            os.makedirs(os.path.dirname(new_full), exist_ok=True)
            shutil.move(old_full, new_full)
        else:
            print(f"Warning: {old_path} does not exist")

if __name__ == "__main__":
    rename_directories()
