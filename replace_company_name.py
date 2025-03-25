#!/usr/bin/env python3
import os
import re
from pathlib import Path

def replace_in_file(file_path):
    """Replace all instances of 'deus' with 'deus' in a file."""
    with open(file_path, 'r', encoding='utf-8') as file:
        try:
            content = file.read()
        except UnicodeDecodeError:
            print(f"Skipping binary file: {file_path}")
            return False

    # Define replacements
    replacements = {
        'Deus': 'Deus',
        'DEUS': 'DEUS',
        'deus': 'deus',
        'Deusteam': 'Deusteam'
    }

    # Perform replacements
    modified = False
    for old, new in replacements.items():
        if old in content:
            content = content.replace(old, new)
            modified = True

    if modified:
        print(f"Modifying: {file_path}")
        with open(file_path, 'w', encoding='utf-8') as file:
            file.write(content)
        return True
    return False

def process_directory(directory):
    """Process all files in a directory recursively."""
    modified_files = []
    skipped_directories = {'.git', '__pycache__', 'node_modules', 'dist', 'build'}
    
    for root, dirs, files in os.walk(directory):
        # Skip unwanted directories
        dirs[:] = [d for d in dirs if d not in skipped_directories]
        
        for file in files:
            if file.startswith('.'):
                continue
                
            file_path = os.path.join(root, file)
            try:
                if replace_in_file(file_path):
                    modified_files.append(file_path)
            except Exception as e:
                print(f"Error processing {file_path}: {e}")

    return modified_files

def rename_files_and_directories(directory):
    """Rename files and directories containing 'deus'."""
    for root, dirs, files in os.walk(directory, topdown=False):
        # Rename files
        for file in files:
            if 'deus' in file.lower():
                old_path = os.path.join(root, file)
                new_name = file.replace('deus', 'deus').replace('Deus', 'Deus')
                new_path = os.path.join(root, new_name)
                os.rename(old_path, new_path)
                print(f"Renamed file: {old_path} -> {new_path}")

        # Rename directories
        for dir_name in dirs:
            if 'deus' in dir_name.lower():
                old_path = os.path.join(root, dir_name)
                new_name = dir_name.replace('deus', 'deus').replace('Deus', 'Deus')
                new_path = os.path.join(root, new_name)
                os.rename(old_path, new_path)
                print(f"Renamed directory: {old_path} -> {new_path}")

def main():
    workspace_dir = os.path.dirname(os.path.abspath(__file__))
    print(f"Processing workspace: {workspace_dir}")
    
    # First, replace content in files
    modified_files = process_directory(workspace_dir)
    print(f"\nModified {len(modified_files)} files")
    
    # Then, rename files and directories
    rename_files_and_directories(workspace_dir)

if __name__ == '__main__':
    main() 