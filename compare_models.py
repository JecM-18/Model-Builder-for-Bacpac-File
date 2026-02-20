import xml.etree.ElementTree as ET
from copy import deepcopy
import os
import sys
import shutil
import zipfile
import hashlib
import re
from datetime import datetime
import subprocess
import threading
import time

# Define namespace
NS = {'ns': 'http://schemas.microsoft.com/sqlserver/dac/Serialization/2012/02'}

def cleanup_output_dir(output_dir):
    """Clean up the output directory contents before processing."""
    print(f"[0] Cleaning up output directory: {output_dir}")
    if os.path.exists(output_dir):
        # Delete contents but not the directory itself (for Docker volumes)
        for item in os.listdir(output_dir):
            item_path = os.path.join(output_dir, item)
            try:
                if os.path.isfile(item_path) or os.path.islink(item_path):
                    os.unlink(item_path)
                elif os.path.isdir(item_path):
                    shutil.rmtree(item_path)
            except Exception as e:
                print(f"    Warning: Could not delete {item_path}: {e}")
    else:
        os.makedirs(output_dir, exist_ok=True)
    print(f"    Output directory ready")

def calculate_sha256(file_path):
    """Calculate SHA256 hash of a file."""
    sha256_hash = hashlib.sha256()
    with open(file_path, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest().upper()

def update_origin_checksum(extract_dir, model_xml_path):
    """Update the checksum in Origin.xml to match the new model.xml."""
    origin_path = os.path.join(extract_dir, "Origin.xml")
    
    if not os.path.exists(origin_path):
        print(f"    Warning: Origin.xml not found at {origin_path}")
        return False
    
    new_hash = calculate_sha256(model_xml_path)
    print(f"    New model.xml hash: {new_hash}")
    
    with open(origin_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    pattern = r'(<Checksum Uri="/model.xml">)[A-Fa-f0-9]+(</Checksum>)'
    replacement = f'\\g<1>{new_hash}\\g<2>'
    
    new_content, count = re.subn(pattern, replacement, content)
    
    if count > 0:
        with open(origin_path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"    Updated Origin.xml checksum")
        return True
    else:
        print(f"    Warning: Could not find checksum pattern in Origin.xml")
        return False

def extract_bacpac(bacpac_path, output_dir=None):
    """Extract .bacpac file and return path to model.xml."""
    if not os.path.exists(bacpac_path):
        print(f"Error: {bacpac_path} not found")
        return None, None
    
    if output_dir is None:
        output_dir = "./output"
    
    os.makedirs(output_dir, exist_ok=True)
    
    bacpac_basename = os.path.basename(bacpac_path)
    extract_dir = os.path.join(output_dir, os.path.splitext(bacpac_basename)[0] + "_extracted")
    os.makedirs(extract_dir, exist_ok=True)
    
    print(f"[1] Extracting {bacpac_path}...")
    
    try:
        with zipfile.ZipFile(bacpac_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        print(f"    Extracted to: {extract_dir}")
        
        model_xml_path = os.path.join(extract_dir, "model.xml")
        if os.path.exists(model_xml_path):
            return model_xml_path, extract_dir
        
        for root, dirs, files in os.walk(extract_dir):
            if "model.xml" in files:
                return os.path.join(root, "model.xml"), extract_dir
        
        print("    Warning: model.xml not found")
        return None, extract_dir
            
    except zipfile.BadZipFile:
        print(f"    Error: Invalid bacpac file")
        return None, None

def parse_model(filepath):
    """Parse the XML model file."""
    tree = ET.parse(filepath)
    root = tree.getroot()
    return tree, root

def is_backup_table(table_name):
    """Check if table is a backup table or should be excluded."""
    # Exclude backup tables
    backup_patterns = [
        r'_BK_\d',           # _BK_ followed by date
        r'_SL_BK_\d',        # _SL_BK_ followed by date
        r'_\d{8}-\d{6}',     # Date pattern like _01052026-030119
        r'_\d{8}_\d{6}',     # Date pattern like _05202025_100415
    ]
    for pattern in backup_patterns:
        if re.search(pattern, table_name):
            return True
    
    # Exclude HangFire tables (they can cause FK constraint issues)
    if '[HangFire]' in table_name or table_name.startswith('[HangFire].'):
        return True
    
    return False

def get_tables_with_columns(root, exclude_backups=True):
    """Extract all SQL tables and their columns."""
    tables = {}
    for element in root.findall('.//ns:Element[@Type="SqlTable"]', NS):
        table_name = element.get('Name')
        if table_name:
            # Skip backup tables if requested
            if exclude_backups and is_backup_table(table_name):
                continue
            
            columns = []
            for col_entry in element.findall('.//ns:Relationship[@Name="Columns"]/ns:Entry/ns:Element', NS):
                col_name = col_entry.get('Name')
                if col_name:
                    columns.append({'name': col_name, 'element': col_entry})
            tables[table_name] = {'element': element, 'columns': columns}
    return tables

def get_all_elements_by_type(root, element_type):
    """Get all elements of a specific type."""
    elements = {}
    for element in root.findall(f'.//ns:Element[@Type="{element_type}"]', NS):
        name = element.get('Name')
        if name:
            elements[name] = element
    return elements

def generate_report(tables1, tables2):
    """Generate comparison report."""
    print("\n" + "="*60)
    print("COMPARISON REPORT")
    print("="*60)
    
    missing_tables = sorted(set(tables2.keys()) - set(tables1.keys()))
    extra_tables = sorted(set(tables1.keys()) - set(tables2.keys()))
    
    print(f"\n[TABLES MISSING IN model.XML] ({len(missing_tables)})")
    for t in missing_tables[:20]:
        print(f"  + {t}")
    if len(missing_tables) > 20:
        print(f"  ... and {len(missing_tables) - 20} more")
    
    print(f"\n[TABLES ONLY IN model.XML] ({len(extra_tables)})")
    for t in extra_tables[:20]:
        print(f"  - {t}")
    if len(extra_tables) > 20:
        print(f"  ... and {len(extra_tables) - 20} more")
    
    missing_cols = []
    for table_name in sorted(set(tables1.keys()) & set(tables2.keys())):
        cols1 = {col['name'] for col in tables1[table_name]['columns']}
        cols2 = {col['name'] for col in tables2[table_name]['columns']}
        diff = cols2 - cols1
        if diff:
            missing_cols.append((table_name, len(diff)))
    
    print(f"\n[COLUMN DIFFERENCES] ({len(missing_cols)} tables)")
    for t, count in missing_cols[:10]:
        print(f"  {t}: +{count} columns")
    
    return missing_tables, missing_cols

def merge_models(base_tree, base_root, new_root):
    """Merge missing elements from new model into base model."""
    model_element = base_root.find('ns:Model', NS)
    if model_element is None:
        print("Error: Could not find Model element")
        return 0, 0, []
    
    tables1 = get_tables_with_columns(base_root, exclude_backups=True)
    tables2 = get_tables_with_columns(new_root, exclude_backups=True)
    
    added_tables = 0
    added_columns = 0
    added_columns_list = []
    
    for table_name, table_data in tables2.items():
        if table_name not in tables1:
            model_element.append(deepcopy(table_data['element']))
            added_tables += 1
        else:
            existing_col_names = {col['name'] for col in tables1[table_name]['columns']}
            for col in table_data['columns']:
                if col['name'] not in existing_col_names:
                    columns_rel = tables1[table_name]['element'].find('ns:Relationship[@Name="Columns"]', NS)
                    if columns_rel is not None:
                        new_entry = ET.SubElement(columns_rel, '{http://schemas.microsoft.com/sqlserver/dac/Serialization/2012/02}Entry')
                        new_entry.append(deepcopy(col['element']))
                        added_columns += 1
                        added_columns_list.append(f"{table_name}.{col['name']}")
    
    for elem_type in ['SqlIndex', 'SqlPrimaryKeyConstraint', 'SqlForeignKeyConstraint', 'SqlDefaultConstraint', 'SqlView', 'SqlProcedure']:
        elements1 = get_all_elements_by_type(base_root, elem_type)
        elements2 = get_all_elements_by_type(new_root, elem_type)
        for elem_name, elem in elements2.items():
            if elem_name not in elements1:
                model_element.append(deepcopy(elem))
    
    return added_tables, added_columns, added_columns_list

def export_bacpac_from_azure(server, database, username, password, output_path):
    """Export bacpac from Azure SQL Database using SqlPackage with progress indicator."""
    print("="*60)
    print("EXPORTING BACPAC FROM AZURE")
    print("="*60)
    
    # Ensure output directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    # Delete existing bacpac file if it exists
    if os.path.exists(output_path):
        print(f"[*] Removing existing bacpac: {output_path}")
        os.remove(output_path)
    
    # Build connection string
    connection_string = (
        f"Server=tcp:{server},1433;"
        f"Initial Catalog={database};"
        f"Persist Security Info=False;"
        f"User ID={username};"
        f"Password={password};"
        f"MultipleActiveResultSets=False;"
        f"Encrypt=True;"
        f"TrustServerCertificate=False;"
        f"Connection Timeout=30;"
    )
    
    # SqlPackage command
    cmd = [
        "SqlPackage",
        "/Action:Export",
        f"/TargetFile:{output_path}",
        f"/SourceConnectionString:{connection_string}",
        "/p:CommandTimeout=600",
        "/p:VerifyExtraction=False"
    ]
    
    print(f"\n[*] Exporting from: {server}/{database}")
    print(f"[*] Output: {output_path}")
    print(f"[*] This may take several minutes...\n")
    
    # Progress bar state
    progress = {"running": True, "phase": "Connecting", "elapsed": 0}
    
    def progress_bar():
        """Display animated progress bar."""
        phases = [
            "Connecting to database",
            "Analyzing schema",
            "Exporting tables",
            "Exporting data",
            "Compressing bacpac",
            "Finalizing"
        ]
        phase_idx = 0
        spinner = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏']
        spinner_idx = 0
        start_time = time.time()
        
        while progress["running"]:
            elapsed = int(time.time() - start_time)
            progress["elapsed"] = elapsed
            
            # Change phase every 30 seconds for visual feedback
            phase_idx = min(elapsed // 30, len(phases) - 1)
            current_phase = phases[phase_idx]
            
            # Create progress bar
            bar_width = 30
            filled = int((elapsed % 60) / 60 * bar_width)
            bar = '█' * filled + '░' * (bar_width - filled)
            
            # Format elapsed time
            mins, secs = divmod(elapsed, 60)
            time_str = f"{mins:02d}:{secs:02d}"
            
            # Print progress
            sys.stdout.write(f"\r{spinner[spinner_idx]} [{bar}] {time_str} | {current_phase}...")
            sys.stdout.flush()
            
            spinner_idx = (spinner_idx + 1) % len(spinner)
            time.sleep(0.1)
        
        # Clear the progress line
        sys.stdout.write("\r" + " " * 80 + "\r")
        sys.stdout.flush()
    
    # Start progress bar in background thread
    progress_thread = threading.Thread(target=progress_bar, daemon=True)
    progress_thread.start()
    
    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True
            # timeout removed
        )
        
        # Stop progress bar
        progress["running"] = False
        progress_thread.join(timeout=1)
        
        elapsed = progress["elapsed"]
        mins, secs = divmod(elapsed, 60)
        
        if result.returncode == 0:
            file_size = os.path.getsize(output_path) / (1024*1024)
            print(f"\n[✓] Export successful!")
            print(f"    Time: {mins}m {secs}s")
            print(f"    File size: {file_size:.2f} MB")
            return True
        else:
            print(f"\n[✗] Export failed!")
            print(f"    Time: {mins}m {secs}s")
            print(f"    Error: {result.stderr}")
            return False
            
    except FileNotFoundError:
        progress["running"] = False
        progress_thread.join(timeout=1)
        print(f"\n[✗] SqlPackage not found. Please install it or add to PATH.")
        print(f"    Download: https://docs.microsoft.com/en-us/sql/tools/sqlpackage-download")
        return False
    except Exception as e:
        progress["running"] = False
        progress_thread.join(timeout=1)
        print(f"\n[✗] Export error: {e}")
        return False

def process_bacpac(bacpac_path, base_model_path, output_dir=None):
    """
    Main function: Extract bacpac, compare with base model, merge, update checksum, and repackage.
    """
    print("="*60)
    print("BACPAC MODEL SYNC TOOL")
    print("="*60)
    
    if output_dir is None:
        output_dir = "./output"
    
    # Step 0: Cleanup output directory
    cleanup_output_dir(output_dir)
    
    # Step 1: Extract bacpac
    extracted_model, extract_dir = extract_bacpac(bacpac_path, output_dir)
    if not extracted_model:
        print("Failed to extract bacpac")
        return False
    
    # Step 2: Parse both models
    print(f"\n[2] Parsing models...")
    print(f"    Base model: {base_model_path}")
    print(f"    Bacpac model: {extracted_model}")
    
    if not os.path.exists(base_model_path):
        print(f"    Error: Base model not found: {base_model_path}")
        return False
    
    base_tree, base_root = parse_model(base_model_path)
    bacpac_tree, bacpac_root = parse_model(extracted_model)
    
    # Step 3: Compare and generate report
    print(f"\n[3] Comparing models...")
    tables_base = get_tables_with_columns(base_root)
    tables_bacpac = get_tables_with_columns(bacpac_root)
    generate_report(tables_base, tables_bacpac)
    
    # Step 4: Merge missing elements into base model
    print(f"\n[4] Merging models...")
    added_tables, added_columns, added_columns_list = merge_models(base_tree, base_root, bacpac_root)
    print(f"    Added {added_tables} tables, {added_columns} columns")
    
    if added_columns_list:
        print(f"\n    [COLUMNS ADDED]")
        for col in added_columns_list:
            print(f"      + {col}")
    
    # Step 5: Save merged model
    print(f"\n[5] Saving merged model...")
    ET.register_namespace('', 'http://schemas.microsoft.com/sqlserver/dac/Serialization/2012/02')
    
    merged_output = os.path.join(output_dir, 'model_merged.xml')
    base_tree.write(merged_output, encoding='utf-8', xml_declaration=True)
    print(f"    Saved to: {merged_output}")
    
    # Step 6: Replace model in extracted bacpac
    print(f"\n[6] Replacing model in extracted bacpac...")
    shutil.copy2(merged_output, extracted_model)
    print(f"    Replaced: {extracted_model}")
    
    # Step 7: Update Origin.xml checksum
    print(f"\n[7] Updating Origin.xml checksum...")
    update_origin_checksum(extract_dir, extracted_model)
    
    # Step 7b: Clean HangFire data to avoid FK issues - MAKE SURE THIS IS CALLED
    clean_hangfire_data(extract_dir)
    
    # Step 8: Repackage bacpac
    print(f"\n[8] Repackaging bacpac...")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    bacpac_name = os.path.splitext(os.path.basename(bacpac_path))[0]
    new_bacpac_path = os.path.join(output_dir, f"{bacpac_name}_updated_{timestamp}.bacpac")
    
    with zipfile.ZipFile(new_bacpac_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, extract_dir)
                zipf.write(file_path, arcname)
    
    print(f"    Created: {new_bacpac_path}")
    
    print("\n" + "="*60)
    print("COMPLETED SUCCESSFULLY")
    print("="*60)
    
    return True

def clean_hangfire_data(extract_dir):
    """Remove HangFire data files/directories to avoid FK constraint issues during import."""
    print(f"\n[7b] Cleaning HangFire data files...")
    data_dir = os.path.join(extract_dir, "Data")
    if os.path.exists(data_dir):
        removed = 0
        for item in os.listdir(data_dir):
            # Remove ALL HangFire related data files/directories
            if item.startswith("HangFire.") or "HangFire" in item:
                item_path = os.path.join(data_dir, item)
                if os.path.isdir(item_path):
                    shutil.rmtree(item_path)
                else:
                    os.remove(item_path)
                print(f"    Removed: {item}")
                removed += 1
        print(f"    Total removed: {removed} HangFire data items")
    else:
        print(f"    No Data directory found")

def main():
    # Azure connection settings from environment
    azure_server = os.environ.get('AZURE_SERVER', '')
    azure_database = os.environ.get('AZURE_DATABASE', '')
    azure_username = os.environ.get('AZURE_USERNAME', '')
    azure_password = os.environ.get('AZURE_PASSWORD', '')
    auto_export = os.environ.get('AUTO_EXPORT', 'false').lower() == 'true'
    
    bacpac_file = os.environ.get('BACPAC_FILE', '')
    model_file = os.environ.get('MODEL_FILE', 'model.xml')
    output_dir = os.environ.get('OUTPUT_DIR', './output')
    bacpac_dir = os.environ.get('BACPAC_DIR', './bacpac')
    
    print(f"AUTO_EXPORT: {auto_export}")
    print(f"AZURE_SERVER: {azure_server}")
    print(f"AZURE_DATABASE: {azure_database}")
    print(f"BACPAC_FILE: {bacpac_file}")
    print(f"MODEL_FILE: {model_file}")
    print(f"OUTPUT_DIR: {output_dir}")
    
    # Auto export from Azure if enabled
    if auto_export and azure_server and azure_database and azure_username and azure_password:
        bacpac_file = os.path.join(bacpac_dir, "contractregistry.bacpac")
        export_success = export_bacpac_from_azure(
            azure_server,
            azure_database,
            azure_username,
            azure_password,
            bacpac_file
        )
        if not export_success:
            print("Failed to export bacpac from Azure. Exiting.")
            return
    
    if bacpac_file:
        process_bacpac(bacpac_file, model_file, output_dir)
    else:
        print("\nEnter paths:")
        bacpac_path = input("Bacpac file path: ").strip()
        model_path = input("Base model.xml path [model.xml]: ").strip() or "model.xml"
        process_bacpac(bacpac_path, model_path, output_dir)

if __name__ == "__main__":
    main()
