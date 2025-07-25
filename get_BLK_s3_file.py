import os
import shutil
import re
import csv
import boto3
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import time
from botocore.exceptions import ClientError, NoCredentialsError

# =============================================================================
# CONFIGURATION - MODIFY THESE PATHS AS NEEDED
# =============================================================================
BASE_PATH = r"D:\AUTOTEKX\Blackline"
OUTPUT_FOLDER = r"D:\AUTOTEKX\Blackline\PROCESSED_FURTHER_SKUS_23_07"
CSV_OUTPUT_PATH = r"D:\AUTOTEKX\Blackline\skus_image_links-23-07_2i.csv"

# S3 Configuration
S3_BUCKET_NAME = "gridximges"
S3_FOLDER = "blackline"
S3_REGION = "me-central-1"

# Processing Configuration
MAX_WORKERS = 8  # Number of concurrent uploads (adjust based on your internet speed)

# =============================================================================

# Updated folder names (SKUs) for processing – ALL CAPS, duplicates kept, order preserved
TARGET_SKUS = [
    "BU103", "BU107", "BU111", "BU111", "BU112", "BU126", "BU148", "BU150", "BU152",
    "BU154", "BU163", "BU170", "BU171", "BU174", "BU175", "BU177", "BU178", "BU179",
    "BU180", "BU182", "BU183", "BU184", "BU190", "BU193", "BU198", "BU314", "BU389",
    "BU496", "BU502", "BU514", "BU515", "BU527", "BU538", "BU541", "BU546", "BU548",
    "BU554", "BU559", "BU563", "BU576", "BU585", "BU588", "BU589", "BU431", "BU430",
    "BU380", "BU460", "BU475", "BU310", "BU428", "BU428", "BU401", "BU146", "BU406",
    "BU324", "BU435", "BLKU375", "BLKU371", "BU521", "BLKU500", "BLKU451", "BLKU315",
    "BLKU466", "BLKU456", "BU473", "BU572", "BU571", "BU476", "BLKU388", "BU443",
    "BU474", "BU484", "BU459", "BU450", "BU523"
]

# Thread-safe counters
class Counters:
    def __init__(self):
        self.processed_files = 0
        self.uploaded_files = 0
        self.failed_uploads = 0
        self.lock = Lock()
    
    def increment_processed(self):
        with self.lock:
            self.processed_files += 1
    
    def increment_uploaded(self):
        with self.lock:
            self.uploaded_files += 1
    
    def increment_failed(self):
        with self.lock:
            self.failed_uploads += 1

counters = Counters()

def print_configuration():
    """Print current configuration for user verification"""
    print("CONFIGURATION:")
    print("=" * 60)
    print(f"Input Path (Base):     {BASE_PATH}")
    print(f"Output Path:           {OUTPUT_FOLDER}")
    print(f"CSV Output Path:       {CSV_OUTPUT_PATH}")
    print(f"S3 Bucket:             {S3_BUCKET_NAME}")
    print(f"S3 Folder:             {S3_FOLDER}")
    print(f"S3 Region:             {S3_REGION}")
    print(f"Max Concurrent Uploads: {MAX_WORKERS}")
    print(f"Target SKUs:           {len(TARGET_SKUS)} SKUs")
    print("=" * 60)

def is_image_file(filename):
    """Check if file is an image based on common image extensions"""
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.svg'}
    return Path(filename).suffix.lower() in image_extensions

def extract_sku_number(folder_name):
    """Extract SKU number from folder name - handles multiple SKU patterns with case insensitive matching"""
    # Improved patterns for SKU extraction with case insensitive matching
    patterns = [
        r'(BLKA\d+)',   # BLKA followed by numbers
        r'(BKLA\d+)',   # BKLA followed by numbers  
        r'(BKLU\d+)',   # BKLU followed by numbers
        r'(BLKU\d+)',   # BLKU followed by numbers
        r'(BU\d+)',     # BU followed by numbers
    ]
    
    for pattern in patterns:
        match = re.search(pattern, folder_name, re.IGNORECASE)
        if match:
            return match.group(1).upper()  # Return uppercase version
    
    return None

def find_sku_in_folder_name(folder_name):
    """Find any target SKU that matches the folder name (case insensitive)"""
    folder_upper = folder_name.upper()
    for sku in TARGET_SKUS:
        if sku.upper() in folder_upper:
            return sku
    return None

def extract_parentheses_index(filename):
    """Extract index from filename pattern: SKU (1).jpg, SKU (2).jpg, etc."""
    pattern = r'\s*\((\d+)\)'
    match = re.search(pattern, filename)
    if match:
        return int(match.group(1))
    return None

def is_whatsapp_image(filename):
    """Check if file is a WhatsApp image"""
    return filename.lower().startswith('whatsapp image')

def categorize_files(folder_path):
    """Categorize all image files in folder by type and return organized data"""
    try:
        all_files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    except PermissionError:
        print(f"  Error: Permission denied accessing {folder_path}")
        return [], [], []
    
    # Filter only image files
    image_files = [f for f in all_files if is_image_file(f)]
    
    # Categorize images
    indexed_files = []      # Files with parentheses indices
    whatsapp_files = []     # WhatsApp images  
    other_files = []        # Other random files
    
    for filename in image_files:
        index = extract_parentheses_index(filename)
        if index is not None:
            indexed_files.append((filename, index))
        elif is_whatsapp_image(filename):
            whatsapp_files.append(filename)
        else:
            other_files.append(filename)
    
    # Sort indexed files by their parentheses number
    indexed_files.sort(key=lambda x: x[1])
    
    # Sort WhatsApp and other files alphabetically for consistent processing
    whatsapp_files.sort()
    other_files.sort()
    
    return indexed_files, whatsapp_files, other_files

def get_next_available_index(indexed_files):
    """Get the next available index after the highest indexed file"""
    if not indexed_files:
        return 1
    
    # Get all used indices
    used_indices = [idx for _, idx in indexed_files]
    
    # Find the next available index (highest + 1)
    return max(used_indices) + 1

def process_folder_files(folder_path, sku):
    """Process image files in a single folder with priority: indexed -> whatsapp -> others"""
    # Categorize all files
    indexed_files, whatsapp_files, other_files = categorize_files(folder_path)
    
    total_files = len(indexed_files) + len(whatsapp_files) + len(other_files)
    if total_files == 0:
        print(f"  No image files found in {sku}")
        return []
    
    print(f"  Found: {len(indexed_files)} indexed, {len(whatsapp_files)} WhatsApp, {len(other_files)} other images")
    
    processed_files = []
    
    # PRIORITY 1: Process indexed files first (maintain their original indices)
    for image_file, original_index in indexed_files:
        old_path = os.path.join(folder_path, image_file)
        file_extension = Path(image_file).suffix
        
        # Use the original index from parentheses
        new_filename = f"{sku}_{original_index}{file_extension}"
        new_path = os.path.join(OUTPUT_FOLDER, new_filename)
        
        try:
            shutil.copy2(old_path, new_path)
            processed_files.append({
                'old_path': old_path,
                'new_path': new_path,
                'new_filename': new_filename,
                'sku': sku,
                'file_type': 'image',
                'original_index': original_index,
                'source_type': 'indexed'
            })
            counters.increment_processed()
            print(f"    ✓ [INDEXED] {image_file} -> {new_filename}")
        except Exception as e:
            print(f"    Error copying {image_file}: {e}")
    
    # Get next available index for fallback files
    next_index = get_next_available_index(indexed_files)
    
    # PRIORITY 2: Process WhatsApp images (continue numbering)
    for image_file in whatsapp_files:
        old_path = os.path.join(folder_path, image_file)
        file_extension = Path(image_file).suffix
        
        new_filename = f"{sku}_{next_index}{file_extension}"
        new_path = os.path.join(OUTPUT_FOLDER, new_filename)
        
        try:
            shutil.copy2(old_path, new_path)
            processed_files.append({
                'old_path': old_path,
                'new_path': new_path,
                'new_filename': new_filename,
                'sku': sku,
                'file_type': 'image',
                'original_index': next_index,
                'source_type': 'whatsapp'
            })
            counters.increment_processed()
            print(f"    ✓ [WHATSAPP] {image_file} -> {new_filename}")
            next_index += 1
        except Exception as e:
            print(f"    Error copying {image_file}: {e}")
    
    # PRIORITY 3: Process other files (continue numbering)
    for image_file in other_files:
        old_path = os.path.join(folder_path, image_file)
        file_extension = Path(image_file).suffix
        
        new_filename = f"{sku}_{next_index}{file_extension}"
        new_path = os.path.join(OUTPUT_FOLDER, new_filename)
        
        try:
            shutil.copy2(old_path, new_path)
            processed_files.append({
                'old_path': old_path,
                'new_path': new_path,
                'new_filename': new_filename,
                'sku': sku,
                'file_type': 'image',
                'original_index': next_index,
                'source_type': 'other'
            })
            counters.increment_processed()
            print(f"    ✓ [OTHER] {image_file} -> {new_filename}")
            next_index += 1
        except Exception as e:
            print(f"    Error copying {image_file}: {e}")
    
    return processed_files

def upload_file_to_s3(file_info):
    """Upload a single file to S3"""
    s3_client = boto3.client(
        's3',
        config=boto3.session.Config(
            retries={'max_attempts': 3, 'mode': 'adaptive'},
            max_pool_connections=50,
            connect_timeout=300,
            read_timeout=300
        )
    )
    
    file_path = file_info['new_path']
    filename = file_info['new_filename']
    s3_object_key = f"{S3_FOLDER}/{filename}"
    
    try:
        # Get file size
        file_size = os.path.getsize(file_path)
        
        # Upload file
        if file_size > 100 * 1024 * 1024:  # 100MB
            # Use multipart upload for large files
            transfer_config = boto3.s3.transfer.TransferConfig(
                multipart_threshold=1024 * 25,
                max_concurrency=10,
                multipart_chunksize=1024 * 25,
                use_threads=True
            )
            
            s3_client.upload_file(
                file_path, 
                S3_BUCKET_NAME, 
                s3_object_key,
                Config=transfer_config,
                ExtraArgs={'ACL': 'public-read'}
            )
        else:
            s3_client.upload_file(
                file_path, 
                S3_BUCKET_NAME, 
                s3_object_key,
                ExtraArgs={'ACL': 'public-read'}
            )
        
        # Generate S3 URL
        s3_url = f"https://{S3_BUCKET_NAME}.s3.{S3_REGION}.amazonaws.com/{s3_object_key}"
        
        counters.increment_uploaded()
        return {
            'success': True,
            'file_info': file_info,
            's3_url': s3_url,
            'file_size': file_size
        }
        
    except Exception as e:
        counters.increment_failed()
        print(f"Failed to upload {filename}: {e}")
        return {
            'success': False,
            'file_info': file_info,
            'error': str(e)
        }

def process_files_and_upload():
    """Main function to process files and upload to S3"""
    
    print_configuration()
    
    # Verify AWS credentials
    try:
        s3_client = boto3.client('s3')
        s3_client.list_buckets()
        print("✓ AWS credentials verified")
    except NoCredentialsError:
        print("✗ AWS credentials not found. Please run 'aws configure' first.")
        return
    except Exception as e:
        print(f"✗ Error connecting to AWS: {e}")
        return
    
    # Check if base path exists
    if not os.path.exists(BASE_PATH):
        print(f"✗ Base path does not exist: {BASE_PATH}")
        return
    
    # Create output folder
    if not os.path.exists(OUTPUT_FOLDER):
        os.makedirs(OUTPUT_FOLDER)
        print(f"✓ Created output folder: {OUTPUT_FOLDER}")
    
    # Get all folders in the base directory
    folders = [f for f in os.listdir(BASE_PATH) if os.path.isdir(os.path.join(BASE_PATH, f))]
    
    # Filter folders for target SKUs - using both extraction methods with improved case handling
    target_folders = []
    processed_skus = set()  # Prevent duplicate processing
    
    for folder in folders:
        # First try to extract SKU from folder name
        sku = extract_sku_number(folder)
        
        # If extraction fails, try to find SKU within folder name
        if not sku:
            sku = find_sku_in_folder_name(folder)
        
        if sku and sku in TARGET_SKUS and sku not in processed_skus:
            target_folders.append((folder, sku))
            processed_skus.add(sku)
            print(f"✓ Found target folder: {folder} -> {sku}")
    
    if not target_folders:
        print("✗ No target SKU folders found")
        print("Available folders:")
        for folder in folders[:10]:  # Show first 10 folders for debugging
            print(f"  - {folder}")
        return
    
    print(f"✓ Found {len(target_folders)} target folders")
    
    # Process files from folders
    print("\nProcessing files...")
    all_processed_files = []
    
    for folder_name, sku in target_folders:
        folder_path = os.path.join(BASE_PATH, folder_name)
        print(f"Processing {sku} from folder: {folder_name}")
        
        processed_files = process_folder_files(folder_path, sku)
        all_processed_files.extend(processed_files)
    
    if not all_processed_files:
        print("✗ No files were processed")
        return
    
    print(f"✓ Processed {len(all_processed_files)} files")
    
    # Upload files to S3 with concurrent processing
    print(f"\nUploading files to S3 (max {MAX_WORKERS} concurrent uploads)...")
    
    upload_results = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all upload tasks
        future_to_file = {executor.submit(upload_file_to_s3, file_info): file_info 
                         for file_info in all_processed_files}
        
        # Process completed uploads
        for future in as_completed(future_to_file):
            result = future.result()
            upload_results.append(result)
            
            if result['success']:
                file_info = result['file_info']
                source_type = file_info.get('source_type', 'unknown')
                print(f"✓ Uploaded [{source_type.upper()}]: {file_info['new_filename']} ({result['file_size']/1024:.1f}KB)")
            
            # Show progress
            completed = len(upload_results)
            total = len(all_processed_files)
            print(f"Progress: {completed}/{total} files processed")
    
    # Generate CSV with image links (sorted by original index to maintain order)
    print(f"\nGenerating CSV: {CSV_OUTPUT_PATH}")
    
    # Organize successful uploads by SKU and sort by original index
    sku_images = {}
    for result in upload_results:
        if result['success']:
            sku = result['file_info']['sku']
            if sku not in sku_images:
                sku_images[sku] = []
            sku_images[sku].append({
                'url': result['s3_url'],
                'index': result['file_info']['original_index'],
                'source_type': result['file_info'].get('source_type', 'unknown')
            })
    
    # Sort URLs by original index for each SKU to maintain proper order
    for sku in sku_images:
        sku_images[sku].sort(key=lambda x: x['index'])
    
    # Write CSV
    with open(CSV_OUTPUT_PATH, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['SKU', 'Image_Links'])
        
        for sku in TARGET_SKUS:
            if sku in sku_images:
                # Extract URLs in sorted order
                sorted_urls = [item['url'] for item in sku_images[sku]]
                image_links = '|'.join(sorted_urls)
                writer.writerow([sku, image_links])
                
                # Show breakdown by source type
                source_counts = {}
                for item in sku_images[sku]:
                    source_type = item['source_type']
                    source_counts[source_type] = source_counts.get(source_type, 0) + 1
                
                source_info = ', '.join([f"{count} {stype}" for stype, count in source_counts.items()])
                print(f"✓ CSV: {sku} -> {len(sorted_urls)} images ({source_info})")
            else:
                writer.writerow([sku, ''])
                print(f"✗ CSV: {sku} -> No images found")
    
    # Final summary
    print(f"\n" + "=" * 60)
    print("PROCESSING COMPLETE")
    print(f"Files processed: {counters.processed_files}")
    print(f"Files uploaded: {counters.uploaded_files}")
    print(f"Failed uploads: {counters.failed_uploads}")
    print(f"SKUs with images: {len(sku_images)}")
    print(f"CSV generated: {CSV_OUTPUT_PATH}")
    print(f"=" * 60)

if __name__ == "__main__":
    print("Advanced SKU Image Processor with Multi-Format Support")
    print("Processing: Indexed Files -> WhatsApp Images -> Other Files")
    print("=" * 70)
    
    try:
        process_files_and_upload()
    except KeyboardInterrupt:
        print("\n✗ Process interrupted by user")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
    
    input("\nPress Enter to exit...")