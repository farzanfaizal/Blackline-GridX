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
OUTPUT_FOLDER = r"D:\AUTOTEKX\Blackline\PROCESSED_FURTHER_MISSING_SKUS"
CSV_OUTPUT_PATH = r"D:\AUTOTEKX\Blackline\skus_image_links-11-07-383.csv"

# S3 Configuration
S3_BUCKET_NAME = "gridximges"
S3_FOLDER = "blackline"
S3_REGION = "me-central-1"

# Processing Configuration
MAX_WORKERS = 3  # Number of concurrent uploads (adjust based on your internet speed)
UPLOAD_IMAGES_ONLY = True  # Set to False if you want to upload videos too

# =============================================================================

# Target SKUs from the provided list
# Updated folder names (SKUs) for processing
TARGET_SKUS = [
    "BLKU383"
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
    print(f"Upload Images Only:    {UPLOAD_IMAGES_ONLY}")
    print(f"Target SKUs:           {len(TARGET_SKUS)} SKUs")
    print("=" * 60)

def is_image_file(filename):
    """Check if file is an image based on common image extensions"""
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.bmp', '.tiff', '.tif', '.webp', '.svg'}
    return Path(filename).suffix.lower() in image_extensions

def is_video_file(filename):
    """Check if file is a video based on common video extensions"""
    video_extensions = {'.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v', '.3gp'}
    return Path(filename).suffix.lower() in video_extensions

def extract_sku_number(folder_name):
    """Extract SKU number from folder name - handles multiple SKU patterns"""
    # Try different patterns for SKU extraction
    patterns = [
        r'(BLKA\d+)',   # BLKA followed by numbers
        r'(BKLA\d+)',   # BKLA followed by numbers  
        r'(BKLU\d+)',   # BKLU followed by numbers
        r'(BLKU\d+)',   # BLKU followed by numbers
    ]
    
    for pattern in patterns:
        match = re.search(pattern, folder_name, re.IGNORECASE)
        if match:
            return match.group(1).upper()  # Return uppercase version
    
    return None

def find_sku_in_folder_name(folder_name):
    """Find any target SKU that matches the folder name"""
    folder_upper = folder_name.upper()
    for sku in TARGET_SKUS:
        if sku in folder_upper:
            return sku
    return None

def process_folder_files(folder_path, sku):
    """Process files in a single folder and return file info"""
    try:
        files = [f for f in os.listdir(folder_path) if os.path.isfile(os.path.join(folder_path, f))]
    except PermissionError:
        print(f"  Error: Permission denied accessing {folder_path}")
        return []
    
    if not files:
        print(f"  No files found in {sku}")
        return []
    
    # Separate images and videos
    image_files = [f for f in files if is_image_file(f)]
    video_files = [f for f in files if is_video_file(f)]
    
    print(f"  Found {len(image_files)} image(s) and {len(video_files)} video(s)")
    
    processed_files = []
    
    # Process image files
    for i, image_file in enumerate(image_files, 1):
        old_path = os.path.join(folder_path, image_file)
        file_extension = Path(image_file).suffix
        new_filename = f"{sku}_{i}{file_extension}"
        new_path = os.path.join(OUTPUT_FOLDER, new_filename)
        
        try:
            shutil.copy2(old_path, new_path)
            processed_files.append({
                'old_path': old_path,
                'new_path': new_path,
                'new_filename': new_filename,
                'sku': sku,
                'file_type': 'image'
            })
            counters.increment_processed()
        except Exception as e:
            print(f"    Error copying {image_file}: {e}")
    
    # Process video files (if enabled)
    if not UPLOAD_IMAGES_ONLY:
        for i, video_file in enumerate(video_files, 1):
            old_path = os.path.join(folder_path, video_file)
            file_extension = Path(video_file).suffix
            new_filename = f"{sku}_VID_{i}{file_extension}"
            new_path = os.path.join(OUTPUT_FOLDER, new_filename)
            
            try:
                shutil.copy2(old_path, new_path)
                processed_files.append({
                    'old_path': old_path,
                    'new_path': new_path,
                    'new_filename': new_filename,
                    'sku': sku,
                    'file_type': 'video'
                })
                counters.increment_processed()
            except Exception as e:
                print(f"    Error copying {video_file}: {e}")
    
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
    
    # Filter folders for target SKUs - using both extraction methods
    target_folders = []
    for folder in folders:
        # First try to extract SKU from folder name
        sku = extract_sku_number(folder)
        
        # If extraction fails, try to find SKU within folder name
        if not sku:
            sku = find_sku_in_folder_name(folder)
        
        if sku and sku in TARGET_SKUS:
            target_folders.append((folder, sku))
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
                print(f"✓ Uploaded: {file_info['new_filename']} ({result['file_size']/1024:.1f}KB)")
            
            # Show progress
            completed = len(upload_results)
            total = len(all_processed_files)
            print(f"Progress: {completed}/{total} files processed")
    
    # Generate CSV with image links
    print(f"\nGenerating CSV: {CSV_OUTPUT_PATH}")
    
    # Organize successful uploads by SKU (images only)
    sku_images = {}
    for result in upload_results:
        if result['success'] and result['file_info']['file_type'] == 'image':
            sku = result['file_info']['sku']
            if sku not in sku_images:
                sku_images[sku] = []
            sku_images[sku].append(result['s3_url'])
    
    # Write CSV
    with open(CSV_OUTPUT_PATH, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['SKU', 'Image_Links'])
        
        for sku in TARGET_SKUS:
            if sku in sku_images:
                image_links = '|'.join(sku_images[sku])
                writer.writerow([sku, image_links])
            else:
                writer.writerow([sku, ''])
    
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
    print("Combined SKU Processor with Concurrent Upload")
    print("Processing Missing SKUs from List")
    print("=" * 60)
    
    try:
        process_files_and_upload()
    except KeyboardInterrupt:
        print("\n✗ Process interrupted by user")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
    
    input("\nPress Enter to exit...")