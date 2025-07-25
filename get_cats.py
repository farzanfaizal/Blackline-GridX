import pandas as pd
import boto3
import json
import time
from pathlib import Path
import os

# =============================================================================
# CONFIGURATION - MODIFY THESE PATHS AS NEEDED
# =============================================================================
CATEGORIES_CSV_PATH = r"C:\Users\ASUS\Desktop\P-Projects\BlackLine\eBay_Categories_list_only.csv"
MAIN_FILE_PATH = r"C:\Users\ASUS\Desktop\P-Projects\BlackLine\B6_B7_one_output.csv"
OUTPUT_FILE_PATH = r"C:\Users\ASUS\Desktop\P-Projects\BlackLine\B6_B7_one_Output.csv(CAT).csv"

# AWS Bedrock Configuration
AWS_REGION = "us-east-1"  # Claude is available in us-east-1
MODEL_ID = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"  # Claude 3 Sonnet

# Processing Configuration
BATCH_SIZE = 10  # Number of titles to process at once
DELAY_BETWEEN_BATCHES = 2  # Seconds to wait between batches to avoid rate limits

# =============================================================================

def print_configuration():
    """Print current configuration for user verification"""
    print("eBay Category Matcher Configuration:")
    print("=" * 60)
    print(f"Categories CSV:        {CATEGORIES_CSV_PATH}")
    print(f"Main File:             {MAIN_FILE_PATH}")
    print(f"Output File:           {OUTPUT_FILE_PATH}")
    print(f"AWS Region:            {AWS_REGION}")
    print(f"Claude Model:          {MODEL_ID}")
    print(f"Batch Size:            {BATCH_SIZE}")
    print(f"Delay Between Batches: {DELAY_BETWEEN_BATCHES}s")
    print("=" * 60)

def load_categories_data():
    """Load eBay categories from CSV file"""
    try:
        df = pd.read_csv(CATEGORIES_CSV_PATH)
        print(f"✓ Loaded {len(df)} categories from CSV")
        
        # Create a formatted string of all categories for Claude
        categories_text = ""
        for _, row in df.iterrows():
            category_tree = row['Category Tree']
            category_id = row['CategoryID']
            categories_text += f"Category: {category_tree} | ID: {category_id}\n"
        
        return categories_text, df
    except Exception as e:
        print(f"✗ Error loading categories CSV: {e}")
        return None, None

def load_main_file():
    """Load the main CSV file with product listings"""
    try:
        df = pd.read_csv(MAIN_FILE_PATH)
        print(f"✓ Loaded {len(df)} products from main file")
        
        # Check if Title column exists (assuming it's column A)
        if 'Title' not in df.columns:
            # If Title column doesn't exist, assume first column is Title
            df.columns = ['Title'] + list(df.columns[1:])
            print("  Note: Renamed first column to 'Title'")
        
        return df
    except Exception as e:
        print(f"✗ Error loading main file: {e}")
        return None

def create_bedrock_client():
    """Create AWS Bedrock client"""
    try:
        bedrock = boto3.client(
            service_name='bedrock-runtime',
            region_name=AWS_REGION
        )
        print("✓ AWS Bedrock client created successfully")
        return bedrock
    except Exception as e:
        print(f"✗ Error creating Bedrock client: {e}")
        return None

def get_category_from_claude(bedrock_client, product_titles, categories_text):
    """Get category recommendations from Claude for a batch of product titles"""
    
    # Create the prompt for Claude
    prompt = f"""You are an expert at categorizing automotive products for eBay listings. 

Here are the available eBay categories with their IDs:
{categories_text}

I need you to analyze these product titles and match each one to the most appropriate eBay category from the list above.

Product titles to categorize:
{chr(10).join([f"{i+1}. {title}" for i, title in enumerate(product_titles)])}

Please respond with ONLY a JSON array in this exact format:
[
  {{"title": "Product Title 1", "category": "Exact Category Tree", "id": "CategoryID"}},
  {{"title": "Product Title 2", "category": "Exact Category Tree", "id": "CategoryID"}},
  ...
]

Important:
- Use the EXACT category tree text as it appears in the list
- Use the EXACT category ID as it appears in the list
- If no perfect match exists, choose the closest automotive-related category
- Ensure the JSON is valid and properly formatted
- Do not include any explanation, just the JSON array
"""

    try:
        # Prepare the request body
        body = {
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 2000,
            "messages": [
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            "temperature": 0.1,
            "top_p": 0.9
        }
        
        # Make the request to Claude
        response = bedrock_client.invoke_model(
            modelId=MODEL_ID,
            body=json.dumps(body),
            contentType="application/json"
        )
        
        # Parse the response
        response_body = json.loads(response['body'].read())
        claude_response = response_body['content'][0]['text']
        
        # Try to parse the JSON response
        try:
            # Extract JSON from the response (in case Claude adds extra text)
            start_idx = claude_response.find('[')
            end_idx = claude_response.rfind(']') + 1
            json_str = claude_response[start_idx:end_idx]
            
            category_matches = json.loads(json_str)
            return category_matches
            
        except json.JSONDecodeError as e:
            print(f"✗ Error parsing Claude's JSON response: {e}")
            print(f"Claude's response: {claude_response}")
            return None
            
    except Exception as e:
        print(f"✗ Error calling Claude: {e}")
        return None

def process_categories():
    """Main function to process categories"""
    
    print_configuration()
    
    # Load data
    categories_text, categories_df = load_categories_data()
    if categories_text is None:
        return
    
    main_df = load_main_file()
    if main_df is None:
        return
    
    # Create Bedrock client
    bedrock_client = create_bedrock_client()
    if bedrock_client is None:
        return
    
    # Prepare output dataframe
    output_df = main_df.copy()
    output_df['eBay_Category'] = ''
    output_df['eBay_Category_ID'] = ''
    
    # Process titles in batches
    titles = main_df['Title'].tolist()
    total_titles = len(titles)
    processed_count = 0
    
    print(f"\nProcessing {total_titles} product titles in batches of {BATCH_SIZE}...")
    
    for i in range(0, total_titles, BATCH_SIZE):
        batch_titles = titles[i:i + BATCH_SIZE]
        batch_indices = list(range(i, min(i + BATCH_SIZE, total_titles)))
        
        print(f"\nProcessing batch {i//BATCH_SIZE + 1}/{(total_titles + BATCH_SIZE - 1)//BATCH_SIZE}")
        print(f"Titles {i+1}-{min(i+BATCH_SIZE, total_titles)} of {total_titles}")
        
        # Get categories from Claude
        category_matches = get_category_from_claude(bedrock_client, batch_titles, categories_text)
        
        if category_matches:
            # Update the dataframe with Claude's recommendations
            for match in category_matches:
                try:
                    # Find the row index for this title
                    title = match['title']
                    category = match['category']
                    category_id = match['id']
                    
                    # Find the corresponding row in the dataframe
                    for idx in batch_indices:
                        if output_df.iloc[idx]['Title'] == title:
                            output_df.iloc[idx, output_df.columns.get_loc('eBay_Category')] = category
                            output_df.iloc[idx, output_df.columns.get_loc('eBay_Category_ID')] = category_id
                            processed_count += 1
                            print(f"  ✓ {title[:50]}... -> {category}")
                            break
                            
                except Exception as e:
                    print(f"  ✗ Error processing match: {e}")
        else:
            print(f"  ✗ Failed to get categories for this batch")
        
        # Delay between batches to avoid rate limits
        if i + BATCH_SIZE < total_titles:
            print(f"  Waiting {DELAY_BETWEEN_BATCHES} seconds before next batch...")
            time.sleep(DELAY_BETWEEN_BATCHES)
    
    # Save the output file
    try:
        output_df.to_csv(OUTPUT_FILE_PATH, index=False)
        print(f"\n✓ Output file saved: {OUTPUT_FILE_PATH}")
        
        # Summary statistics
        filled_categories = output_df[output_df['eBay_Category'] != ''].shape[0]
        print(f"\nSummary:")
        print(f"Total products: {total_titles}")
        print(f"Successfully categorized: {filled_categories}")
        print(f"Missing categories: {total_titles - filled_categories}")
        
    except Exception as e:
        print(f"✗ Error saving output file: {e}")

if __name__ == "__main__":
    print("eBay Category Matcher with Claude AI")
    print("=" * 60)
    
    try:
        process_categories()
    except KeyboardInterrupt:
        print("\n✗ Process interrupted by user")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
    
    input("\nPress Enter to exit...")