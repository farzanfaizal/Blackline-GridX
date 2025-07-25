import pandas as pd
import json
import boto3
import re
import unicodedata
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# === Logger Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# === AWS Bedrock Claude Config ===
AWS_REGION_CLAUDE = "us-east-1"
BEDROCK_MODEL_ID = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
bedrock = boto3.client("bedrock-runtime", region_name=AWS_REGION_CLAUDE)

MAX_TOKENS = 512
TEMPERATURE = 0.7
TOP_P = 0.9
TOP_K = 250

# === HTML Template with Placeholders ===
HTML_TEMPLATE = """<style>:root{{--black:#1a1a1a;--white:#ffffff;--blue-primary:#007aff;--blue-light:#e5f2ff;--gray-text:#4a4a4a;--gray-light:#f9f9f9;--gray-border:#dce1e6;}}body,html{{margin:0;padding:0;background-color:var(--white);color:var(--gray-text);font-family:Arial,Helvetica,sans-serif;}}.wrapper{{max-width:850px;margin:20px auto;background-color:var(--gray-light);border-radius:16px;padding:20px;box-sizing:border-box;}}.container{{max-width:800px;margin:0 auto;padding:0;}}.header{{display:flex;align-items:center;justify-content:space-between;gap:20px;padding:15px;background:var(--white);border-radius:12px;border:1px solid var(--gray-border);flex-wrap:wrap;margin-bottom:25px;}}.title-container{{flex:1;min-width:300px;}}.title{{font-size:22px;font-weight:700;color:var(--black);line-height:1.3;margin:0 0 5px 0;}}.subtitle{{font-size:15px;color:var(--gray-text);font-weight:500;}}.stamp{{width:200px;height:auto;object-fit:contain;flex-shrink:0;}}.main-content{{display:flex;flex-wrap:wrap;margin-bottom:25px;}}.product-details{{flex:1;min-width:320px;background:var(--white);border:1px solid var(--gray-border);border-radius:12px;padding:25px;}}.product-details h3{{font-size:18px;font-weight:700;color:var(--black);margin:0 0 15px 0;border-bottom:2px solid var(--blue-primary);padding-bottom:8px;}}.product-details p{{font-size:15px;line-height:1.6;margin:0 0 15px 0;}}.product-details strong{{font-weight:600;color:var(--black);}}.fitment-container h3{{font-size:18px;font-weight:700;color:var(--black);margin:0 0 15px 0;}}table.fitment{{width:100%;border-collapse:collapse;margin-bottom:25px;background:var(--white);border-radius:10px;overflow:hidden;border:1px solid var(--gray-border);}}table.fitment th,table.fitment td{{padding:12px 15px;text-align:left;border-bottom:1px solid var(--gray-border);}}table.fitment th{{background-color:var(--black);color:var(--white);font-weight:600;font-size:15px;}}table.fitment td{{font-size:15px;}}table.fitment tbody tr:last-child td{{border-bottom:none;}}table.fitment tbody tr:nth-child(even){{background-color:var(--gray-light);}}.seller-info{{margin-top:20px;font-size:15px;line-height:1.6;}}.seller-info ul{{list-style:none;padding-left:0;}}.seller-info li::before{{content:"";}}</style><div class="wrapper"><div class="container"><div class="header"><div class="title-container"><h1 class="title">{title}</h1><p class="subtitle"><strong>Model Years:</strong> {model_years} | <strong>Part No:</strong> {part_number}</p></div><img src="https://gridximges.s3.me-central-1.amazonaws.com/logos/BLKStamp.jpg" alt="Authenticity Stamp" class="stamp" /></div><div class="main-content"><div class="product-details"><h3>Product Description</h3>{product_description}<div class="seller-info"><ul><li>&#9989; 100% tested and verified <b>By Blackline Auto's Quality Team</b></li><li>&#9989; <b>Genuine Used, Excellent Condition</b>, inspected and approved</li><li>&#9989; Easy installation</li></ul><h3>Payment, Shipping & Returns</h3><ul><li><strong>&#9989; Payment Policy:</strong> We accept only online payment methods, and you can choose any of the options provided by eBay at the time of checkout. If you have any queries or if you require any clarification, please contact us through eBay messaging service.</li><li><strong>&#9989; Shipping Policy:</strong> We provide Free Shipping worldwide to most of the countries using reputed couriers like DHL, FedEx or Aramex.</li><li><strong>&#9989; International Buyers Please Note:</strong> Import Duties, Taxes and charges are not included in the item price or shipping cost.</li><li><strong>&#9989; Return Policy:</strong> We accept 14 days returns. Please clarify all your doubts before purchase.</li><li><strong>&#9989; Handling Time:</strong> All packages are shipped within 3 working days.</li></ul><p>Trusted by professionals, Blackline Autos supplies premium components for luxury vehicles.<br>Please confirm the part number before purchase. For compatibility checks or bulk orders, feel free to contact us.</p></div></div></div><div class="fitment-container"><h3>Vehicle Fitment</h3>{fitment_table}</div></div></div>"""


def normalize_text(text):
    if pd.isna(text):
        return ""
    text = str(text)
    text = unicodedata.normalize("NFKD", text)
    text = re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", text)
    return text.strip()

def preserve_part_number(part_no):
    """Preserve part numbers as strings to avoid scientific notation conversion"""
    if pd.isna(part_no):
        return ""
    
    # Convert to string and strip whitespace
    part_str = str(part_no).strip()
    
    # Handle scientific notation if it exists
    if 'e+' in part_str.lower() or 'e-' in part_str.lower():
        try:
            # Try to convert back from scientific notation
            num = float(part_str)
            if num.is_integer():
                part_str = str(int(num))
            else:
                part_str = str(num)
        except ValueError:
            # If conversion fails, keep as string
            pass
    
    return part_str

def parse_car_details_and_part_name(combined_text):
    """
    Enhanced parsing using ONLY Claude's intelligence - NO hardcoded lists
    """
    if pd.isna(combined_text) or str(combined_text).strip() == "":
        return "", ""
    
    text = normalize_text(combined_text)
    
    # STRICT prompt for make extraction and single car selection
    prompt_text = f"""You are an automotive expert. Parse this parts listing text with STRICT requirements:

Text: {text}

CRITICAL REQUIREMENTS - FOLLOW EXACTLY:
1. COMPULSORY: Car details MUST start with MAKE (manufacturer like BMW, Mercedes, Audi, Volkswagen, Porsche, Bentley, Jaguar, Land Rover, etc.)
2. If multiple cars listed (like "VW Golf / Audi S4" or "Continental GT/GTC and Flying Spur"), select ONLY THE FIRST ONE
3. If model appears first without make (like "Passat" or "Flying Spur"), use your automotive knowledge to identify correct make
4. Include model and year range if available
5. Part name must be clean description without "GENUINE USED"

EXAMPLES OF CORRECT OUTPUT:
Input: "Passat Alltrack B7 2012-2014..." → CAR_DETAILS: Volkswagen Passat Alltrack B7 2012-2014
Input: "Flying Spur models 2003-2017..." → CAR_DETAILS: Bentley Flying Spur 2003-2017
Input: "Continental GT/GTC and Flying Spur..." → CAR_DETAILS: Bentley Continental GT (first one only)
Input: "VW Golf / Audi S4..." → CAR_DETAILS: Volkswagen Golf (first one only)

Return EXACTLY in this format:
CAR_DETAILS: [MAKE Model Year]
PART_NAME: [clean part description]

NO explanations, follow format exactly."""

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": prompt_text}]
        }],
        "max_tokens": 200,
        "temperature": 0.01,  # Very low for strict compliance
        "top_k": 30,
        "top_p": 0.5
    }
    
    result = call_claude(payload)
    
    # Parse the result
    car_details = ""
    part_name = ""
    
    if result:
        lines = result.strip().split('\n')
        for line in lines:
            if line.startswith('CAR_DETAILS:'):
                car_details = line.replace('CAR_DETAILS:', '').strip()
            elif line.startswith('PART_NAME:'):
                part_name = line.replace('PART_NAME:', '').strip()
    
    # If Claude fails, use emergency make identification
    if not car_details or not part_name:
        logger.warning(f"   ⚠️ Primary parsing failed, using emergency extraction...")
        
        # Emergency make identification prompt
        emergency_prompt = f"""EMERGENCY: Extract car manufacturer and model from: {text}

RULES:
- Must start with MAKE (BMW, Mercedes, Audi, etc.)
- If model only (like "Passat"), identify correct make
- Single car only, no multiple selections
- Format: MAKE Model

Text: {text}
Output:"""
        
        emergency_payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": emergency_prompt}]
            }],
            "max_tokens": 50,
            "temperature": 0.01
        }
        
        emergency_result = call_claude(emergency_payload)
        if emergency_result:
            car_details = emergency_result.strip()
        
        # Extract part name from remaining text
        if not part_name:
            part_name = re.sub(r'\bGENUINE USED\b', '', text).strip()
            if len(part_name) > 100:  # If too long, take middle portion
                mid = len(part_name) // 2
                part_name = part_name[mid:].strip()[:50]
    
    return car_details, part_name

def call_claude(payload, retries=3, sleep_time=2):
    """Claude API call with retry on error."""
    for attempt in range(retries):
        try:
            response = bedrock.invoke_model(
                modelId=BEDROCK_MODEL_ID,
                body=json.dumps(payload),
                contentType="application/json",
                accept="application/json"
            )
            response_data = json.loads(response['body'].read())
            return response_data['content'][0]['text'].strip()
        except Exception as e:
            logger.warning(f"Claude call failed (attempt {attempt+1}/{retries}): {e}")
            time.sleep(sleep_time)
    return ""

def clean_part_number(raw_part_number):
    """Clean and normalize part number using Claude"""
    if pd.isna(raw_part_number) or str(raw_part_number).strip() == "":
        return ""
    
    # First apply basic preservation
    cleaned_basic = preserve_part_number(raw_part_number)
    
    # If it looks already clean (alphanumeric with common separators), return as is
    if re.match(r'^[A-Za-z0-9\-_.]+$', cleaned_basic) and len(cleaned_basic) < 50:
        return cleaned_basic
    
    prompt_text = f"""Clean and normalize this automotive part number. Remove any formatting errors, extra characters, or encoding issues. Return only the clean part number with no additional text or explanations.

Part Number: {raw_part_number}

Rules:
- Keep only alphanumeric characters, hyphens, dots, and underscores
- Remove any special characters, spaces, or formatting artifacts
- If the part number appears to be in scientific notation, convert it back to normal format
- If multiple part numbers are present, return only the primary one
- If the input is completely garbled or unreadable, return "INVALID"

Return only the cleaned part number:"""

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": prompt_text}]
        }],
        "max_tokens": 100,
        "temperature": 0.1,
        "top_k": 50,
        "top_p": 0.8
    }
    
    cleaned = call_claude(payload)
    
    # Fallback to basic cleaning if Claude fails or returns invalid
    if not cleaned or cleaned == "INVALID" or len(cleaned) > 100:
        return cleaned_basic
    
    return cleaned.strip()

def clean_all_part_numbers(df):
    """Clean all part numbers in the dataframe before processing"""
    logger.info("🧹 Starting part number cleaning process...")
    
    total_parts = len(df)
    cleaned_count = 0
    
    # Clean PART NO column
    if 'PART NO' in df.columns:
        for idx, row in df.iterrows():
            original_part = row['PART NO']
            cleaned_part = clean_part_number(original_part)
            
            if str(original_part) != str(cleaned_part):
                logger.info(f"   Row {idx+1}: '{original_part}' → '{cleaned_part}'")
                cleaned_count += 1
            
            df.at[idx, 'PART NO'] = cleaned_part
    
    # Also clean SKU column if it exists
    if 'SKU' in df.columns:
        for idx, row in df.iterrows():
            original_sku = row['SKU']
            cleaned_sku = clean_part_number(original_sku)
            
            if str(original_sku) != str(cleaned_sku):
                logger.info(f"   Row {idx+1} SKU: '{original_sku}' → '{cleaned_sku}'")
                cleaned_count += 1
            
            df.at[idx, 'SKU'] = cleaned_sku
    
    logger.info(f"✅ Part number cleaning completed. {cleaned_count} part numbers were cleaned out of {total_parts} total rows.")
    return df

def generate_title_with_compulsory_make(car_details, part_no, part_name, sku):
    """Generate title with COMPULSORY make enforcement using ONLY Claude intelligence"""
    identifier = part_no if part_no else sku
    
    # STRICT prompts with compulsory make requirement
    prompts = [
        # Attempt 1: Full format with strict make requirement
        f"""Create a professional eBay title EXACTLY 79 characters or less. STRICT REQUIREMENTS:

Input Data:
- Car: {car_details}
- Part: {part_name}
- Part Number: {identifier}

COMPULSORY RULES - NO EXCEPTIONS:
1. Title MUST start with car MAKE (BMW, Mercedes, Audi, Volkswagen, Porsche, Bentley, etc.)
2. Use proper Title Case formatting
3. Fix any spelling errors
4. Part numbers stay UPPERCASE
5. Format: [MAKE Model Year] [Part Description] Used Genuine [PART-NUMBER]
6. Maximum 79 characters including spaces

If car details don't start with MAKE, identify and add the correct manufacturer.

Return ONLY the title - no explanations:""",

        # Attempt 2: Shorter format with make enforcement
        f"""Create eBay title under 79 characters with COMPULSORY MAKE:

Car: {car_details}
Part: {part_name}
Number: {identifier}

STRICT FORMAT: [MAKE Model] [Part] Used [NUMBER]
- Must start with manufacturer name
- Professional Title Case
- Fix spelling errors
- 79 chars max

Title only:""",

        # Attempt 3: Compact with make requirement
        f"""eBay title under 79 chars - MUST start with car MAKE:

{car_details} | {part_name} | {identifier}

Requirements:
- Start with manufacturer (BMW/Mercedes/Audi/etc)
- Title Case
- Format: [MAKE Model] [Part] [CODE]

Title:""",

        # Attempt 4: Minimal with make enforcement
        f"""Short title under 79 chars starting with MAKE:

Data: {car_details} {part_name} {identifier}

MUST begin with manufacturer name.
Title:"""
    ]
    
    for attempt, prompt_text in enumerate(prompts, 1):
        logger.info(f"   🎯 Title attempt {attempt}/4 with make enforcement")
        
        payload = {
            "anthropic_version": "bedrock-2023-05-31",
            "messages": [{
                "role": "user",
                "content": [{"type": "text", "text": prompt_text}]
            }],
            "max_tokens": 120,
            "temperature": 0.1,  # Low temperature for strict compliance
            "top_k": 40,
            "top_p": 0.6
        }
        
        title = call_claude(payload)
        
        if title:
            # Clean the title
            title = title.strip().strip('"').strip("'").strip()
            title = re.sub(r'\s+', ' ', title)  # Normalize whitespace
            
            # Validate make is present (first word should be a manufacturer)
            first_word = title.split()[0] if title.split() else ""
            
            # Check if title meets requirements
            if len(title) <= 79 and len(title) >= 20 and first_word.isalpha() and len(first_word) > 2:
                logger.info(f"   ✅ Title with make: '{first_word}' - '{title}' ({len(title)} chars)")
                return title
            else:
                logger.warning(f"   ⚠️ Title failed validation: {len(title)} chars, first word: '{first_word}'")
    
    # Final fallback using Claude to ensure make
    logger.warning("   🚨 All attempts failed, using emergency make enforcement...")
    
    # Emergency make extraction and title creation
    make_prompt = f"""Extract ONLY the car manufacturer from: {car_details}
    
Examples: BMW, Mercedes, Audi, Volkswagen, Porsche, Bentley

Return manufacturer only:"""
    
    make_payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [{"role": "user", "content": [{"type": "text", "text": make_prompt}]}],
        "max_tokens": 30,
        "temperature": 0.01
    }
    
    make_result = call_claude(make_payload)
    make = make_result.strip().title() if make_result else "Auto"
    
    # Create emergency fallback title
    title_parts = [make]
    
    # Add model/year if available
    if car_details and len(car_details.split()) > 1:
        model_parts = car_details.split()[1:3]  # Take next 1-2 words
        title_parts.extend(model_parts)
    
    # Add simplified part name
    if part_name:
        clean_part = re.sub(r'\b(GENUINE|USED)\b', '', part_name, flags=re.IGNORECASE).strip()
        part_words = clean_part.split()[:2]  # Max 2 words
        title_parts.extend([word.title() for word in part_words if word])
    
    title_parts.append("Used")
    
    if identifier:
        title_parts.append(identifier.upper())
    
    # Build and truncate if needed
    emergency_title = ' '.join(title_parts)
    if len(emergency_title) > 79:
        emergency_title = emergency_title[:76] + "..."
    
    logger.info(f"   🔧 Emergency title: '{emergency_title}' ({len(emergency_title)} chars)")
    return emergency_title

def generate_product_description(car_details, part_name, identifier, model_years):
    """Generate product description using raw data instead of title"""
    prompt_text = f"""Write a concise product description (max 90 words) for a used automotive part eBay listing in a friendly but professional tone.

Car Details: {car_details}
Part Name: {part_name} 
Part Number: {identifier}
Model Years: {model_years}

Focus on:
- OEM/genuine quality and condition (used, tested, inspected)
- Perfect fitment for the specific vehicle
- Cost savings over new parts
- Easy installation
- Quality verification and testing

Return only HTML <p> blocks, no headings or duplicate details:"""

    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [{
            "role": "user",
            "content": [{"type": "text", "text": prompt_text}]
        }],
        "max_tokens": 250,
        "temperature": 0.65,
        "top_k": 120,
        "top_p": 0.93
    }
    return call_claude(payload)

def generate_fitment_table(car_details, part_name, part_no):
    fitment_prompt = f"""Generate a list of vehicle fitment details as a JSON array of dictionaries (fields: Make, Model, Year) that this part may fit, based on the following:\nCar Details: {car_details}\nPart: {part_name}\nPart Number: {part_no}\nFormat: [{{"Make":"...","Model":"...","Year":"..."}},...]\nOnly output the JSON list."""
    payload = {
        "anthropic_version": "bedrock-2023-05-31",
        "messages": [{"role": "user", "content": [{"type": "text", "text": fitment_prompt}]}],
        "max_tokens": 400,
        "temperature": 0.4,
        "top_k": 250,
        "top_p": 0.95,
    }
    result = call_claude(payload)
    try:
        fitments = json.loads(result)
    except Exception as e:
        logger.warning(f"⚠️ Failed to fetch fitment data: {e}")
        fitments = []
    if not fitments:
        return '<table class="fitment"><tbody><tr><td colspan="3">Fitment data unavailable</td></tr></tbody></table>'
    rows = "".join([f"<tr><td>{item.get('Make','')}</td><td>{item.get('Model','')}</td><td>{item.get('Year','')}</td></tr>" for item in fitments])
    return f'<table class="fitment"><thead><tr><th scope="col">Make</th><th scope="col">Model</th><th scope="col">Year</th></tr></thead><tbody>{rows}</tbody></table>'


def extract_year_range(car_details):
    # Example: extract years like "2017–2022" or "2017-2022"
    match = re.search(r"(\d{4})[\u2013\u2014\-](\d{4})", car_details)
    if match:
        return f"{match.group(1)} {match.group(2)}"
    return ""

def extract_mpn_from_title(title):
    # Extract everything after the last '- ' (dash + space) instead of pipe
    parts = title.strip().rsplit('- ', 1)
    if len(parts) == 2:
        return parts[1].strip()
    return ""

def process_row(row, idx, total):
    # Parse the combined field
    combined_field = row.get("CAR DETAILS AND PART NAME", "")
    car_details, part_name = parse_car_details_and_part_name(combined_field)
    
    # Part numbers should already be cleaned at this point
    part_no = str(row.get("PART NO", "")).strip()
    sku = str(row.get("SKU", "")).strip()
    
    part_no_fallback = sku if part_no.lower() in ["", "-", "no part number", "invalid"] else part_no

    model_years = extract_year_range(car_details)
    logger.info(f"🔄 Processing row {idx + 1} of {total}")
    logger.info(f"   📄 Raw: '{combined_field[:50]}...'")
    logger.info(f"   🚗 Parsed: '{car_details}' | 🔧 Part: '{part_name[:30]}...'")

    # Generate title with compulsory make enforcement
    title = generate_title_with_compulsory_make(car_details, part_no_fallback, part_name, sku)
    
    # Log final result with make validation
    first_word = title.split()[0] if title.split() else "NONE"
    logger.info(f"   📝 FINAL (Make: {first_word}): '{title}' ({len(title)} chars)")
    
    # Use raw data for description instead of title
    product_description = generate_product_description(car_details, part_name, part_no_fallback, model_years)
    fitment_html = generate_fitment_table(car_details, part_name, part_no_fallback)

    # Use the cleaned part number
    mpn = part_no_fallback if part_no_fallback else sku

    full_html = HTML_TEMPLATE.format(
        title=title,
        model_years=model_years,
        part_number=mpn,
        product_description=product_description,
        fitment_table=fitment_html
    )

    return {
        "Title": title,
        "Description": full_html,
        "MPN": mpn,
        "SKU": sku
    }

def process_csv():
    input_file = r"B6_B7_one_input.csv"
    output_file = r"B6_B7_one_output.csv"
    
    # Read CSV with proper handling of part numbers as strings
    df = pd.read_csv(input_file, dtype=str, encoding="cp1252").fillna("")
    
    # *** NEW: Clean all part numbers before processing ***
    df = clean_all_part_numbers(df)
    
    output_rows = [None] * len(df)
    max_workers = 8  # Tune up to 4-6 if Claude can handle it without throttling

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_row, row, idx, len(df)): idx
            for idx, (_, row) in enumerate(df.iterrows())
        }
        for future in as_completed(futures):
            idx = futures[future]
            try:
                output_rows[idx] = future.result()
            except Exception as exc:
                logger.error(f"Row {idx} generated an exception: {exc}")
                output_rows[idx] = {"Title": "", "Description": "", "MPN": "", "SKU": ""}

    pd.DataFrame(output_rows).to_csv(output_file, index=False, encoding="utf-8")
    logger.info(f"✅ All done. Output saved to: {output_file}")

if __name__ == "__main__":
    process_csv()