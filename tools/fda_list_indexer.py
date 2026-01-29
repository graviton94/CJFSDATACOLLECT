import requests
from bs4 import BeautifulSoup
import pandas as pd
import concurrent.futures
import logging
import re
from pathlib import Path
import time
import sys

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fda_list_indexer.log')
    ]
)
logger = logging.getLogger(__name__)

BASE_URL = "https://www.accessdata.fda.gov/cms_ia/"
LIST_URL = BASE_URL + "ialist.html"
OUTPUT_DIR = Path("data/reference")
OUTPUT_FILE = OUTPUT_DIR / "fda_list_master.parquet"

def get_all_alerts_metadata():
    """Fetches the master list of Import Alerts and extracts metadata from the main table."""
    try:
        logger.info(f"Fetching master list from {LIST_URL}...")
        response = requests.get(LIST_URL, timeout=30)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        alerts = []
        
        # Strategy: Iterate ALL rows in the document, regardless of which table
        all_rows = soup.find_all('tr')
        
        for row in all_rows:
            cols = row.find_all(['td', 'th'])
            if not cols: continue
            
            # Locate the cell containing the specific importalert link
            target_link = None
            alert_no = None
            
            # Usually the first cell, but let's search just in case
            for col in cols:
                link = col.find('a', href=True)
                if link and 'importalert_' in link['href']:
                    target_link = link
                    alert_no = link.get_text(strip=True)
                    break
            
            if not target_link:
                continue

            # If we found a valid link, try to extract siblings
            # Assumption: Structure is [AlertNo+Link, Type, Date, Name] or similar
            # We explicitly look for Date pattern in remaining columns if position is unsure,
            # or rely on index 2 if structure is standard.
            
            href = target_link['href']
            full_url = href if href.startswith('http') else BASE_URL + href
            
            # Default values
            alert_type = "Unknown"
            publish_date = "Unknown"
            alert_name = "Unknown"
            
            # Clean extraction from columns
            col_texts = [c.get_text(strip=True) for c in cols]
            
            # Robust Parsing Strategy:
            # Table Structure is typically: [Alert #, Type, Publish Date, Title]
            # But sometimes Alert # column contains the link with the number.
            
            # 1. Try to get Alert Number from the link text first
            possible_alert_no = target_link.get_text(strip=True)
            
            # Validation: Alert No should look like "99-19" (Digits-Digits)
            # Relaxed regex to handle any length of digits (e.g. 16-04, 99-19)
            if re.match(r'^\d+-\d+$', possible_alert_no):
                alert_no = possible_alert_no
            else:
                # If link text is NOT a number (e.g. it's the title linked), look for number in 1st col
                # This handles cases where structure might be different
                first_col_text = col_texts[0]
                # Updated regex to be more robust
                match = re.search(r'(\d+-\d+)', first_col_text)
                if match:
                    alert_no = match.group(1)
                else:
                    # Fallback: keep what we found or skip
                    alert_no = possible_alert_no
            
            # 2. Extract other fields based on column position
            # We assume standard 4-column layout: [Alert #, Type, Date, Title]
            if len(col_texts) >= 4:
                alert_type = col_texts[1]
                publish_date = col_texts[2]
                alert_name = col_texts[3]
            elif len(col_texts) == 3:
                # [Alert #, Date, Title] ??
                publish_date = col_texts[1]
                alert_name = col_texts[2]
            else:
                 # Fallback: Use link text as title if it wasn't the number
                 if alert_no != possible_alert_no:
                     alert_name = possible_alert_no 
            
            # Final Safety Check: If Alert Name is empty but we have extra columns, try to find it
            if alert_name == "Unknown" and len(col_texts) > 1:
                alert_name = col_texts[-1] # Valid for most tables where description is last

            alerts.append({
                'Alert_No': alert_no,
                'URL': full_url,
                'Type': alert_type,
                'Publish_Date': publish_date,
                'Title': alert_name
            })
            
        logger.info(f"Found {len(alerts)} alerts in master list.")
        return alerts
    except Exception as e:
        logger.error(f"Failed to fetch master list: {e}")
        return []

def extract_alert_number(text):
    """Extracts alert number pattern like 99-19."""
    match = re.search(r'(\d+-\d+)', text)
    return match.group(1) if match else None

def parse_alert_page(alert_meta):
    """Parses a single Import Alert page for detailed content (Lists, Charge Code, Prod Desc)."""
    # Unpack metadata
    url = alert_meta['URL']
    alert_no = alert_meta['Alert_No']
    last_updated = alert_meta['Publish_Date'] # From Master List
    
    # Default Result
    result = {
        'Alert_No': alert_no,
        'OASIS_Charge_Code_Line': "N/A",
        'Product_Description': "N/A",
        'Last_Updated_Date': last_updated,
        'Has_Red_List': False,
        'Has_Yellow_List': False,
        'Has_Green_List': False,
        'URL': url
    }
    
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        response = requests.get(url, headers=headers, timeout=20)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Extract textual content
        text_content = soup.get_text(separator='\n', strip=True)

        # 1. OASIS Charge Code Line
        # Regex to find the line starting with "OASIS charge code" (case insensitive)
        match = re.search(r'(OASIS charge code:.*?)(?:\n|$)', text_content, re.IGNORECASE)
        if match:
            result['OASIS_Charge_Code_Line'] = match.group(1).strip()
        
        # 2. Check for List Types
        result['Has_Red_List'] = bool(re.search(r'Red\s*List', text_content, re.IGNORECASE))
        result['Has_Yellow_List'] = bool(re.search(r'Yellow\s*List', text_content, re.IGNORECASE))
        result['Has_Green_List'] = bool(re.search(r'Green\s*List', text_content, re.IGNORECASE))

        # 3. Product Description
        # Look for h3 "Product Description:" and get the next sibling text
        prod_desc = "N/A"
        # Try finding the specific header
        prod_header = soup.find('h3', string=re.compile(r'Product Description', re.IGNORECASE))
        if prod_header:
            # Get the content of the next sibling (likely a div or p)
            # We iterate next_siblings to skip newlines/empty strings
            for sibling in prod_header.find_next_siblings():
                if sibling.name: # Ensure it's a tag
                    text = sibling.get_text(strip=True)
                    if text:
                        prod_desc = text
                        break
        else:
            # Fallback: Regex on full text
            pd_match = re.search(r'Product Description:\s*\n*(.*?)(?:\n\n|\n[A-Z]|$)', text_content, re.IGNORECASE | re.DOTALL)
            if pd_match:
                prod_desc = pd_match.group(1).strip()[:500]
        
        result['Product_Description'] = prod_desc
                        
    except Exception as e:
        logger.debug(f"Parsing error on {url}: {e}")
    
    # We return a list to match previous structure (though it's 1:1 now)
    return [result]

def main():
    start_time = time.time()
    
    # Ensure output dir
    if not OUTPUT_DIR.exists():
        OUTPUT_DIR.mkdir(parents=True)
        logger.info(f"Created directory: {OUTPUT_DIR}")

    # 1. Get Links & Metadata from Master List (Web)
    new_alerts_meta = get_all_alerts_metadata()
    if not new_alerts_meta:
        logger.error("No alerts found from master list. Exiting.")
        return

    # Convert to DataFrame for easier handling
    df_new = pd.DataFrame(new_alerts_meta)
    
    # Required columns for detailed info (initially empty/NaN for new records)
    detail_cols = ['OASIS_Charge_Code_Line', 'Product_Description', 'Has_Red_List', 'Has_Yellow_List', 'Has_Green_List', 'Last_Updated_Date']
    for col in detail_cols:
        if col not in df_new.columns:
            df_new[col] = None

    # Flag for downstream collectors
    df_new['Is_New_Or_Updated'] = False
    
    # User Control Flag: IsCollect (Default True)
    df_new['IsCollect'] = True
    
    # Manual Override Columns (Default None)
    # User can fill these in parquet to valid fuzzy matching
    df_new['Manual_Hazard_Item'] = None
    df_new['Manual_Product_Type'] = None
    df_new['Manual_Class_M'] = None # Hazard Class M (Category)
    df_new['Manual_Class_L'] = None # Hazard Class L (Top Category)

    # 2. Load Existing Data (for Change Detection)
    updates_needed = []
    
    if OUTPUT_FILE.exists():
        try:
            df_old = pd.read_parquet(OUTPUT_FILE)
            logger.info(f"Loaded existing index with {len(df_old)} records for comparison.")
            
            # Create a lookup dictionary for fast access: Alert_No -> Record Dict
            old_map = df_old.set_index('Alert_No').to_dict('index')
            
            # Iterate through new metadata to identity changes
            for index, row in df_new.iterrows():
                alert_no = row['Alert_No']
                new_date = row['Publish_Date']
                
                old_record = old_map.get(alert_no)
                
                should_update = False
                
                if not old_record:
                    # Case 1: New Alert -> Trigger full scraping
                    should_update = True
                    df_new.at[index, 'Is_New_Or_Updated'] = True
                    # logger.info(f"New Alert detected: {alert_no}")
                else:
                    # PRESERVE USER FLAGS: IsCollect, IsManual
                    df_new.at[index, 'IsCollect'] = bool(old_record.get('IsCollect', True))
                    df_new.at[index, 'Manual_Hazard_Item'] = old_record.get('Manual_Hazard_Item')
                    df_new.at[index, 'Manual_Product_Type'] = old_record.get('Manual_Product_Type')
                    df_new.at[index, 'Manual_Class_M'] = old_record.get('Manual_Class_M')
                    df_new.at[index, 'Manual_Class_L'] = old_record.get('Manual_Class_L')

                    # CRITICAL: Preserve existing metadata (Title, OASIS, Product Description)
                    # We do not want to patch/overwrite these fields for existing records.
                    df_new.at[index, 'Title'] = old_record.get('Title', row['Title'])
                    for col in detail_cols:
                        if col != 'Last_Updated_Date': # We handle date separately
                            df_new.at[index, col] = old_record.get(col)

                    if old_record.get('Publish_Date') != new_date:
                        # Case 2: Date Changed -> Only update date and mark for downstream collector
                        # But skip should_update = True (no detail scraping)
                        df_new.at[index, 'Is_New_Or_Updated'] = True
                        df_new.at[index, 'Last_Updated_Date'] = new_date
                        logger.info(f"Date updated for {alert_no}: {old_record.get('Publish_Date')} -> {new_date} (Metadata preserved)")
                    else:
                        # Case 3: No Change
                        df_new.at[index, 'Last_Updated_Date'] = old_record.get('Publish_Date')
                        df_new.at[index, 'Is_New_Or_Updated'] = False

                if should_update:
                    # OPTIMIZATION: If IsCollect is False, do NOT scrape details.
                    if not df_new.at[index, 'IsCollect']:
                        logger.info(f"Skipping update scrape for blocked alert: {alert_no}")
                        continue
                        
                    # Add metadata dict to processing list for parse_alert_page
                    updates_needed.append(row.to_dict())
            
        except Exception as e:
            logger.error(f"Error loading existing parquet: {e}. Rescraping all.")
            df_new['Is_New_Or_Updated'] = True # Force update all
            updates_needed = new_alerts_meta # Fallback: Scrape all
    else:
        logger.info("No existing index found. Scarping all.")
        df_new['Is_New_Or_Updated'] = True # All are new
        updates_needed = new_alerts_meta

    # 3. Process Needed Updates in Parallel
    if updates_needed:
        logger.info(f"📉 Incremental Update: Parsing {len(updates_needed)} alerts out of {len(df_new)} total.")
        
        updated_records = []
        max_workers = 20
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            # parse_alert_page returns a list of 1 dict
            future_to_meta = {executor.submit(parse_alert_page, meta): meta for meta in updates_needed}
            
            completed = 0
            for future in concurrent.futures.as_completed(future_to_meta):
                try:
                    results = future.result()
                    if results:
                        updated_records.extend(results)
                except Exception as exc:
                    logger.error(f"Error fetching details: {exc}")
                
                completed += 1
                if completed % 20 == 0:
                    logger.info(f"Progress: {completed}/{len(updates_needed)} parsed.")

        # 4. Merge Updates into Main DataFrame
        # updated_records contains dictionaries with keys like 'Alert_No', 'OASIS...', etc.
        if updated_records:
            df_updates = pd.DataFrame(updated_records)
            
            # Update df_new with values from df_updates based on Alert_No
            # We use set_index and update for efficient merging
            df_new.set_index('Alert_No', inplace=True)
            df_updates.set_index('Alert_No', inplace=True)
            
            # Update columns provided by the parser
            df_new.update(df_updates)
            
            # Reset index
            df_new.reset_index(inplace=True)
    else:
        logger.info("✅ No updates needed. All records are up-to-date.")

    # 5. Save Data
    # Basic cleanup
    # Ensure correct types
    df_new['Alert_No'] = df_new['Alert_No'].astype(str)
    
    logger.info(f"Saving {len(df_new)} rows to {OUTPUT_FILE}")
    df_new.to_parquet(OUTPUT_FILE, index=False)
    
    # Optional: Save CSV
    csv_path = str(OUTPUT_FILE).replace('.parquet', '.csv')
    df_new.to_csv(csv_path, index=False)
    logger.info(f"Also saved CSV to {csv_path}")

    elapsed = time.time() - start_time
    logger.info(f"Job finished in {elapsed:.2f} seconds.")

if __name__ == "__main__":
    main()
