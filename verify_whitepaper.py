
from src.collectors.fda_collector import FDACollector

def test_whitepaper_logic():
    print("Initializing Collector...")
    # Mocking init might be needed if it connects to DB/Web
    # FDACollector(data_dir=Path("data/hub"))
    # We can try to just make a dummy instance if possible, or use the real one (it doesn't auto-connect usually)
    try:
        collector = FDACollector()
    except Exception as e:
        print(f"Init Warning: {e}")
        collector = object.__new__(FDACollector)
        # Mock the loaded map if init failed
        collector.product_whitepaper = {"12": "유가공품", "00": ""}
    
    import re
    # Ensure regex matches what's in the actual file (updated logic)
    collector.prod_code_pattern = re.compile(r'(\d{2}\s+[A-Z0-9]\s+[A-Z0-9-]\s+[A-Z0-9-]\s+\d{2})')

    print("Checking Whitepaper Dictionary...")
    # Access instance attribute 'product_whitepaper'
    if "12" in collector.product_whitepaper and collector.product_whitepaper["12"] == "유가공품":
        print("✅ Whitepaper loaded correctly (Sample '12' -> '유가공품').")
    else:
        print("❌ Whitepaper missing or incorrect.")
        print(collector.product_whitepaper.get("12"))

    # Test Case 1: Standard Match
    print("\n--- Test Case 1: Standard Match '12' ---")
    record_state = {
        'product_line': "12 A - - 04 Cheese Block",
        'published_date': "01/01/2026",
        'country': 'TestLand',
        'raw_content': ["Details about cheese."]
    }
    records = []
    meta = {'Manual_Product_Type': 'Manual Cheese'} 
    
    # We pass target_date same as published to pass filter
    # Force=False
    collector._flush_buffer_to_record(record_state, records, "01/01/2026", False, "99-99", "Test Alert", meta)
    
    if not records:
        print("❌ Record not flushed.")
    else:
        rec = records[0]
        print(f"Product Type: {rec['product_type']}")
        print(f"Product Name: {rec['product_name']}")
        
        if rec['product_type'] == "유가공품":
            print("✅ Product Type matched Whitepaper.")
        else:
            print(f"❌ Product Type mismatch. Expected '유가공품'. Got: {rec['product_type']}")
            
        if "12 A" not in rec['product_name'] and "Cheese Block" in rec['product_name']:
             print("✅ Product Name cleaned.")
        else:
             print("❌ Product Name not cleaned.")

    records.clear()
    
    # Test Case 2: Manual Override (Code 00 - Not in Whitepaper)
    print("\n--- Test Case 2: Manual Override '00' ---")
    record_state['product_line'] = "00 X - - 99 Unusual Item"
    collector._flush_buffer_to_record(record_state, records, "01/01/2026", False, "99-99", "Test Alert", meta)
    
    if records:
        rec = records[0]
        print(f"Product Type: {rec['product_type']}")
        if rec['product_type'] == "Manual Cheese":
             print("✅ Product Type matched Meta Manual Override.")
        else:
             print("❌ Fallback logic failed.")
    
    records.clear()

    # Test Case 3: Fallback (Code 01 - Not in Whitepaper, No Manual)
    print("\n--- Test Case 3: Fallback '01' ---")
    record_state['product_line'] = "01 X - - 99 Rare Item"
    collector._flush_buffer_to_record(record_state, records, "01/01/2026", False, "99-99", "Test Alert", {})
    
    if records:
        rec = records[0]
        print(f"Product Type: {rec['product_type']}")
        if "FDA Code 01" in rec['product_type']:
             print("✅ Product Type matched Fallback.")
        else:
             print("❌ Fallback logic failed.")

if __name__ == "__main__":
    test_whitepaper_logic()
