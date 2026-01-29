
import pandas as pd
from pathlib import Path

def create_mapping_file():
    # Defining the Normalized Mapping with English Names
    data = [
        {"FDA_CODE": "02", "ENG_NM": "Whole Grains/Milled Grain Products/Starches", "KOR_NM": "곡류"},
        {"FDA_CODE": "03", "ENG_NM": "Bakery Products/Dough/Mixes/Icings", "KOR_NM": "빵류"},
        {"FDA_CODE": "04", "ENG_NM": "Macaroni/Noodle Products", "KOR_NM": "면류"},
        {"FDA_CODE": "05", "ENG_NM": "Cereal Preparations/Breakfast Foods", "KOR_NM": "곡류가공품"},
        {"FDA_CODE": "07", "ENG_NM": "Snack Food Items", "KOR_NM": "과자류"},
        {"FDA_CODE": "09", "ENG_NM": "Milk/Butter/Dried Milk Products", "KOR_NM": "유가공품"},
        {"FDA_CODE": "12", "ENG_NM": "Cheese/Cheese Products", "KOR_NM": "자연치즈"},
        {"FDA_CODE": "13", "ENG_NM": "Ice Cream and related Products", "KOR_NM": "빙과류"},
        {"FDA_CODE": "14", "ENG_NM": "Filled Milk/Imitation Milk Products", "KOR_NM": "기타음료"},
        {"FDA_CODE": "15", "ENG_NM": "Eggs/Egg Products", "KOR_NM": "알가공품"},
        {"FDA_CODE": "16", "ENG_NM": "Fishery/Seafood Products", "KOR_NM": "수산물"},
        {"FDA_CODE": "17", "ENG_NM": "Meats, Meat Products and Poultry", "KOR_NM": "축산물"},
        {"FDA_CODE": "18", "ENG_NM": "Vegetable Protein Products", "KOR_NM": "두부류 또는 묵류"},
        {"FDA_CODE": "20", "ENG_NM": "Fruit/Fruit Products", "KOR_NM": "과일류"},
        {"FDA_CODE": "21", "ENG_NM": "Fruit/Fruit Products", "KOR_NM": "과일류"},
        {"FDA_CODE": "22", "ENG_NM": "Fruit/Fruit Products", "KOR_NM": "과일류"},
        {"FDA_CODE": "23", "ENG_NM": "Nuts/Edible Seeds", "KOR_NM": "땅콩 또는 견과류"},
        {"FDA_CODE": "24", "ENG_NM": "Vegetables/Vegetable Products", "KOR_NM": "채소류"},
        {"FDA_CODE": "25", "ENG_NM": "Vegetables/Vegetable Products", "KOR_NM": "채소류"},
        {"FDA_CODE": "26", "ENG_NM": "Vegetable Oils", "KOR_NM": "식용유지류"},
        {"FDA_CODE": "27", "ENG_NM": "Dressing/Condiments", "KOR_NM": "드레싱류"},
        {"FDA_CODE": "28", "ENG_NM": "Spices, Flavors And Salts", "KOR_NM": "식품첨가물"},
        {"FDA_CODE": "29", "ENG_NM": "Drinks, Soft Drinks, and Waters", "KOR_NM": "음료류"},
        {"FDA_CODE": "30", "ENG_NM": "Beverage Bases/Concentrates/Nectars", "KOR_NM": "음료류"},
        {"FDA_CODE": "31", "ENG_NM": "Coffee/Tea", "KOR_NM": "다류"},
        {"FDA_CODE": "32", "ENG_NM": "Alcoholic Beverages", "KOR_NM": "주류"},
        {"FDA_CODE": "33", "ENG_NM": "Candy W/O Chocolate/Candy Specialties/Chewing Gums", "KOR_NM": "캔디류"},
        {"FDA_CODE": "34", "ENG_NM": "Chocolate/Cocoa Products/Cocoa beans", "KOR_NM": "초콜릿류"},
        {"FDA_CODE": "35", "ENG_NM": "Gelatin/Rennet/Pudding Mixes/Pie Fillings", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "36", "ENG_NM": "Food Sweeteners/Nutritive syrups/honey/molasses", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "37", "ENG_NM": "Multiple food dinners/Gravies/Sauces/Specialties", "KOR_NM": "소스"},
        {"FDA_CODE": "38", "ENG_NM": "Soups", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "39", "ENG_NM": "Prepared Salad Products", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "40", "ENG_NM": "Baby Food Products", "KOR_NM": "특수용도식품"},
        {"FDA_CODE": "41", "ENG_NM": "Dietary Conv Food/Meal Replacements", "KOR_NM": "건강기능식품"},
        {"FDA_CODE": "42", "ENG_NM": "EDIBLE INSECTS AND INSECT-DERIVED FOODS", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "45", "ENG_NM": "Food Additives (Human Use)", "KOR_NM": "식품첨가물"},
        {"FDA_CODE": "46", "ENG_NM": "Food Additives (Human Use)", "KOR_NM": "식품첨가물"},
        {"FDA_CODE": "47", "ENG_NM": "Multiple Food Warehouses", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "50", "ENG_NM": "Color Additiv Food/Drug/Cosmetic", "KOR_NM": "식품첨가물"},
        {"FDA_CODE": "51", "ENG_NM": "Food Service/Conveyance", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "52", "ENG_NM": "Miscellaneous Food Related Items", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "53", "ENG_NM": "Cosmetics", "KOR_NM": "화장품"},
        {"FDA_CODE": "54", "ENG_NM": "Vitamins/Minerals (Human/Animal); Proteins", "KOR_NM": "건강기능식품"},
        {"FDA_CODE": "55", "ENG_NM": "Pharm Necess & Ctnr For Drug/Bio", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "56", "ENG_NM": "Antibiotics (Human/Animal)", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "57", "ENG_NM": "Bio & Licensed In-Vivo & In-Vitro Diag", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "58", "ENG_NM": "Human/Animal Therapeutic Biologic", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "59", "ENG_NM": "Multiple Drug Warehouses", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "60", "ENG_NM": "Human and Animal Drugs", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "61", "ENG_NM": "Human and Animal Drugs", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "62", "ENG_NM": "Human and Animal Drugs", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "63", "ENG_NM": "Human and Animal Drugs", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "64", "ENG_NM": "Human and Animal Drugs", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "65", "ENG_NM": "Human and Animal Drugs", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "66", "ENG_NM": "Human and Animal Drugs", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "67", "ENG_NM": "Type A Medicated Articles", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "68", "ENG_NM": "Animal Devices and Diagnostic Products", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "69", "ENG_NM": "Medicated Animal Feeds", "KOR_NM": "사료"},
        {"FDA_CODE": "70", "ENG_NM": "Animal Food(Non-Medicated Feed)", "KOR_NM": "사료"},
        {"FDA_CODE": "71", "ENG_NM": "Byprodcts For Animal Foods", "KOR_NM": "사료"},
        {"FDA_CODE": "72", "ENG_NM": "Pet/Laboratory Animal Food", "KOR_NM": "사료"},
        {"FDA_CODE": "73", "ENG_NM": "Anesthesiology", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "74", "ENG_NM": "Cardiovascular", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "75", "ENG_NM": "Chemistry", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "76", "ENG_NM": "Dental", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "77", "ENG_NM": "Ear,Nose And Throat", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "78", "ENG_NM": "Gastroenterological & Urological", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "79", "ENG_NM": "General & Plastic Surgery", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "80", "ENG_NM": "General Hospital/Personal Use", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "81", "ENG_NM": "Hematology", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "82", "ENG_NM": "Immunology", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "83", "ENG_NM": "Microbiology", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "84", "ENG_NM": "Neurological", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "85", "ENG_NM": "Obstetrical & Gynecological", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "86", "ENG_NM": "Ophthalmic", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "87", "ENG_NM": "Orthopedic", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "88", "ENG_NM": "Pathology", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "89", "ENG_NM": "Physical Medicine", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "90", "ENG_NM": "Radiology", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "91", "ENG_NM": "Toxicology", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "92", "ENG_NM": "Molecular Genetics", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "94", "ENG_NM": "Ionizing Non-Medical Devices", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "95", "ENG_NM": "Light Emitting Non-Device Products", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "96", "ENG_NM": "Radio Frequency Emitting Products", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "97", "ENG_NM": "Sound Emitting Products", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "98", "ENG_NM": "Tobacco Products", "KOR_NM": "기타가공품"},
        {"FDA_CODE": "99", "ENG_NM": "Bio/Anim Drug/Feed&Food/Med Dev", "KOR_NM": "기타가공품"},
    ]

    df = pd.DataFrame(data)
    
    # --- Hierarchy Enrichment ---
    master_path = Path("data/reference/product_code_master.parquet")
    if master_path.exists():
        print(f"🔍 Enriching with hierarchy from {master_path}...")
        master_df = pd.read_parquet(master_path)
        
        # 1. Create a code -> name map for hierarchy lookup
        code_to_name = master_df.set_index('PRDLST_CD')['KOR_NM'].to_dict()
        
        # 2. Preparation for merge: Get unique entries for each name
        # We drop duplicates by KOR_NM to avoid fan-out, prioritizing the shortest code or first entry
        master_subset = master_df[['KOR_NM', 'PRDLST_CD', 'HRNK_PRDLST_CD', 'HTRK_PRDLST_CD']].drop_duplicates(subset=['KOR_NM'])
        
        # 3. Merge hierarchy codes
        df = df.merge(master_subset, on='KOR_NM', how='left')
        
        # 4. Map parent and top-level names using the code_to_name dictionary
        df['HRNK_KOR_NM'] = df['HRNK_PRDLST_CD'].map(code_to_name)
        df['HTRK_KOR_NM'] = df['HTRK_PRDLST_CD'].map(code_to_name)
        
        # Rename columns for clarity if needed, or keep as is
        # Reorder columns to a logical flow
        cols = ['FDA_CODE', 'ENG_NM', 'KOR_NM', 'PRDLST_CD', 'HRNK_KOR_NM', 'HRNK_PRDLST_CD', 'HTRK_KOR_NM', 'HTRK_PRDLST_CD']
        # Filter only existing columns in case of join misses
        df = df[[c for c in cols if c in df.columns]]
    else:
        print(f"⚠️ Master file not found at {master_path}, skipping enrichment.")

    out_path = Path("data/reference/fda_product_code_mapping.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(out_path, index=False)
    print(f"✅ Enriched mapping file saved to {out_path} ({len(df)} rows)")
    print(df.head())

if __name__ == "__main__":
    create_mapping_file()
