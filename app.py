import gzip
import json
import os
from collections import defaultdict

# --- CONFIGURATION ---
INPUT_FILENAME = 'openfoodfacts-products.jsonl.gz'
OUTPUT_PRODUCTS = 'pakistani_grocery_products.json'
OUTPUT_BRANDS = 'pakistani_brands_summary.json'

def process_data():
    if not os.path.exists(INPUT_FILENAME):
        print(f"Error: '{INPUT_FILENAME}' not found.")
        return

    print("Processing data... This will generate 2 files.")
    
    # In-memory storage for brand aggregation
    # Structure: {'Brand Name': ['Product A', 'Product B']}
    brand_map = defaultdict(list)
    
    count = 0

    # Open the compressed input and the first output file
    with gzip.open(INPUT_FILENAME, 'rt', encoding='utf-8') as f_in, \
         open(OUTPUT_PRODUCTS, 'w', encoding='utf-8') as f_prod:
        
        f_prod.write("[\n") # Start Product JSON Array
        first_item = True
        
        for line_number, line in enumerate(f_in):
            try:
                product = json.loads(line)
                
                # --- STEP 1: IDENTIFY PAKISTANI ITEMS ---
                code = product.get('code', '')
                
                # Create a search string of all location-related fields
                search_text = (
                    str(product.get('countries_tags', [])) + " " +
                    str(product.get('manufacturing_places_tags', [])) + " " +
                    str(product.get('origins_tags', [])) + " " +
                    str(product.get('purchase_places_tags', [])) + " " +
                    str(product.get('labels_tags', []))
                ).lower()

                is_pakistan = False
                if code.startswith('896') or 'pakistan' in search_text:
                    is_pakistan = True

                # --- STEP 2: SAVE PRODUCT & AGGREGATE BRANDS ---
                if is_pakistan:
                    # A. Write to Products File
                    if not first_item:
                        f_prod.write(",\n")
                    json.dump(product, f_prod)
                    first_item = False
                    
                    # B. Aggregate for Brands File
                    # Get product name
                    p_name = product.get('product_name', 'Unknown Product')
                    
                    # Get brands (often comma separated like "Nestle, Milkpak")
                    raw_brands = product.get('brands', 'Unknown Brand')
                    if raw_brands:
                        # Split by comma to handle multiple brands
                        brand_list = [b.strip() for b in raw_brands.split(',')]
                        for brand in brand_list:
                            if brand: # Skip empty strings
                                brand_map[brand].append(p_name)
                    else:
                        brand_map['Unknown Brand'].append(p_name)

                    count += 1
            
            except json.JSONDecodeError:
                continue

            if line_number > 0 and line_number % 100000 == 0:
                print(f"Scanned {line_number} records... Found {count} Pakistani items.")

        f_prod.write("\n]") # Close Product JSON Array

    print(f"\nPhase 1 Complete. Saved {count} products to {OUTPUT_PRODUCTS}.")
    print("Phase 2: Generating Brand Summary...")

    # --- STEP 3: CREATE THE BRAND SUMMARY JSON ---
    brand_summary_list = []
    
    # Convert dictionary to the requested List format
    for brand_name, products_list in brand_map.items():
        # Remove duplicates from product names if desired
        unique_products = list(set(products_list))
        
        entry = {
            "parent_company": brand_name,
            "product_count": len(unique_products),
            "products": unique_products
        }
        brand_summary_list.append(entry)

    # Sort by count (highest first) so you see top brands at the top
    brand_summary_list.sort(key=lambda x: x['product_count'], reverse=True)

    # Write the second file
    with open(OUTPUT_BRANDS, 'w', encoding='utf-8') as f_brands:
        json.dump(brand_summary_list, f_brands, indent=4)

    print(f"Phase 2 Complete. Brand summary saved to {OUTPUT_BRANDS}.")

if __name__ == "__main__":
    process_data()