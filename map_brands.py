import gzip
import json
import re
import os

# Configuration
BRAND_FILE = r'd:\curiologix\barcode\brand.json'
INPUT_FILE = r'd:\curiologix\barcode\openfoodfacts-products.jsonl.gz'
# ... (Imports and config remain same, updated OUTPUT_FILE extension)
OUTPUT_FILE_JSON = r'd:\curiologix\barcode\mapped_products.json'
OUTPUT_COUNTS_JSON = r'd:\curiologix\barcode\brand_counts.json'
OUTPUT_COUNTS_MD = r'd:\curiologix\barcode\brand_counts.md'

def load_brands(filepath):
    """Load and normalize brands from the JSON file."""
    if not os.path.exists(filepath):
        print(f"Error: Brand file '{filepath}' not found.")
        return []
    
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)
        # Handle the specific structure provided
        if "grocery_brands_pakistan" in data:
            return [b.strip() for b in data["grocery_brands_pakistan"] if b.strip()]
        # Fallback if structure changes
        return []

def main():
    # 1. Load Brands
    brands = load_brands(BRAND_FILE)
    if not brands:
        print("No brands found to map.")
        return

    print(f"Loaded {len(brands)} brands for mapping.")
    
    # 2. Compile Regex
    # Pattern: \b(brand1|brand2|...)\b (case insensitive)
    escaped_brands = [re.escape(b) for b in brands]
    pattern_str = r'\b(' + '|'.join(escaped_brands) + r')\b'
    try:
        brand_pattern = re.compile(pattern_str, re.IGNORECASE)
    except re.error as e:
        print(f"Error compiling regex: {e}")
        return

    print("Pattern compiled. Scanning file...")

    # 3. Stream and Filter
    matched_count = 0
    scanned_count = 0
    # Also track "Unknown/Other" if matched by regex but not in initial list perfectly? 
    # Actually, the regex matches the brands in the list. 
    # But we might want to capture exactly which brand matched to increment its count.
    
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        return
    # Create a lower-case to canonical map
    normalized_brands = {b.lower(): b for b in brands}
    brand_counts = {b: 0 for b in brands}

    # Using JSONL for output as it handles large datasets better than a single JSON array
    with gzip.open(INPUT_FILE, 'rt', encoding='utf-8') as f_in, \
         open(OUTPUT_FILE_JSON, 'w', encoding='utf-8') as f_out:
        
        f_out.write('[\n') # Start JSON array
        first_entry = True

        for line in f_in:
            scanned_count += 1
            if scanned_count % 50000 == 0:
                print(f"Scanned {scanned_count} records... Found {matched_count} matches.")

            try:
                product = json.loads(line)
                product_brands = product.get('brands', '')
                
                if not product_brands:
                    continue

                # Check for match
                matches = brand_pattern.findall(product_brands)
                
                if matches:
                    unique_matches = set(m.lower() for m in matches)
                    
                    found_any = False
                    for m in unique_matches:
                        if m in normalized_brands:
                            canonical_name = normalized_brands[m]
                            brand_counts[canonical_name] += 1
                            found_any = True
                    
                    if found_any:
                        if not first_entry:
                            f_out.write(',\n')
                        f_out.write(json.dumps(product))
                        first_entry = False
                        matched_count += 1

            except json.JSONDecodeError:
                continue
            except Exception as e:
                print(f"Error processing line {scanned_count}: {e}")
                continue

        f_out.write('\n]') # End JSON array

    print(f"\nProcessing Complete.")
    print(f"Total Scanned: {scanned_count}")
    print(f"Total Matched: {matched_count}")
    print(f"Mapped products saved to: {OUTPUT_FILE_JSON}")
    
    # Write Counts JSON
    with open(OUTPUT_COUNTS_JSON, 'w', encoding='utf-8') as f:
        # Sort by count descending
        sorted_counts = dict(sorted(brand_counts.items(), key=lambda item: item[1], reverse=True))
        json.dump(sorted_counts, f, indent=4)
    print(f"Brand counts (JSON) saved to: {OUTPUT_COUNTS_JSON}")

    # Write Counts Markdown
    with open(OUTPUT_COUNTS_MD, 'w', encoding='utf-8') as f:
        f.write(f"# Brand Product Counts\n\nTotal Matched Products: {matched_count}\n\n")
        f.write("| Brand | Count |\n")
        f.write("|-------|-------|\n")
        
        # We already sorted for JSON, reuse or re-sort
        sorted_items = sorted(brand_counts.items(), key=lambda item: item[1], reverse=True)
        
        for brand, count in sorted_items:
            # Only list brands with at least 1 product? Or all?
            # User intent likely checking coverage, so all is good, or only non-zero.
            # Let's show all to be safe, or just non-zero if list is huge.
            # Given input list is ~200 items, all is fine.
            if count > 0:
                f.write(f"| {brand} | {count} |\n")
    
    print(f"Brand counts (MD) saved to: {OUTPUT_COUNTS_MD}")


if __name__ == "__main__":
    main()
