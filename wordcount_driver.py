import re, os, math, time
from collections import defaultdict
from multiprocessing import Pool, cpu_count

DATASET_FILE   = "branded_food.txt"
TOP_N          = 20
BLOCK_SIZE_MB  = 128

STOP_WORDS = {
    "the","and","a","an","in","of","to","is","it","that","this","was",
    "for","on","are","as","with","be","at","by","from","or","but","not",
    "have","had","has","he","she","they","we","you","i","its","do","did",
    "will","so","if","up","out","about","no","can","all","been","s","g"
}

def analyse_blocks(filepath):
    size_bytes = os.path.getsize(filepath)
    size_mb    = size_bytes / (1024 * 1024)
    num_blocks = math.ceil(size_mb / BLOCK_SIZE_MB)
    return size_bytes, size_mb, num_blocks

def map_block(lines):
    pairs = defaultdict(int)
    for line in lines:
        for word in re.findall(r'[a-z]+', line.lower()):
            if word not in STOP_WORDS and len(word) > 1:
                pairs[word] += 1
    return dict(pairs)

def reduce_counts(partial_list):
    total = defaultdict(int)
    for partial in partial_list:
        for word, count in partial.items():
            total[word] += count
    return total

if __name__ == "__main__":
    print("=" * 65)
    print("  Hadoop MapReduce — Word Count with Stop-Word Filtering")
    print("  Dataset : USDA Branded Food  (branded_food.txt)")
    print("=" * 65)

    try:
        with open(DATASET_FILE, "r", encoding="utf-8", errors="ignore") as f:
            all_lines = f.readlines()
        size_bytes, size_mb, num_blocks = analyse_blocks(DATASET_FILE)
        print(f"\n[HDFS] File size         : {size_mb:.2f} MB  ({size_bytes:,} bytes)")
        print(f"[HDFS] Block size        : {BLOCK_SIZE_MB} MB")
        print(f"[HDFS] Blocks created    : {num_blocks}")
        print(f"[HDFS] Total lines       : {len(all_lines):,}")
    except FileNotFoundError:
        print(f"\n[WARN] '{DATASET_FILE}' not found – using built-in sample.\n")
        all_lines = [
            "id,fdc_id,branded_food_category,brand_owner,ingredients",
            "1,100,Snack,General Mills,WHOLE GRAIN OATS SUGAR CORN STARCH SALT",
            "2,101,Beverage,Coca Cola,CARBONATED WATER CORN SYRUP CARAMEL COLOR CAFFEINE",
            "3,102,Dairy,Kraft,CULTURED MILK CREAM CHEESE CULTURE SALT",
            "4,103,Bakery,Pepperidge,ENRICHED FLOUR WHEAT NIACIN IRON WATER SUGAR YEAST SALT",
            "5,104,Condiment,Heinz,TOMATOES VINEGAR CORN SYRUP SALT ONION POWDER SPICES",
            "6,105,Snack,Frito Lay,POTATOES CORN OIL SALT MALTODEXTRIN SUGAR ONION POWDER",
            "7,106,Cereal,Kellogg,WHOLE GRAIN WHEAT SUGAR SALT MALT FLAVOR VITAMINS IRON",
            "8,107,Sauce,Campbell,TOMATO PUREE WATER CORN STARCH SALT SUGAR ONION POWDER",
            "9,108,Dairy,Dannon,CULTURED GRADE MILK SUGAR WATER CORN STARCH PECTIN",
            "10,109,Snack,Nabisco,ENRICHED FLOUR SUGAR PALM OIL SALT CORN STARCH SODA",
        ] * 500  # simulate larger dataset
        size_bytes = sum(len(l.encode()) for l in all_lines)
        size_mb    = size_bytes / (1024 * 1024)
        num_blocks = max(1, math.ceil(size_mb / BLOCK_SIZE_MB))
        print(f"[HDFS] Simulated size    : {size_mb:.4f} MB")
        print(f"[HDFS] Blocks (simulated): {num_blocks}")

    lines_per_block = max(1, math.ceil(len(all_lines) / max(num_blocks, 1)))
    blocks = [all_lines[i:i+lines_per_block]
              for i in range(0, len(all_lines), lines_per_block)]

    print(f"\n[MAP]  Splitting into {len(blocks)} block(s), "
          f"~{lines_per_block:,} lines each")

    t0 = time.time()
    workers = min(cpu_count(), len(blocks))
    print(f"[MAP]  Launching {workers} parallel mapper(s)…")
    with Pool(workers) as pool:
        partial_results = pool.map(map_block, blocks)
    print(f"[MAP]  Done in {time.time()-t0:.2f}s  "
          f"({len(partial_results)} partial results)")

    print("[SHUFFLE] Grouping and sorting intermediate keys…")

    t1 = time.time()
    word_counts = reduce_counts(partial_results)
    print(f"[REDUCE] Aggregated {len(word_counts):,} unique words "
          f"in {time.time()-t1:.2f}s")

    top_n = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:TOP_N]

    print(f"\n{'─'*40}")
    print(f"  TOP {TOP_N} MOST FREQUENT WORDS")
    print(f"{'─'*40}")
    print(f"  {'Rank':<6}{'Word':<25}{'Count':>8}")
    print(f"{'─'*40}")
    for rank, (word, count) in enumerate(top_n, 1):
        print(f"  {rank:<6}{word:<25}{count:>8,}")
    print(f"{'─'*40}")
    print(f"\n[DONE] Total unique words: {len(word_counts):,}")
    print(f"[DONE] Total wall time   : {time.time()-t0:.2f}s")

    with open("wordcount_output.txt", "w") as out:
        out.write(f"{'Word':<30}{'Count':>10}\n")
        out.write("-" * 40 + "\n")
        for word, count in sorted(word_counts.items(), key=lambda x: x[1], reverse=True):
            out.write(f"{word:<30}{count:>10}\n")
    print("[DONE] Full output saved → wordcount_output.txt")