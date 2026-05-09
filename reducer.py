import sys
from collections import defaultdict

word_counts = defaultdict(int)

for line in sys.stdin:
    line = line.strip()
    if '\t' not in line:
        continue
    word, count = line.split('\t', 1)
    try:
        word_counts[word] += int(count)
    except ValueError:
        continue

for word, count in sorted(word_counts.items(), key=lambda x: x[1], reverse=True):
    print(f"{word}\t{count}")