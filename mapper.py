import sys
import re

STOP_WORDS = {
    "the","and","a","an","in","of","to","is","it","that","this","was",
    "for","on","are","as","with","be","at","by","from","or","but","not",
    "have","had","has","he","she","they","we","you","i","its","do","did",
    "will","so","if","up","out","about","no","can","all","been","s","g"
}

for line in sys.stdin:
    line = line.strip().lower()
    words = re.findall(r'[a-z]+', line)
    for word in words:
        if word not in STOP_WORDS and len(word) > 1:
            print(f"{word}\t1")