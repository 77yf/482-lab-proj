import threading
import time
import random
import queue

NUM_PROCESSES = 3
ROUNDS        = 2

token_holder  = 0
token_lock    = threading.Lock()
channels      = [queue.Queue() for _ in range(NUM_PROCESSES)]
print_lock    = threading.Lock()

log_lines = []

def log(msg):
    with print_lock:
        timestamp = f"[t={time.time()-start:.2f}s]"
        line = f"  {timestamp}  {msg}"
        print(line)
        log_lines.append(line)

def process(rank):
    name = f"P{rank + 1}"

    for round_num in range(1, ROUNDS + 1):
        log(f"{name}  waiting for token…")
        token = channels[rank].get()
        log(f"{name}  ✓ received token  (round {round_num})")

        wants_to_print = random.choice([True, True, False])
        if wants_to_print:
            log(f"{name}  → REQUEST  to print (round {round_num})")
            time.sleep(random.uniform(0.1, 0.3))
            log(f"{name}  ▶ PRINTING  document (round {round_num})  ◀")
            time.sleep(random.uniform(0.1, 0.2))
            log(f"{name}  ✔ DONE printing, releasing printer")
        else:
            log(f"{name}  (no print request this round, passing token)")

        successor = (rank + 1) % NUM_PROCESSES
        time.sleep(0.05)
        log(f"{name}  → passing token to P{successor + 1}")
        channels[successor].put("TOKEN")

    log(f"{name}  finished all rounds.")

print("=" * 65)
print("  Problem 3 – Token-Based Distributed Mutual Exclusion")
print(f"  {NUM_PROCESSES} processes  |  Ring topology  |  Shared printer")
print("=" * 65)
print(f"\n  Ring: P1 → P2 → P3 → P1  (token circulates)")
print(f"  Each process runs {ROUNDS} round(s)\n")

start = time.time()

channels[0].put("TOKEN")

threads = [threading.Thread(target=process, args=(r,), daemon=True)
           for r in range(NUM_PROCESSES)]
for t in threads:
    t.start()
for t in threads:
    t.join(timeout=30)

print(f"\n  Total time: {time.time()-start:.2f}s")
print("=" * 65)
print("\n  MUTUAL EXCLUSION GUARANTEE:")
print("  Only the process holding the token can enter the")
print("  critical section (printer access) at any given time.")
print("  No two processes ever print simultaneously.")
print("=" * 65)