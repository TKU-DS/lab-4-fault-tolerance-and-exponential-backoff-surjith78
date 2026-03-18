import time
import random
import json
import os

# ==========================================
# Data Engineering - Lab 4: System Fault Tolerance
# ==========================================

DLQ_FILE = "dead_letter_queue.json"
MAX_RETRIES = 5
BASE_DELAY = 1.0  # seconds

def cloud_api_mock(payload):
    """
    Simulates a highly unstable Cloud API.
    - 70% chance of success (HTTP 200)
    - 20% chance of Rate Limit (HTTP 429)
    - 10% chance of Server Crash (HTTP 500)
    """
    roll = random.random()
    if roll < 0.70:
        return True, "200 OK"
    elif roll < 0.90:
        return False, "429 Too Many Requests"
    else:
        return False, "500 Internal Server Error"

def save_to_dlq(payload):
    """[Fallback] Saves the failed payload to a local Dead Letter Queue (Disk)."""
    if os.path.exists(DLQ_FILE):
        with open(DLQ_FILE, 'r') as f:
            dlq = json.load(f)
    else:
        dlq = []
        
    dlq.append({
        "timestamp": time.time(),
        "payload": payload
    })
    
    with open(DLQ_FILE, 'w') as f:
        json.dump(dlq, f, indent=4)
    print(f"      [DLQ] Payload safely written to local disk ({DLQ_FILE}).")

def upload_with_backoff(payload):
    """
    [Robust Architecture] Implements Exponential Backoff with Jitter.
    """
    print(f"\n[*] Attempting to upload: {payload}")
    
    for attempt in range(1, MAX_RETRIES + 1):
        success, status = cloud_api_mock(payload)
        
        if success:
            print(f"    -> [Success] {status} on attempt {attempt}")
            return True
            
        print(f"    -> [Failed] {status} on attempt {attempt}")
        
        if attempt < MAX_RETRIES:
            # TODO 1: Calculate Exponential Backoff
            # Hint: backoff = BASE_DELAY * (2 ** (attempt - 1))
            backoff = BASE_DELAY * (2 ** (attempt - 1))
            
            # TODO 2: Add Jitter (Randomness)
            # Hint: Use random.uniform(0, 0.5) to generate jitter
            jitter = random.uniform(0, 0.5)
            
            # TODO 3: Calculate total sleep time and pause the execution
            # Hint: sleep_time = backoff + jitter
            sleep_time = backoff + jitter
            time.sleep(sleep_time)
            pass # Remove this pass when implementing
            
    # If we exhaust all retries, do not crash!
    print("    -> [Fatal] Max retries reached. Triggering DLQ fallback.")
    
    # TODO 4: Call the DLQ function to save the payload locally
    save_to_dlq(payload)
    return False

if __name__ == "__main__":
    print("=== Edge Pipeline: Fault Tolerance Test ===")
    
    if os.path.exists(DLQ_FILE):
        os.remove(DLQ_FILE)
        
    test_payloads = [
        {"sensor": "temp", "value": 25.4},
        {"sensor": "temp", "value": 26.1},
        {"sensor": "temp", "value": 25.9}
    ]
    
    for payload in test_payloads:
        upload_with_backoff(payload)
        
    print("\n===========================================")
    print("Experiment completed. Check if dead_letter_queue.json exists if any payload failed completely.")