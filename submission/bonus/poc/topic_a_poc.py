# NB: Topic A PoC — LLM Observability at Scale
# This PoC demonstrates:
# 1. PII Redaction at Bronze -> Silver transition.
# 2. Z-Ordering for high-cardinality multi-tenant queries.
# 3. Cost-effective Medallion layout.

import os
import sys
import time
import re
import polars as pl
from deltalake import DeltaTable, write_deltalake
from datetime import datetime, timedelta

# Add parent scripts to path for lakehouse helpers if needed
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "..", "..", "scripts"))
from lakehouse import path, reset

# Config paths using the helper
BRONZE = path("bronze", "bonus_logs")
SILVER = path("silver", "bonus_logs")
GOLD   = path("gold", "bonus_metrics")

def generate_mock_logs(n=10000):
    """Generates mock LLM logs with PII (email/phones)."""
    data = []
    tenants = [f"tenant_{i:03d}" for i in range(50)] # 50 distinct tenants
    
    for i in range(n):
        # Injected PII
        pii_email = f"user_{i}@gmail.com" if i % 10 == 0 else ""
        pii_phone = f"0912{i:06d}" if i % 15 == 0 else ""
        
        prompt = f"Hello, my email is {pii_email}. My phone is {pii_phone}. Help me with X."
        
        data.append({
            "request_id": f"req_{i}",
            "tenant_id": tenants[i % 50],
            "ts": datetime.now() - timedelta(minutes=i % 1000),
            "prompt": prompt,
            "latency_ms": 100 + (i % 500),
            "tokens": 50 + (i % 200),
            "cost_usd": (50 + (i % 200)) * 0.00002
        })
    return pl.DataFrame(data)

def redact_pii(text):
    """Simple regex redaction for Email and Phone."""
    text = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[EMAIL_REDACTED]', text)
    text = re.sub(r'\d{10,11}', '[PHONE_REDACTED]', text)
    return text

# --- Step 1: Bronze (Raw) ---
print("--- Step 1: Bronze Landing ---")
reset(BRONZE)
raw_data = generate_mock_logs(20000)
write_deltalake(BRONZE, raw_data.to_arrow(), mode="overwrite")
print(f"Bronze written: {len(raw_data)} rows")

# --- Step 2: Silver (Redacted & Z-Ordered) ---
print("\n--- Step 2: Silver (Redaction + Z-Order) ---")
reset(SILVER)

# Read from Bronze
bronze_df = pl.from_arrow(DeltaTable(BRONZE).to_pyarrow_table())

# Redaction Logic
silver_df = bronze_df.with_columns([
    pl.col("prompt").map_elements(redact_pii, return_dtype=pl.Utf8).alias("prompt")
])

# Add Date column for partitioning
silver_df = silver_df.with_columns([
    pl.col("ts").cast(pl.Date).alias("date")
])

# Write to Silver
write_deltalake(SILVER, silver_df.to_arrow(), mode="overwrite", partition_by=["date"])

# Apply Z-Order for Tenant-based performance
dt_silver = DeltaTable(SILVER)
print("Applying Z-Order by tenant_id...")
dt_silver.optimize.z_order(["tenant_id"])

print("Silver written and Z-Ordered.")
print("Sample redacted prompt (first 100 chars):")
print(silver_df.select("prompt").head(1).item()[:100])

# --- Step 3: Gold (Aggregates) ---
print("\n--- Step 3: Gold (Aggregates) ---")
reset(GOLD)

# Hourly Aggregates using Polars
gold_df = silver_df.group_by(["tenant_id", "date"]).agg([
    pl.col("latency_ms").mean().alias("avg_latency"),
    pl.col("cost_usd").sum().alias("total_cost"),
    pl.count("request_id").alias("request_count")
])

write_deltalake(GOLD, gold_df.to_arrow(), mode="overwrite")
print(f"Gold metrics written: {len(gold_df)} rows")

# --- Step 4: Benchmark Point Query ---
print("\n--- Step 4: Performance Benchmark ---")
TARGET_TENANT = "tenant_042"

def query_tenant(table_path):
    t0 = time.time()
    dt = DeltaTable(table_path)
    # Simulate a filtered read that leverages file pruning
    res = dt.to_pyarrow_table(filters=[("tenant_id", "=", TARGET_TENANT)])
    return time.time() - t0, len(res)

latency, count = query_tenant(SILVER)
print(f"Filter by {TARGET_TENANT} took: {latency:.4f}s (returned {count} rows)")

print("\nBonus PoC Completed successfully.")
