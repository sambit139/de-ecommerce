
import pandas as pd
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

SOURCE = PROJECT_ROOT / "data" / "processed"
DESTINATION = PROJECT_ROOT / "data" / "ingested"

datasets = [
    "prepared_clickstream",
    "prepared_customer",
    "prepared_product"
]

print("BATCH INGESTION VERIFICATION")
print("=" * 50)

for name in datasets:
    csv_file = SOURCE / f"{name}.csv"
    parquet_file = DESTINATION / f"{name}.parquet"

    original = pd.read_csv(csv_file)
    ingested = pd.read_parquet(parquet_file)

    print(f"\nDataset: {name}")
    print("Original CSV rows:", len(original))
    print("Parquet rows:", len(ingested))
    print("Row count matches:", len(original) == len(ingested))
    print("Ingestion timestamp present:",
          "ingestion_timestamp" in ingested.columns)

print("\nVerification completed!")
