
import pandas as pd
from pathlib import Path
from datetime import datetime, timezone

# Automatically identify the project root
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Input and output folders
SOURCE = PROJECT_ROOT / "data" / "processed"
DESTINATION = PROJECT_ROOT / "data" / "ingested"

# Create destination folder if it doesn't exist
DESTINATION.mkdir(parents=True, exist_ok=True)

# Datasets to ingest
datasets = [
    "prepared_clickstream",
    "prepared_customer",
    "prepared_product"
]

print("=" * 55)
print("ECOMMERCE DATA ENGINEERING - BATCH INGESTION")
print("=" * 55)

print("\nProject root:", PROJECT_ROOT)
print("Source folder:", SOURCE)
print("Source folder exists:", SOURCE.exists())

if not SOURCE.exists():
    raise FileNotFoundError(
        f"Source folder not found: {SOURCE}"
    )

print("\nFiles available:")
for file in SOURCE.iterdir():
    print(" -", file.name)

print("\nStarting batch ingestion...\n")

successful = 0

for name in datasets:

    source_file = SOURCE / f"{name}.csv"

    print("-" * 55)
    print("Processing:", name)

    if not source_file.exists():
        print("ERROR: Dataset not found:", source_file)
        continue

    try:
        # Read prepared CSV dataset
        df = pd.read_csv(source_file)

        # Add ingestion timestamp
        df["ingestion_timestamp"] = datetime.now(timezone.utc)

        # Convert and save as Parquet
        output_file = DESTINATION / f"{name}.parquet"
        df.to_parquet(output_file, index=False)

        print("Status: SUCCESS")
        print(f"Records ingested: {len(df):,}")
        print(f"Columns: {len(df.columns)}")
        print("Output:", output_file)

        successful += 1

    except Exception as e:
        print("ERROR:", str(e))

print("\n" + "=" * 55)
print("BATCH INGESTION SUMMARY")
print("=" * 55)
print(f"Successful datasets: {successful}/{len(datasets)}")
print("Output folder:", DESTINATION)

if successful == len(datasets):
    print("STATUS: ALL DATASETS INGESTED SUCCESSFULLY")
else:
    print("STATUS: INGESTION INCOMPLETE - CHECK ERRORS")

print("=" * 55)
