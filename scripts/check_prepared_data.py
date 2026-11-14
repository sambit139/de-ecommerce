
import pandas as pd
from pathlib import Path

folder = Path("data/processed")

for file in folder.glob("*"):
    if file.suffix.lower() not in [".csv", ".parquet"]:
        continue

    if file.suffix.lower() == ".csv":
        df = pd.read_csv(file)
    else:
        df = pd.read_parquet(file)

    print("\nDataset:", file.name)
    print("Shape:", df.shape)
    print("Missing values:", df.isnull().sum().sum())
    print("Duplicate rows:", df.duplicated().sum())
    print(df.head(3))
