from datasets import load_dataset
import pandas as pd
from sklearn.preprocessing import LabelEncoder
import pickle
import os

CATEGORY = "Electronics"  # Change to: Electronics, Cell_Phones_and_Accessories, etc.
MAX_RECORDS = 2_000_000
OUTPUT_DIR = "/data"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print(f"Loading Amazon Reviews 2023 - {CATEGORY} from Hugging Face...")
dataset = load_dataset(
    "McAuley-Lab/Amazon-Reviews-2023",
    f"0core_rating_only_{CATEGORY}",
    trust_remote_code=True
)

df = dataset["full"].to_pandas()
print(f"Total records loaded: {len(df):,}")

df = df[["user_id", "parent_asin", "rating", "timestamp"]].copy()
df.columns = ["user_id", "item_id", "rating", "timestamp"]

df = df.dropna()
df["rating"] = df["rating"].astype(float)
df = df[df["rating"].between(1.0, 5.0)]
print(f"Records after cleaning: {len(df):,}")

le_user = LabelEncoder()
le_item = LabelEncoder()
df["user_id_int"] = le_user.fit_transform(df["user_id"])
df["item_id_int"] = le_item.fit_transform(df["item_id"])

if len(df) > MAX_RECORDS:
    df = df.sample(n=MAX_RECORDS, random_state=42)
    print(f"Sampled down to {MAX_RECORDS:,} records")

train = df.sample(frac=0.8, random_state=42)
test  = df.drop(train.index)
print(f"Train: {len(train):,} | Test: {len(test):,}")

train.to_parquet(f"{OUTPUT_DIR}/train_ratings.parquet", index=False)
test.to_parquet(f"{OUTPUT_DIR}/test_ratings.parquet",   index=False)
df.to_parquet(f"{OUTPUT_DIR}/all_ratings.parquet",      index=False)

with open(f"{OUTPUT_DIR}/label_encoders.pkl", "wb") as f:
    pickle.dump({"user": le_user, "item": le_item}, f)

print(f"Done. Files saved to {OUTPUT_DIR}/")
print(f"  - train_ratings.parquet ({len(train):,} rows)")
print(f"  - test_ratings.parquet  ({len(test):,}  rows)")
print(f"  - all_ratings.parquet   ({len(df):,}    rows)")
print(f"  - label_encoders.pkl")
