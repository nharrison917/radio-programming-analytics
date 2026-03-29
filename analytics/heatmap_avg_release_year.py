import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np


DB_PATH = Path("radio_plays.db")
conn = sqlite3.connect(DB_PATH)

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

query = """
SELECT
    p.play_ts,
    CASE
        WHEN c.mb_first_release_year IS NOT NULL
         AND c.mb_first_release_year < c.best_year
        THEN c.mb_first_release_year
        ELSE c.best_year
    END AS best_year
FROM plays p
JOIN plays_to_canonical ptc
    ON p.id = ptc.play_id
JOIN canonical_tracks c
    ON ptc.canonical_id = c.canonical_id
WHERE p.is_music_show = 1
  AND c.best_year IS NOT NULL
"""

df = pd.read_sql_query(query, conn)
conn.close()

df["play_ts"] = pd.to_datetime(df["play_ts"], errors="coerce")
df["day_of_week"] = df["play_ts"].dt.day_name()
df["hour"] = df["play_ts"].dt.hour
df["date"] = df["play_ts"].dt.date

import numpy as np

# Step 1: per date/hour aggregation
per_date_hour = (
    df.groupby(["date", "day_of_week", "hour"])
      .agg(
          avg_year=("best_year", "mean"),
          track_count=("best_year", "size")
      )
      .reset_index()
)

# Step 2: aggregate across same weekday/hour combinations
heatmap_data = (
    per_date_hour.groupby(["day_of_week", "hour"])
      .agg(
          best_year=("avg_year", "mean"),
          avg_tracks=("track_count", "mean")
      )
      .reset_index()
)

# Optional: require minimum average tracks per cell
MIN_TRACKS = 3
heatmap_data.loc[
    heatmap_data["avg_tracks"] < MIN_TRACKS,
    "best_year"
] = np.nan

weekday_order = [
    "Monday", "Tuesday", "Wednesday",
    "Thursday", "Friday", "Saturday", "Sunday"
]

heatmap_data["day_of_week"] = pd.Categorical(
    heatmap_data["day_of_week"],
    categories=weekday_order,
    ordered=True
)

pivot = heatmap_data.pivot(
    index="day_of_week",
    columns="hour",
    values="best_year"
)  # <-- NO fillna(0)

data = pivot.values.astype(float)

# Mask missing values so they don't affect the color scale
masked = np.ma.masked_invalid(data)

plt.figure(figsize=(12, 6))
plt.imshow(masked, aspect="auto")
plt.colorbar(label="Average Album Release Year")

plt.xticks(range(len(pivot.columns)), pivot.columns)
plt.yticks(range(len(pivot.index)), pivot.index)
plt.xlabel("Hour of Day")
plt.ylabel("Day of Week")
plt.title("Weekly Heatmap: Average Spotify Album Release Year")
plt.tight_layout()

output_path = OUTPUT_DIR / "heatmap_avg_release_year.png"
plt.savefig(output_path, dpi=300)
plt.close()

print(f"Saved heatmap to: {output_path}")

print(heatmap_data.sort_values("avg_tracks").head(10))