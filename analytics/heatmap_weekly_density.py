import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

# Adjust if needed — this assumes you're running from project root
DB_PATH = Path("radio_plays.db")

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)

conn = sqlite3.connect(DB_PATH)

query = """
SELECT
    p.play_ts,
    c.spotify_duration_ms
FROM plays p
JOIN plays_to_canonical ptc
    ON p.id = ptc.play_id
JOIN canonical_tracks c
    ON ptc.canonical_id = c.canonical_id
WHERE p.is_music_show = 1
  AND c.spotify_duration_ms IS NOT NULL
"""

df = pd.read_sql_query(query, conn)
conn.close()

df["play_ts"] = pd.to_datetime(df["play_ts"], errors="coerce")

df["day_of_week"] = df["play_ts"].dt.day_name()
df["hour"] = df["play_ts"].dt.hour

# Extract calendar date
df["date"] = df["play_ts"].dt.date

# Convert milliseconds → minutes
df["duration_minutes"] = df["spotify_duration_ms"] / 60000

# Step 1: total minutes per date + hour
per_date_hour = (
    df.groupby(["date", "day_of_week", "hour"])["duration_minutes"]
    .sum()
    .reset_index()
)

# Step 2: average across same weekday/hour combinations
heatmap_data = (
    per_date_hour.groupby(["day_of_week", "hour"])["duration_minutes"]
    .mean()
    .reset_index()
)

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
    values="duration_minutes"
).fillna(0)

plt.figure(figsize=(12, 6))
plt.imshow(pivot.values, aspect="auto")
plt.colorbar(label="Total Music Minutes")
plt.xticks(range(len(pivot.columns)), pivot.columns)
plt.yticks(range(len(pivot.index)), pivot.index)
plt.xlabel("Hour of Day")
plt.ylabel("Day of Week")
plt.title("Weekly Music Density Heatmap (Total Spotify Minutes)")

output_path = OUTPUT_DIR / "heatmap_weekly_density.png"

plt.savefig(output_path, dpi=300)
plt.close()

print(f"Saved heatmap to: {output_path}")