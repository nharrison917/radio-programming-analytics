# analytics/boxplot_release_year.py
# -*- coding: utf-8 -*-

import sys
from pathlib import Path
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from analytics.analysis import load_base_dataset

OUTPUT_DIR = Path(__file__).resolve().parent / "outputs"
OUTPUT_DIR.mkdir(exist_ok=True)


def build_release_year_boxplot():
    print("Loading dataset...")
    df = load_base_dataset()

    df = df.dropna(subset=["spotify_album_release_year"]).copy()
    df["spotify_album_release_year"] = df["spotify_album_release_year"].astype(int)

    # Sort shows by median release year (oldest to newest left to right)
    show_order = (
        df.groupby("station_show")["spotify_album_release_year"]
        .median()
        .sort_values()
        .index.tolist()
    )

    fig = go.Figure()

    for show in show_order:
        years = df[df["station_show"] == show]["spotify_album_release_year"]
        fig.add_trace(go.Box(
            y=years,
            name=show,
            boxpoints="outliers",
            marker_size=4,
        ))

    fig.update_layout(
        title="Distribution of Track Release Years by Show",
        yaxis_title="Album Release Year",
        xaxis_title="Show",
        showlegend=False,
        height=600,
        margin=dict(b=160),
        xaxis=dict(tickangle=-35),
    )

    output_path = OUTPUT_DIR / "boxplot_release_year.html"
    fig.write_html(str(output_path))
    print(f"Saved to: {output_path}")


if __name__ == "__main__":
    build_release_year_boxplot()
