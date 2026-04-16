# Library Exports (generated 2026-04-16)
# fn=function, class=class

## (root)/
rs_main.py
  fn run_normalize
  fn run_seed_canonicals
  fn run_map_plays
  fn run_ingest
  fn main

## analytics/
analysis.py
  fn get_connection
  fn load_base_dataset
  fn unique_artists_per_show
  fn unique_artists_per_hour
  fn shannon_entropy
  fn entropy_by_show
  fn exclusive_artist_percentage
  fn average_album_year_by_show
  fn freshness_percentage_by_show
  fn artist_breadth
  fn top_fresh_tracks_by_week
  fn print_fresh_tracks_report
  fn run_analysis
band_age.py
  fn _load_data
  fn _apply_segmentation
  fn _coverage_summary
  fn _compute_band_age
  fn _summary_csv
  fn _boxplot
  fn run_band_age
boxplot_release_year.py
  fn build_release_year_boxplot
era_continuity.py
  fn _show_params
  fn load_data
  fn chart_mean_gap
  fn chart_fingerprint
  fn chart_buckets
  fn _modal_era
  fn _segment_block
  fn get_inband_tracks
  fn load_segmented_tracks
  fn compute_segmented_metrics
  fn print_segmented_comparison
  fn run_era_continuity
heatmap_avg_release_year.py
  fn run_heatmap_avg_release_year
heatmap_weekly_density.py
  fn run_heatmap_weekly_density
segment_breakers.py
  fn _get_oob_indices
  fn run_segment_breakers
show_clustering.py
  fn _display_label
  fn _get_conn
  fn _load_plays
  fn compute_scalar_features
  fn compute_repertoire_similarity
  fn _dendrogram
  fn _scalar_heatmap
  fn _similarity_heatmap
  fn run_show_clustering
visuals.py
  fn get_connection
  fn load_dataset
  fn compute_unique_artists_per_hour
  fn compute_freshness
  fn build_scatter_plot
  fn run_visuals
wednesday_freshness.py
  fn load_plays
  fn flag_new
  fn freshness_pct_by_day
  fn hours_with_new_pct_by_day
  fn make_traces
  fn build_figure
  fn print_summary
  fn run_wednesday_freshness

## scraper/
artist_enrichment.py
  fn seed_canonical_artists
  fn _handle_rate_limit
  fn _fetch_all_releases
  fn _fetch_releases_with_backoff
  fn _parse_earliest_release
  fn enrich_artists
audit.py
  fn run_full_audit
canonical.py
  fn seed_new_canonicals
  fn map_new_plays
db.py
  fn migrate_db
  fn init_db
  fn insert_play
enrichment.py
  fn similarity
  fn get_spotify_token
  fn spotify_search_tracks
  fn enrich_all
fetch.py
  fn is_allowed_url
  fn fetch_url
mb_artist_enrichment.py
  fn _get
  fn _mbid_from_isrc_response
  fn _resolve_via_isrc
  fn _resolve_via_name
  fn _run_pass_a
  fn _print_pass_a_summary
  fn _earliest_rg_year
  fn _run_pass_b
  fn _print_pass_b_summary
  fn _write_mb_artist_missing_csv
  fn _write_mb_artist_large_delta_csv
  fn run_mb_artist_enrichment
  fn run_set_artist_meta
mb_enrichment.py
  fn _earliest_valid_year
  fn _lookup_isrc
  fn _clean_secondary_types
  fn _lookup_title_artist
  fn _write_mb_failed_csv
  fn run_mb_enrichment
  fn _print_pass_summary
  fn _integrity_check
  fn _build_result
normalization.py
  fn normalize_new_plays
normalization_logic.py
  fn strip_diacritics
  fn squash_spaces
  fn normalize_common_punct
  fn normalize_for_key
  fn drop_leading_the
  fn extract_year
  fn classify_version_type
  fn extract_feat_artists_from_text
  fn extract_trailing_parentheticals
  fn extract_version_suffix
  fn normalize_artist
  fn normalize_title
  fn normalize_title_artist
orchestrator.py
  fn get_existing_station_shows
  fn build_play_url
  fn get_last_play_ts
  fn run_scrape
  fn run_backfill
overrides.py
  fn _parse_year_input
  fn _parse_duration_input
  fn _format_duration
  fn _fetch_canonical
  fn _fetch_existing_override
  fn run_add_override
  fn run_set_meta
parsing.py
  fn extract_hour_from_source
  fn parse_timestamp_guess
  fn parse_station_show_from_header
  fn parse_played_page
spotify_backfill.py
  fn _fetch_single_track
  fn backfill_spotify_meta
utils.py
  fn create_backup
  fn setup_logging
  fn rotate_backups
  fn rotate_logs
weekly.py
  fn run_enrich_spotify
  fn run_weekly

## scratch/
spotify_status_helper.py
  fn update_spotify_status
  fn bulk_update_status

## scripts/
pipeline_status.py
  fn _pct
  fn _bar
  fn run_status
