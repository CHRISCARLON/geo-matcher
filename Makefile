# usrn-matcher — task runner
# Usage: make <target>

.PHONY: \
	init \
	prepare-usrns prepare-usrns-line \
	prepare-uprns prepare-uprns-buffer \
	prepare-soil prepare-stops prepare-counts prepare-built prepare-gas-pipe prepare-ngn-mains prepare-all \
	match-soil-national match-soil-national-explain \
	match-soil-uprn match-built-uprn \
	match-soil-leeds match-soil-leeds-explain \
	match-stops-national match-counts-national match-all-national \
	match-gas-pipe-sample match-gas-pipe-sample-explain \
	match-gas-pipe-national match-gas-pipe-national-explain \
	match-ngn-mains-sample match-ngn-mains-sample-explain \
	match-ngn-mains-national match-ngn-mains-national-explain \
	clean-output clean-matched

# ── Setup ─────────────────────────────────────────────────────────────────────

init:
	usrn-matcher init

# ── Prepare USRNs ─────────────────────────────────────────────────────────────

prepare-usrns:
	usrn-matcher prepare-usrns \
		--usrn-gpkg input_data/osopenusrn_202607.gpkg \
		--force

prepare-usrns-line:
	usrn-matcher prepare-usrns-line \
		--buffer-m 10 \
		--force \
		--threads 4

# ── Prepare UPRNs ─────────────────────────────────────────────────────────────

prepare-uprns:
	usrn-matcher prepare-uprns \
		--uprn-gpkg input_data/osopenuprn_202608.gpkg \
		--force \
		--threads 4

prepare-uprns-buffer:
	usrn-matcher prepare-uprns-buffer \
		--buffer-m 10 \
		--force \
		--threads 4

# ── Prepare ───────────────────────────────────────────────────────────────────

prepare-soil:
	usrn-matcher prepare-gpkg \
		--rhs-name soil \
		--rhs-row-group-size 100 \
		--force

prepare-built:
	usrn-matcher prepare-gpkg \
		--rhs-name os_open_built_up_areas \
		--rhs-row-group-size 100 \
		--force

prepare-stops:
	usrn-matcher prepare-csv \
		--name   stops \
		--x-col  Easting \
		--y-col  Northing \
		--force

prepare-counts:
	usrn-matcher prepare-csv \
		--name   count_points \
		--x-col  easting \
		--y-col  northing \
		--force

prepare-gas-pipe:
	usrn-matcher prepare-parquet \
		--name         gas_pipe \
		--parquet      downloads/gas-pipe-infrastructure-gpi_open.parquet \
		--geometry-col geo_shape \
		--source-crs   EPSG:4326 \
		--threads      2 \
		--force

prepare-ngn-mains:
	usrn-matcher prepare-gpkg \
		--rhs-name  ngn_mains \
		--rhs-gpkg  input_data/ngn_mains.gpkg \
		--threads   4 \
		--force

prepare-all: prepare-soil prepare-stops prepare-counts prepare-built

# ── Match — National (no bbox) ────────────────────────────────────────────────

match-soil-national:
	usrn-matcher match \
		--rhs-name   soil \
		--mode       polygon \
		--output     parquet

match-soil-uprn:
	usrn-matcher match \
		--lhs-name   uprn \
		--rhs-name   soil \
		--mode       polygon \
		--batches    200 \
		--output     parquet

match-built-uprn:
	usrn-matcher match \
		--lhs-name   uprn \
		--rhs-name   os_open_built_up_areas \
		--mode       polygon \
		--batches    200 \
		--output     parquet

match-stops-national:
	usrn-matcher match \
		--rhs-name stops \
		--mode     point \
		--distance 10 \
		--output   parquet

match-stops-london:
	usrn-matcher match \
		--rhs-name stops \
		--mode     point \
		--distance 10 \
		--output   parquet \
		--city     LONDON

match-counts-national:
	usrn-matcher match \
		--rhs-name count_points \
		--mode     point \
		--distance 10 \
		--output   csv

match-soil-leeds:
	usrn-matcher match \
		--rhs-name soil \
		--mode     polygon \
		--city     LEEDS \
		--output   csv

match-soil-leeds-explain:
	usrn-matcher match \
		--rhs-name soil \
		--mode     polygon \
		--city     LEEDS \
		--output   csv \
		--explain

match-soil-national-explain:
	usrn-matcher match \
		--rhs-name soil \
		--mode     polygon \
		--output   csv \
		--explain

# --explain runs EXPLAIN ANALYZE and then the query again, so it doubles every run.
# Kept out of the default targets; use the -explain variants when you want a plan.

match-gas-pipe-sample:
	usrn-matcher match \
		--rhs-name         gas_pipe \
		--mode             line \
		--distance         10 \
		--phase3-distance  15 \
		--rhs-id-col       asset_id \
		--usrn-line-parquet output_data/usrns_line_10m_27700.parquet \
		--threads          4 \
		--city             MANCHESTER \
		--output           csv

match-gas-pipe-sample-explain:
	usrn-matcher match \
		--rhs-name         gas_pipe \
		--mode             line \
		--distance         10 \
		--phase3-distance  15 \
		--rhs-id-col       asset_id \
		--usrn-line-parquet output_data/usrns_line_10m_27700.parquet \
		--threads          4 \
		--city             MANCHESTER \
		--output           csv \
		--explain

match-gas-pipe-national:
	usrn-matcher match \
		--rhs-name         gas_pipe \
		--mode             line \
		--distance         10 \
		--phase3-distance  15 \
		--rhs-id-col       asset_id \
		--usrn-line-parquet output_data/usrns_line_10m_27700.parquet \
		--threads          4 \
		--output           parquet

match-gas-pipe-national-explain:
	usrn-matcher match \
		--rhs-name         gas_pipe \
		--mode             line \
		--distance         10 \
		--phase3-distance  15 \
		--rhs-id-col       asset_id \
		--usrn-line-parquet output_data/usrns_line_10m_27700.parquet \
		--threads          4 \
		--output           parquet \
		--explain

match-ngn-mains-sample:
	usrn-matcher match \
		--rhs-name         ngn_mains \
		--mode             line \
		--distance         10 \
		--phase3-distance  15 \
		--rhs-id-col       ASSET_ID \
		--usrn-line-parquet output_data/usrns_line_10m_27700.parquet \
		--threads          4 \
		--city             NEWCASTLE \
		--output           csv

match-ngn-mains-sample-explain:
	usrn-matcher match \
		--rhs-name         ngn_mains \
		--mode             line \
		--distance         10 \
		--phase3-distance  15 \
		--rhs-id-col       ASSET_ID \
		--usrn-line-parquet output_data/usrns_line_10m_27700.parquet \
		--threads          4 \
		--city             NEWCASTLE \
		--output           csv \
		--explain

match-ngn-mains-national:
	usrn-matcher match \
		--rhs-name         ngn_mains \
		--mode             line \
		--distance         10 \
		--phase3-distance  15 \
		--rhs-id-col       ASSET_ID \
		--usrn-line-parquet output_data/usrns_line_10m_27700.parquet \
		--threads          4 \
		--output           parquet

match-ngn-mains-national-explain:
	usrn-matcher match \
		--rhs-name         ngn_mains \
		--mode             line \
		--distance         10 \
		--phase3-distance  15 \
		--rhs-id-col       ASSET_ID \
		--usrn-line-parquet output_data/usrns_line_10m_27700.parquet \
		--threads          4 \
		--output           parquet \
		--explain

match-all-national: match-soil-national match-stops-national match-counts-national

# ── Clean ─────────────────────────────────────────────────────────────────────

clean-output:
	rm -f output_data/*.parquet

clean-matched:
	rm -f matched_data/*
