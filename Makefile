# usrn-matcher — task runner
# Usage: make <target>

STOPS_CSV = input_data/Stops.csv

.PHONY: \
	init \
	prepare-usrns prepare-usrns-force \
	prepare-soil prepare-stops prepare-counts prepare-all \
	prepare-soil-force prepare-stops-force prepare-counts-force prepare-all-force \
	match-soil-leeds-parquet match-soil-leeds-csv match-soil-leeds \
	match-stops-leeds match-counts-leeds match-all-leeds \
	match-soil-manchester-parquet match-soil-manchester-csv match-soil-manchester \
	match-stops-manchester match-counts-manchester match-all-manchester \
	match-soil-birmingham-csv match-all-birmingham \
	match-soil-national match-stops-national match-counts-national match-all-national \
	export-soil-national export-stops-national export-counts-national export-all-national \
	clean-output clean-matched

# ── Setup ─────────────────────────────────────────────────────────────────────

init:
	usrn-matcher init

# ── Prepare USRNs ─────────────────────────────────────────────────────────────

prepare-usrns:
	usrn-matcher prepare-usrns

prepare-usrns-force:
	usrn-matcher prepare-usrns --force

# ── Prepare (skip if parquet already exists) ──────────────────────────────────

prepare-soil:
	usrn-matcher prepare-gpkg \
		--rhs-name soil

prepare-stops:
	usrn-matcher prepare-csv \
		--csv    $(STOPS_CSV) \
		--name   stops \
		--x-col  Easting \
		--y-col  Northing

prepare-counts:
	usrn-matcher prepare-csv \
		--name   count_points \
		--x-col  easting \
		--y-col  northing

prepare-all: prepare-soil prepare-stops prepare-counts

# ── Prepare (force re-run) ────────────────────────────────────────────────────

prepare-soil-force:
	usrn-matcher prepare-gpkg \
		--rhs-name soil \
		--force

prepare-stops-force:
	usrn-matcher prepare-csv \
		--csv    $(STOPS_CSV) \
		--name   stops \
		--x-col  Easting \
		--y-col  Northing \
		--force

prepare-counts-force:
	usrn-matcher prepare-csv \
		--name   count_points \
		--x-col  easting \
		--y-col  northing \
		--force

prepare-all-force: prepare-soil-force prepare-stops-force prepare-counts-force

# ── Match — Leeds ─────────────────────────────────────────────────────────────

match-soil-leeds-parquet:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--city     LEEDS \
		--output   parquet

match-soil-leeds-csv:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--city     LEEDS \
		--output   csv \
		--matched-dir matched_data/csv

match-soil-leeds: match-soil-leeds-parquet match-soil-leeds-csv

match-stops-leeds:
	usrn-matcher match \
		--rhs-name stops \
		--mode     nearest \
		--distance 50 \
		--city     LEEDS \
		--output   parquet

match-counts-leeds:
	usrn-matcher match \
		--rhs-name count_points \
		--mode     nearest \
		--distance 50 \
		--city     LEEDS \
		--output   parquet

match-all-leeds: match-soil-leeds match-stops-leeds match-counts-leeds

# ── Match — Manchester ────────────────────────────────────────────────────────

match-soil-manchester-parquet:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--city     MANCHESTER \
		--output   parquet

match-soil-manchester-csv:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--city     MANCHESTER \
		--output   csv \
		--matched-dir matched_data/csv

match-soil-manchester: match-soil-manchester-parquet match-soil-manchester-csv

match-stops-manchester:
	usrn-matcher match \
		--rhs-name stops \
		--mode     nearest \
		--distance 50 \
		--city     MANCHESTER \
		--output   parquet

match-counts-manchester:
	usrn-matcher match \
		--rhs-name count_points \
		--mode     nearest \
		--distance 50 \
		--city     MANCHESTER \
		--output   parquet

match-all-manchester: match-soil-manchester match-stops-manchester match-counts-manchester

# ── Match — Birmingham ────────────────────────────────────────────────────────

match-soil-birmingham-csv:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--city     BIRMINGHAM \
		--output   csv \
		--matched-dir matched_data/csv

match-all-birmingham: match-soil-birmingham-csv

# ── Match — National (no bbox) ────────────────────────────────────────────────

match-soil-national:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--output   csv

match-stops-national:
	usrn-matcher match \
		--rhs-name stops \
		--mode     nearest \
		--distance 10 \
		--output   csv

match-counts-national:
	usrn-matcher match \
		--rhs-name count_points \
		--mode     nearest \
		--distance 10 \
		--output   csv

match-all-national: match-soil-national match-stops-national match-counts-national

# ── Export — DTF format, national ────────────────────────────────────────────

export-soil-national:
	usrn-matcher export \
		--rhs-name soil \
		--mode     intersect

export-stops-national:
	usrn-matcher export \
		--rhs-name stops \
		--mode     nearest \
		--distance 10

export-counts-national:
	usrn-matcher export \
		--rhs-name count_points \
		--mode     nearest \
		--distance 10

export-all-national: export-soil-national export-stops-national export-counts-national

# ── Clean ─────────────────────────────────────────────────────────────────────

clean-output:
	rm -f output_data/*.parquet

clean-matched:
	rm -f matched_data/*
