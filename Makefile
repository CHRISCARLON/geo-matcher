# usrn-matcher — task runner
# Usage: make <target>

STOPS_CSV = input_data/Stops.csv

.PHONY: \
	init \
	prepare-usrns prepare-usrns-force \
	prepare-soil prepare-stops prepare-counts prepare-all \
	match-soil-national match-soil-national-explain \
	match-soil-leeds match-soil-leeds-explain \
	match-stops-national match-counts-national match-all-national \
	dtf-export-soil-national dtf-export-stops-national dtf-export-counts-national dtf-export-all-national \
	clean-output clean-matched

# ── Setup ─────────────────────────────────────────────────────────────────────

init:
	usrn-matcher init

# ── Prepare USRNs ─────────────────────────────────────────────────────────────

prepare-usrns:
	usrn-matcher prepare-usrns

prepare-usrns-force:
	usrn-matcher prepare-usrns --force

# ── Prepare ───────────────────────────────────────────────────────────────────

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

# ── Match — National (no bbox) ────────────────────────────────────────────────

match-soil-national:
	usrn-matcher match \
		--rhs-name   soil \
		--mode       intersect \
		--output     csv \

match-stops-national:
	usrn-matcher match \
		--rhs-name stop \
		--mode     nearest \
		--distance 10 \
		--output   csv \

match-counts-national:
	usrn-matcher match \
		--rhs-name count_points \
		--mode     nearest \
		--distance 10 \
		--output   csv \

match-soil-leeds:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--city     LEEDS \
		--output   csv

match-soil-leeds-explain:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--city     LEEDS \
		--output   csv \
		--explain

match-soil-national-explain:
	usrn-matcher match \
		--rhs-name soil \
		--mode     intersect \
		--output   csv \
		--batches  10 \
		--explain

match-all-national: match-soil-national match-stops-national match-counts-national

# ── DTF Export — national ─────────────────────────────────────────────────────

dtf-export-soil-national:
	usrn-matcher dtf-export \
		--rhs-name soil \
		--mode     intersect

dtf-export-stops-national:
	usrn-matcher dtf-export \
		--rhs-name stops \
		--mode     nearest

dtf-export-counts-national:
	usrn-matcher dtf-export \
		--rhs-name count_points \
		--mode     nearest

dtf-export-all-national: dtf-export-soil-national dtf-export-stops-national dtf-export-counts-national

# ── Clean ─────────────────────────────────────────────────────────────────────

clean-output:
	rm -f output_data/*.parquet

clean-matched:
	rm -f matched_data/*
