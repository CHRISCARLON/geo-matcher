# Current Focus

- Speed up Cadent Gas national joins
- Test Nortern Gas joins
- Test accuracy and precision of the line joins

## Done

- Fail loudly on a stale corridor file — `_assert_corridor_file_current` compares the
  two parquet footers' row counts in `execute_line_join`, so both the national and
  filtered paths are covered, and raises with the rebuild command.
