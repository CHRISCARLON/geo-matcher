# Currently

Expect things to change.

- Linestring matching / better way of handling different types of join
- Per-chunk explain stats in NationalMode: currently only chunk 1 is explained (`if explain and i == 0`); consider logging key pruning metrics (row groups pruned, bytes scanned) for every chunk so geographic variation in USRN pruning is visible
