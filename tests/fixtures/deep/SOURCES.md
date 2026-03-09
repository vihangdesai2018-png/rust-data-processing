## Deep test fixture sources / licensing

These fixtures are used by feature-gated integration tests (`cargo test --features deep_tests ...`).

### `seattle-weather.csv`
- **Upstream**: Vega Datasets (`seattle-weather.csv`)
- **Metadata**: `vega/vega-datasets` `datapackage.json` describes the dataset license as **U.S. Government Dataset** (`other-pd`) and cites NOAA NCDC as the source.
- **Links**:
  - `https://raw.githubusercontent.com/vega/vega-datasets/main/data/seattle-weather.csv`
  - `https://raw.githubusercontent.com/vega/vega-datasets/main/datapackage.json`

### `rle-dict-snappy-checksum.parquet`
- **Upstream**: Apache Parquet testing data
- **License**: Apache License 2.0
- **Links**:
  - `https://github.com/apache/parquet-testing`
  - `https://raw.githubusercontent.com/apache/parquet-testing/master/data/rle-dict-snappy-checksum.parquet`
  - `https://raw.githubusercontent.com/apache/parquet-testing/master/LICENSE.txt`

### `job_runs_sample.json`
- **Source**: Synthetic (created for this repository).
- **Purpose**: Mimics “data lake / lakehouse job run” style payloads (nested objects, nulls) to exercise dot-path JSON extraction.

