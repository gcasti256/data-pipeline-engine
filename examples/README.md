# Examples

Runnable examples demonstrating the data-pipeline-engine.

## Prerequisites

1. Install the package in development mode from the repository root:

   ```bash
   pip install -e ".[dev]"
   ```

2. Create the sample data directory used by the examples:

   ```bash
   mkdir -p data
   ```

3. For `basic_etl.py` and the YAML config, place a CSV file at `data/sales.csv` with
   columns like `region`, `price`, `quantity`, `amount`.

4. For `streaming_pipeline.py`, place a JSONL file at `data/events.jsonl` where each
   line is a JSON object with fields like `event_type`, `amount`, and `exchange_rate`.

## Examples

### basic_etl.py

A complete ETL pipeline using the programmatic API. Demonstrates how to:

- Define source and sink connectors (CSV, SQLite)
- Build a DAG with filter, map, and aggregate transforms
- Wire nodes together with edges
- Execute the pipeline and inspect results

```bash
python examples/basic_etl.py
```

### streaming_pipeline.py

A streaming pipeline with batch windowing. Demonstrates how to:

- Read records from a JSONL file in streaming mode
- Apply time/count-based batch windows (`BatchWindow`)
- Transform each batch independently
- Write batched results to SQLite

```bash
python examples/streaming_pipeline.py
```

### api_ingestion.py

REST API ingestion with validation and dead-letter routing. Demonstrates how to:

- Fetch data from a REST API (JSONPlaceholder)
- Run two API calls concurrently using parallel DAG branches
- Join datasets from different sources
- Validate records against a Pydantic schema
- Route invalid records to a dead-letter queue
- Write valid records to a JSON file

```bash
python examples/api_ingestion.py
```

### sample_config.yaml

A declarative YAML configuration equivalent to the `basic_etl.py` example. Shows
the config-driven approach to defining sources, transforms, and sinks without
writing Python code. Run it with:

```bash
pipeline-engine run examples/sample_config.yaml
```
