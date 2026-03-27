"""Streaming pipeline example.

Reads records from a JSONL file in streaming mode, applies transforms
per batch using time/count-based windowing, and writes results to SQLite.
"""
import asyncio
from pipeline_engine.core import DAG, Node, PipelineExecutor
from pipeline_engine.connectors import JSONLConnector, SQLiteConnector
from pipeline_engine.transforms import FilterTransform, MapTransform
from pipeline_engine.streaming import BatchWindow


async def main():
    # --- Connectors ---
    # JSONLConnector in streaming mode yields records one at a time
    source = JSONLConnector("data/events.jsonl", streaming=True)
    sink = SQLiteConnector("data/stream_output.db")

    # --- Transforms applied to each batch ---
    only_purchases = FilterTransform(condition="event_type == 'purchase'")
    add_fields = MapTransform(columns={
        "amount_usd": "amount * exchange_rate",
        "processed_at": "now()",
    })

    # --- Batch window: flush every 100 records OR every 5 seconds ---
    window = BatchWindow(max_count=100, max_seconds=5)

    # --- Build the DAG ---
    dag = DAG(name="event_stream")

    # Node 1: stream records from the JSONL file
    async def stream_source(ctx):
        """Yield records lazily from the JSONL file."""
        async for record in source.stream():
            yield record

    # Node 2: batch the stream using the window policy
    async def batch_records(ctx):
        """Collect records into batches based on count/time thresholds."""
        upstream = ctx["results"][ctx["dependencies"][0]]
        async for batch in window.apply(upstream):
            yield batch

    # Node 3: filter and transform each batch
    async def transform_batch(ctx):
        """Apply filter + map transforms to every batch that arrives."""
        batches = ctx["results"][ctx["dependencies"][0]]
        async for batch in batches:
            filtered = only_purchases.execute(batch)
            enriched = add_fields.execute(filtered)
            yield enriched

    # Node 4: write each transformed batch to SQLite
    async def load_batch(ctx):
        """Persist each batch to the destination table."""
        batches = ctx["results"][ctx["dependencies"][0]]
        total_written = 0
        async for batch in batches:
            count = await sink.write(batch, table="purchases")
            total_written += count
        return {"total_records_written": total_written}

    # Register nodes
    dag.add_node(Node(id="stream", operation=stream_source))
    dag.add_node(Node(id="batch", operation=batch_records))
    dag.add_node(Node(id="transform", operation=transform_batch))
    dag.add_node(Node(id="load", operation=load_batch))

    # Wire up the edges: stream -> batch -> transform -> load
    dag.add_edge("stream", "batch")
    dag.add_edge("batch", "transform")
    dag.add_edge("transform", "load")

    # --- Execute ---
    executor = PipelineExecutor(dag, mode="streaming")
    state = await executor.execute()

    print(f"Streaming pipeline completed: {state.status.value}")
    print(f"Duration: {state.duration:.2f}s")
    for node_id, ns in state.node_states.items():
        status = ns.status.value
        records = ns.output_records
        print(f"  {node_id}: {status} ({records} records)")


if __name__ == "__main__":
    asyncio.run(main())
