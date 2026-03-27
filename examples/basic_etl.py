"""Basic ETL pipeline example.

Reads sales data from CSV, cleans and transforms it,
then loads aggregated results into SQLite.
"""
import asyncio
from pipeline_engine.core import DAG, Node, PipelineExecutor
from pipeline_engine.connectors import CSVConnector, SQLiteConnector
from pipeline_engine.transforms import FilterTransform, MapTransform, AggregateTransform


async def main():
    # Define connectors
    source = CSVConnector("data/sales.csv")
    sink = SQLiteConnector("data/output.db")

    # Define transforms
    clean = FilterTransform(condition="amount > 0")
    enrich = MapTransform(columns={"total": "price * quantity", "region": "upper(region)"})
    summarize = AggregateTransform(
        group_by=["region"],
        aggregations={"total_sales": "sum(total)", "order_count": "count()"}
    )

    # Build DAG
    dag = DAG(name="sales_etl")

    # Create operation functions that work with the executor context
    async def read_source(ctx):
        data = await source.read()
        return data

    async def apply_clean(ctx):
        data = ctx["results"][ctx["dependencies"][0]]
        return clean.execute(data)

    async def apply_enrich(ctx):
        data = ctx["results"][ctx["dependencies"][0]]
        return enrich.execute(data)

    async def apply_summarize(ctx):
        data = ctx["results"][ctx["dependencies"][0]]
        return summarize.execute(data)

    async def write_sink(ctx):
        data = ctx["results"][ctx["dependencies"][0]]
        count = await sink.write(data, table="sales_summary")
        return {"records_written": count}

    dag.add_node(Node(id="extract", operation=read_source))
    dag.add_node(Node(id="clean", operation=apply_clean))
    dag.add_node(Node(id="enrich", operation=apply_enrich))
    dag.add_node(Node(id="summarize", operation=apply_summarize))
    dag.add_node(Node(id="load", operation=write_sink))

    dag.add_edge("extract", "clean")
    dag.add_edge("clean", "enrich")
    dag.add_edge("enrich", "summarize")
    dag.add_edge("summarize", "load")

    # Execute
    executor = PipelineExecutor(dag)
    state = await executor.execute()

    print(f"Pipeline completed: {state.status.value}")
    print(f"Duration: {state.duration:.2f}s")
    for node_id, ns in state.node_states.items():
        print(f"  {node_id}: {ns.status.value} ({ns.output_records} records)")

if __name__ == "__main__":
    asyncio.run(main())
