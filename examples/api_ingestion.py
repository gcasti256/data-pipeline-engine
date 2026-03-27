"""REST API ingestion example.

Fetches posts and user data from JSONPlaceholder, validates each record
against a Pydantic schema, routes invalid records to a dead-letter queue,
and writes valid records to a JSON file.
"""
import asyncio
from pydantic import BaseModel, Field, field_validator
from pipeline_engine.core import DAG, Node, PipelineExecutor
from pipeline_engine.connectors import RESTConnector, JSONFileConnector
from pipeline_engine.transforms import MapTransform
from pipeline_engine.routing import DeadLetterQueue


# ---------------------------------------------------------------------------
# 1. Pydantic schema for validation
# ---------------------------------------------------------------------------

class EnrichedPost(BaseModel):
    """Schema that every record must satisfy before it reaches the sink."""
    id: int
    title: str = Field(min_length=1, max_length=300)
    body: str
    user_id: int = Field(gt=0)
    username: str
    word_count: int = Field(ge=0)

    @field_validator("title")
    @classmethod
    def title_not_blank(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("title must not be blank")
        return v.strip()


async def main():
    # --- Connectors ---
    posts_api = RESTConnector(
        base_url="https://jsonplaceholder.typicode.com",
        endpoint="/posts",
        method="GET",
    )
    users_api = RESTConnector(
        base_url="https://jsonplaceholder.typicode.com",
        endpoint="/users",
        method="GET",
    )
    output = JSONFileConnector("data/enriched_posts.json")

    # --- Dead-letter queue for records that fail validation ---
    dlq = DeadLetterQueue(path="data/dead_letters.jsonl")

    # --- Transform: add a computed field ---
    add_word_count = MapTransform(columns={
        "word_count": "len(body.split())",
    })

    # --- Build DAG ---
    dag = DAG(name="api_ingestion")

    # Fetch posts from the API
    async def fetch_posts(ctx):
        """Pull all posts from the /posts endpoint."""
        return await posts_api.read()

    # Fetch users (runs in parallel with fetch_posts)
    async def fetch_users(ctx):
        """Pull all users from the /users endpoint."""
        return await users_api.read()

    # Join posts with usernames
    async def join_data(ctx):
        """Merge user info into each post record."""
        posts = ctx["results"]["fetch_posts"]
        users = ctx["results"]["fetch_users"]
        user_lookup = {u["id"]: u["username"] for u in users}
        for post in posts:
            post["username"] = user_lookup.get(post["userId"], "unknown")
            post["user_id"] = post.pop("userId")
        return posts

    # Enrich: add word_count column
    async def enrich(ctx):
        data = ctx["results"][ctx["dependencies"][0]]
        return add_word_count.execute(data)

    # Validate each record; route failures to the DLQ
    async def validate(ctx):
        """Validate every record against the Pydantic schema.

        Valid records pass through; invalid ones are sent to the
        dead-letter queue along with the validation error message.
        """
        data = ctx["results"][ctx["dependencies"][0]]
        valid = []
        for record in data:
            try:
                EnrichedPost(**record)
                valid.append(record)
            except Exception as exc:
                await dlq.send(record, reason=str(exc))
        return valid

    # Write valid records to a JSON file
    async def write_output(ctx):
        data = ctx["results"][ctx["dependencies"][0]]
        count = await output.write(data)
        return {"records_written": count}

    # Register nodes
    dag.add_node(Node(id="fetch_posts", operation=fetch_posts))
    dag.add_node(Node(id="fetch_users", operation=fetch_users))
    dag.add_node(Node(id="join", operation=join_data))
    dag.add_node(Node(id="enrich", operation=enrich))
    dag.add_node(Node(id="validate", operation=validate))
    dag.add_node(Node(id="load", operation=write_output))

    # Edges — fetch_posts and fetch_users run concurrently, then join
    dag.add_edge("fetch_posts", "join")
    dag.add_edge("fetch_users", "join")
    dag.add_edge("join", "enrich")
    dag.add_edge("enrich", "validate")
    dag.add_edge("validate", "load")

    # --- Execute ---
    executor = PipelineExecutor(dag)
    state = await executor.execute()

    # --- Report ---
    print(f"Pipeline completed: {state.status.value}")
    print(f"Duration: {state.duration:.2f}s")
    for node_id, ns in state.node_states.items():
        print(f"  {node_id}: {ns.status.value} ({ns.output_records} records)")

    dlq_count = await dlq.count()
    print(f"\nDead-letter queue: {dlq_count} rejected records")
    if dlq_count > 0:
        print(f"  Inspect with: cat data/dead_letters.jsonl")


if __name__ == "__main__":
    asyncio.run(main())
