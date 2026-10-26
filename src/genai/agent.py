"""ShopSphere Assistant: a tool-calling RAG agent (Mosaic AI Agent Framework, MLflow ChatAgent).

Logged with MLflow "models from code" by 21_agent_build_and_deploy. Tools:
- search_knowledge: Vector Search over policies, product descriptions and support tickets
- get_order_status: order status, dates, items and payment from the silver/gold tables
- get_churn_risk: latest churn score for a customer from ml.churn_predictions

Configuration comes from the ModelConfig passed at logging time (see CONFIG below), so the
same code runs in the notebook and on Model Serving. Auth uses the Databricks SDK, which
picks up the serving endpoint's automatic credentials for the declared resources.
"""

import json
import random
import time
import uuid
from typing import Any, Optional

import mlflow
import openai
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from mlflow.pyfunc import ChatAgent
from mlflow.types.agent import ChatAgentChunk, ChatAgentMessage, ChatAgentResponse, ChatContext

CONFIG = mlflow.models.ModelConfig(
    development_config={
        "llm_endpoint": "databricks-llama-4-maverick",
        "embedding_endpoint": "databricks-gte-large-en",
        "vs_index": "shopsphere_dev.ai.knowledge_index",
        "warehouse_id": "",  # set at logging time
        "catalog": "shopsphere_dev",
        "max_tool_rounds": 5,
    }
)

SYSTEM_PROMPT = """You are the ShopSphere Assistant for an online marketplace in Brazil (Olist data, prices in BRL).
Answer questions about orders, shipping, returns, payments, products and customer feedback.
Rules:
- Use the tools. For policy questions call search_knowledge with source_type "policy". For a specific
  order, call get_order_status. For a customer's churn risk, call get_churn_risk.
- Base answers only on tool results. If the tools don't have the answer, say so.
- Cite the policy section or ticket you used in brackets, for example [ShopSphere Returns and Refunds Policy - 1. Right of withdrawal].
- Be concise: a short answer, then key details as a few bullet points if needed."""

TOOLS = [
    {
        "type": "function",
        "function": {
            "name": "search_knowledge",
            "description": "Semantic search over ShopSphere policies (shipping, returns, payments, seller rules), product descriptions and customer support tickets.",
            "parameters": {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "description": "What to search for, in English"},
                    "source_type": {"type": "string", "enum": ["policy", "product", "ticket"], "description": "Optional filter"},
                },
                "required": ["query"],
            },
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_order_status",
            "description": "Look up one order by its 32-character order_id: status, purchase/estimated/delivered dates, items, total, payment and review score.",
            "parameters": {"type": "object", "properties": {"order_id": {"type": "string"}}, "required": ["order_id"]},
        },
    },
    {
        "type": "function",
        "function": {
            "name": "get_churn_risk",
            "description": "Latest churn prediction for a customer, by customer_unique_id (32 characters).",
            "parameters": {"type": "object", "properties": {"customer_unique_id": {"type": "string"}}, "required": ["customer_unique_id"]},
        },
    },
]


class ShopSphereAgent(ChatAgent):
    def __init__(self):
        self.w = WorkspaceClient()
        self.llm = self.w.serving_endpoints.get_open_ai_client()
        self.cfg = {k: CONFIG.get(k) for k in ("llm_endpoint", "embedding_endpoint", "vs_index", "warehouse_id", "catalog", "max_tool_rounds")}

    # --- tools ---------------------------------------------------------------------------
    @mlflow.trace(span_type="RETRIEVER")
    def search_knowledge(self, query: str, source_type: Optional[str] = None) -> list[dict]:
        # The index stores precomputed embeddings (self-managed), so embed the question with the same model
        vector = self.llm.embeddings.create(model=self.cfg["embedding_endpoint"], input=[query]).data[0].embedding
        res = self.w.vector_search_indexes.query_index(
            index_name=self.cfg["vs_index"],
            columns=["chunk_id", "source_type", "title", "text"],
            query_vector=vector,
            num_results=5,
            filters_json=json.dumps({"source_type": source_type}) if source_type else None,
        )
        cols = [c.name for c in res.manifest.columns]
        rows = [dict(zip(cols, r)) for r in (res.result.data_array or [])]
        return [{"id": r["chunk_id"], "title": r["title"], "text": r["text"], "score": r.get("score")} for r in rows]

    def _sql(self, statement: str, params: dict) -> list[dict]:
        r = self.w.statement_execution.execute_statement(
            warehouse_id=self.cfg["warehouse_id"],
            statement=statement,
            parameters=[StatementParameterListItem(name=k, value=v) for k, v in params.items()],
            wait_timeout="30s",
        )
        if r.status.state != StatementState.SUCCEEDED:
            return [{"error": str(r.status.error.message if r.status.error else r.status.state)}]
        cols = [c.name for c in r.manifest.schema.columns]
        return [dict(zip(cols, row)) for row in (r.result.data_array or [])]

    @mlflow.trace(span_type="TOOL")
    def get_order_status(self, order_id: str) -> dict:
        c = self.cfg["catalog"]
        rows = self._sql(
            f"""
            SELECT o.order_id, o.order_status, string(o.order_purchase_ts) AS purchased,
                   string(o.order_estimated_delivery_ts) AS estimated_delivery,
                   string(o.order_delivered_customer_ts) AS delivered, o.delivery_days,
                   (SELECT count(*) FROM {c}.silver.order_items i WHERE i.order_id = o.order_id) AS items,
                   (SELECT round(sum(price + freight_value), 2) FROM {c}.silver.order_items i WHERE i.order_id = o.order_id) AS order_total_brl,
                   (SELECT concat_ws(', ', collect_set(payment_type)) FROM {c}.silver.payments p WHERE p.order_id = o.order_id) AS payment_types,
                   (SELECT max(review_score) FROM {c}.silver.reviews r WHERE r.order_id = o.order_id) AS review_score
            FROM {c}.silver.orders o WHERE o.order_id = :order_id""",
            {"order_id": order_id.strip()},
        )
        return rows[0] if rows else {"error": f"No order found with id {order_id}"}

    @mlflow.trace(span_type="TOOL")
    def get_churn_risk(self, customer_unique_id: str) -> dict:
        c = self.cfg["catalog"]
        rows = self._sql(
            f"""
            SELECT customer_unique_id, string(as_of_date) AS as_of_date, round(churn_probability, 4) AS churn_probability,
                   risk_band, model_version, recency_days, frequency, round(monetary, 2) AS monetary_brl
            FROM {c}.ml.churn_predictions WHERE customer_unique_id = :cid
            ORDER BY as_of_date DESC, scored_ts DESC LIMIT 1""",
            {"cid": customer_unique_id.strip()},
        )
        return rows[0] if rows else {"error": f"No churn score for customer {customer_unique_id}"}

    def _call_tool(self, name: str, args: dict) -> Any:
        fn = {"search_knowledge": self.search_knowledge, "get_order_status": self.get_order_status, "get_churn_risk": self.get_churn_risk}.get(name)
        if fn is None:
            return {"error": f"unknown tool {name}"}
        try:
            return fn(**args)
        except Exception as e:  # return the error to the model instead of failing the request
            return {"error": f"{type(e).__name__}: {e}"}

    def _complete(self, convo: list[dict], attempts: int = 4):
        """Chat completion with backoff: pay-per-token endpoints return 429 under bursts of requests."""
        for attempt in range(attempts):
            try:
                return self.llm.chat.completions.create(model=self.cfg["llm_endpoint"], messages=convo, tools=TOOLS, max_tokens=800)
            except openai.RateLimitError:
                if attempt == attempts - 1:
                    raise
                time.sleep(2 ** attempt * 3 + random.random())

    # --- ChatAgent -----------------------------------------------------------------------
    @mlflow.trace(span_type="AGENT")
    def predict(self, messages: list[ChatAgentMessage], context: Optional[ChatContext] = None,
                custom_inputs: Optional[dict[str, Any]] = None) -> ChatAgentResponse:
        convo = [{"role": "system", "content": SYSTEM_PROMPT}] + [
            {"role": m.role, "content": m.content} for m in messages if m.role in ("user", "assistant") and m.content
        ]
        out: list[ChatAgentMessage] = []
        for _ in range(int(self.cfg["max_tool_rounds"])):
            resp = self._complete(convo)
            msg = resp.choices[0].message
            if not msg.tool_calls:
                out.append(ChatAgentMessage(role="assistant", content=msg.content or "", id=str(uuid.uuid4())))
                return ChatAgentResponse(messages=out)
            calls = [{"id": t.id, "type": "function", "function": {"name": t.function.name, "arguments": t.function.arguments}} for t in msg.tool_calls]
            convo.append({"role": "assistant", "content": msg.content or "", "tool_calls": calls})
            out.append(ChatAgentMessage(role="assistant", content=msg.content or "", tool_calls=calls, id=str(uuid.uuid4())))
            for t in msg.tool_calls:
                result = self._call_tool(t.function.name, json.loads(t.function.arguments or "{}"))
                content = json.dumps(result, default=str)
                convo.append({"role": "tool", "tool_call_id": t.id, "content": content})
                out.append(ChatAgentMessage(role="tool", content=content, name=t.function.name, tool_call_id=t.id, id=str(uuid.uuid4())))
        out.append(ChatAgentMessage(role="assistant", content="I couldn't finish within the tool-call limit. Please narrow the question.", id=str(uuid.uuid4())))
        return ChatAgentResponse(messages=out)


mlflow.openai.autolog()
AGENT = ShopSphereAgent()
mlflow.models.set_model(AGENT)
