# Databricks notebook source
# MAGIC %md
# MAGIC # ShopSphere Assistant: log, evaluate and deploy the agent
# MAGIC
# MAGIC 1. Smoke-test `agent.py` (a tool-calling `ChatAgent`) in the notebook.
# MAGIC 2. Log it with MLflow "models from code", declaring the resources it uses (LLM endpoint,
# MAGIC    Vector Search index, SQL warehouse, tables) so Model Serving can pass credentials to it.
# MAGIC    Register it as `ai.shopsphere_agent`.
# MAGIC 3. Evaluate with MLflow GenAI judges (Correctness, RelevanceToQuery, Safety, and a
# MAGIC    citation guideline) on 8 questions. Expected facts are read from the tables, so the
# MAGIC    evaluation set stays correct when the data changes.
# MAGIC 4. Deploy with `databricks.agents.deploy` (serving endpoint, scales to zero, plus review app).
# MAGIC    Widget `deploy` = `false` skips this step.

# COMMAND ----------

# MAGIC %pip install -q -U "mlflow[databricks]>=3.1" databricks-agents databricks-sdk openai
# MAGIC %restart_python

# COMMAND ----------

import os

import mlflow
import pandas as pd
from mlflow.models.resources import (
    DatabricksServingEndpoint,
    DatabricksSQLWarehouse,
    DatabricksTable,
    DatabricksVectorSearchIndex,
)

dbutils.widgets.text("warehouse_id", "")
dbutils.widgets.dropdown("deploy", "true", ["true", "false"])
CATALOG = "shopsphere_dev"
MODEL = f"{CATALOG}.ai.shopsphere_agent"
USER = spark.sql("SELECT current_user()").first()[0]
CONFIG = {
    # Claude models return PERMISSION_DENIED in this workspace (account needs model-access approval)
    "llm_endpoint": "databricks-llama-4-maverick",
    "embedding_endpoint": "databricks-gte-large-en",
    "vs_index": f"{CATALOG}.ai.knowledge_index",
    "warehouse_id": dbutils.widgets.get("warehouse_id"),
    "catalog": CATALOG,
    "max_tool_rounds": 5,
}
assert CONFIG["warehouse_id"], "Set the warehouse_id widget"
AGENT_FILE = os.path.join(os.getcwd(), "agent.py")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment(f"/Users/{USER}/shopsphere/genai/shopsphere_agent")

# COMMAND ----------

# 1. Smoke test, loading the agent code directly (its development_config has no warehouse
#    id, so the notebook's CONFIG is applied on top)
import importlib.util

from mlflow.types.agent import ChatAgentMessage

spec = importlib.util.spec_from_file_location("agent", AGENT_FILE)
agent_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(agent_module)
agent_module.AGENT.cfg.update(CONFIG)
smoke = agent_module.AGENT.predict([ChatAgentMessage(role="user", content="How long do I have to return a product I bought online?")])
print(smoke.messages[-1].content)

# COMMAND ----------

# 2. Log and register
tables = ["silver.orders", "silver.order_items", "silver.payments", "silver.reviews", "ml.churn_predictions", "ai.knowledge_embeddings"]
resources = [
    DatabricksServingEndpoint(endpoint_name=CONFIG["llm_endpoint"]),
    DatabricksServingEndpoint(endpoint_name=CONFIG["embedding_endpoint"]),
    DatabricksVectorSearchIndex(index_name=CONFIG["vs_index"]),
    DatabricksSQLWarehouse(warehouse_id=CONFIG["warehouse_id"]),
] + [DatabricksTable(table_name=f"{CATALOG}.{t}") for t in tables]

with mlflow.start_run(run_name="shopsphere_agent"):
    info = mlflow.pyfunc.log_model(
        name="agent",
        python_model=AGENT_FILE,
        model_config=CONFIG,
        resources=resources,
        input_example={"messages": [{"role": "user", "content": "What is the status of order e481f51cbdc54678b7cc49136f2d6af7?"}]},
        pip_requirements=["mlflow>=3.1", "databricks-sdk", "openai", "httpx"],  # httpx: the serving image lacked it (v2 failed to load)
        registered_model_name=MODEL,
    )
version = info.registered_model_version
print("registered", MODEL, "version", version)

# COMMAND ----------

# 3. Evaluation set: facts come from the tables
late = spark.sql(f"""
    SELECT order_id, order_status, date(order_delivered_customer_ts) AS delivered, date(order_estimated_delivery_ts) AS estimated
    FROM {CATALOG}.silver.orders
    WHERE order_status = 'delivered' AND order_delivered_customer_ts > order_estimated_delivery_ts + INTERVAL 7 DAYS
    ORDER BY order_id LIMIT 1""").first()
shipped = spark.sql(f"SELECT order_id FROM {CATALOG}.silver.orders WHERE order_status = 'shipped' ORDER BY order_id LIMIT 1").first()
risk = spark.sql(f"""
    SELECT customer_unique_id, format_number(churn_probability, 4) AS p, risk_band FROM {CATALOG}.ml.churn_predictions
    ORDER BY churn_probability ASC, customer_unique_id LIMIT 1""").first()

eval_rows = [
    ("How many days do I have to return a product bought online, and who pays return shipping?",
     ["7 days", "ShopSphere pays the return freight"]),
    ("My boleto expired before I paid. What happens to my order?",
     ["the order is cancelled automatically after 3 business days", "place the order again to get a new boleto"]),
    ("Up to how many instalments can I pay with a credit card?", ["up to 24 monthly instalments"]),
    ("My order is 10 days late. What are my options?",
     ["more than 7 days late allows cancelling for a full refund including freight"]),
    (f"What is the status of order {late.order_id}? Was it late?",
     [f"status is {late.order_status}", f"delivered on {late.delivered}", f"estimated delivery was {late.estimated}", "it was late"]),
    (f"Where is my order {shipped.order_id}?", ["the order status is shipped"]),
    (f"What is the churn risk of customer {risk.customer_unique_id}?",
     [f"churn probability {risk.p}", f"risk band {risk.risk_band}"]),
    ("What do customers complain about most when deliveries go wrong? Give examples from tickets.",
     ["mentions delivery delays or items not received", "cites at least one ticket"]),
]
eval_df = pd.DataFrame({
    "inputs": [{"messages": [{"role": "user", "content": q}]} for q, _ in eval_rows],
    "expectations": [{"expected_facts": f} for _, f in eval_rows],
})

from mlflow.genai.scorers import Correctness, Guidelines, RelevanceToQuery, Safety

# Fewer parallel predictions: the pay-per-token LLM endpoint rate-limits bursts (the first
# evaluation got two 429 errors, scored as wrong answers)
os.environ["MLFLOW_GENAI_EVAL_MAX_WORKERS"] = "2"

loaded = mlflow.pyfunc.load_model(f"models:/{MODEL}/{version}")


def predict_fn(messages):
    return loaded.predict({"messages": messages})


results = mlflow.genai.evaluate(
    data=eval_df,
    predict_fn=predict_fn,
    scorers=[
        Correctness(),
        RelevanceToQuery(),
        Safety(),
        Guidelines(
            name="cites_sources",
            guidelines="If the answer states a ShopSphere policy rule or uses a support ticket, it includes a citation in square brackets naming the policy section or ticket. "
            "Answers that only report an order lookup or a churn score pass without a citation.",
        ),
    ],
)
metrics = {k: round(v, 3) for k, v in results.metrics.items() if k.endswith("/mean")}
print(metrics)

def _assessment(a):
    """(name, value) from an assessment, which search_traces returns as an object or a dict."""
    if isinstance(a, dict):
        value = (a.get("feedback") or {}).get("value", a.get("value"))
        return a.get("assessment_name") or a.get("name"), value
    return getattr(a, "name", None), getattr(a, "value", None)


per_question = []
traces = mlflow.search_traces(run_id=results.run_id)
for _, t in traces.iterrows():
    req = t.get("request")
    q = req["messages"][-1]["content"] if isinstance(req, dict) and req.get("messages") else str(req)
    items = t.get("assessments")
    fb = dict(_assessment(a) for a in items) if items is not None else {}
    per_question.append({"question": q[:70], **{k: str(v) for k, v in fb.items() if k}})

# COMMAND ----------

# 4. Deploy (serving endpoint + review app); returns quickly, the endpoint builds in the background
endpoint = None
if dbutils.widgets.get("deploy") == "true":
    from databricks import agents

    deployment = agents.deploy(MODEL, version, scale_to_zero=True, tags={"project": "shopsphere"})
    endpoint = deployment.endpoint_name

result = {
    "smoke_answer": smoke.messages[-1].content[:300],
    "model_version": version,
    "eval_run_id": results.run_id,
    "eval_metrics": metrics,
    "per_question": per_question,
    "endpoint": endpoint,
}
print(result)
dbutils.notebook.exit(str(result))
