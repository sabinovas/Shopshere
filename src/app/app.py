"""ShopSphere Assistant: Databricks App (Streamlit).

Tabs: chat with the ShopSphere agent, churn-risk lookup, KPI tiles.
Runs with on-behalf-of-user authorization: the app forwards the signed-in user's token
(header x-forwarded-access-token), so every query uses that user's own Unity Catalog
permissions and the app's service principal needs no table grants.
"""

import os

import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementParameterListItem, StatementState

WAREHOUSE_ID = os.environ["WAREHOUSE_ID"]
AGENT_ENDPOINT = os.environ["AGENT_ENDPOINT"]
CATALOG = os.environ.get("CATALOG", "shopsphere_dev")

st.set_page_config(page_title="ShopSphere Assistant", page_icon="🛍️", layout="wide")


def client() -> WorkspaceClient:
    token = st.context.headers.get("x-forwarded-access-token")
    if token:  # on-behalf-of-user
        return WorkspaceClient(host=os.environ["DATABRICKS_HOST"], token=token, auth_type="pat")
    return WorkspaceClient()  # local development: default auth


def sql(statement: str, params: dict | None = None) -> pd.DataFrame:
    r = client().statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=statement,
        parameters=[StatementParameterListItem(name=k, value=v) for k, v in (params or {}).items()],
        wait_timeout="50s",
    )
    if r.status.state != StatementState.SUCCEEDED:
        raise RuntimeError(r.status.error.message if r.status.error else str(r.status.state))
    cols = [c.name for c in r.manifest.schema.columns]
    return pd.DataFrame(r.result.data_array or [], columns=cols)


st.title("🛍️ ShopSphere Assistant")
st.caption("Olist marketplace data on Azure Databricks: lakehouse, ML and a RAG agent. Policies are synthetic.")
chat_tab, churn_tab, kpi_tab = st.tabs(["Assistant", "Churn risk", "KPIs"])

# --- Assistant -------------------------------------------------------------------------------
with chat_tab:
    if "messages" not in st.session_state:
        st.session_state.messages = []
    for m in st.session_state.messages:
        with st.chat_message(m["role"]):
            st.markdown(m["content"])
    st.caption("Try: *How long do I have to return a product?* · *What is the status of order e481f51cbdc54678b7cc49136f2d6af7?*")
    if prompt := st.chat_input("Ask about orders, shipping, returns, payments or customer feedback"):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        with st.chat_message("assistant"):
            with st.spinner("Thinking…"):
                try:
                    resp = client().api_client.do(
                        "POST", f"/serving-endpoints/{AGENT_ENDPOINT}/invocations",
                        body={"messages": [{"role": m["role"], "content": m["content"]} for m in st.session_state.messages]},
                    )
                    msgs = resp.get("messages", [])
                    answer = next((m["content"] for m in reversed(msgs) if m.get("role") == "assistant" and m.get("content")), "No answer.")
                    tools = [m.get("name") for m in msgs if m.get("role") == "tool"]
                except Exception as e:  # show the error rather than crash the app
                    answer, tools = f"The assistant is unavailable: {e}", []
            st.markdown(answer)
            if tools:
                st.caption("Tools used: " + ", ".join(t for t in tools if t))
        st.session_state.messages.append({"role": "assistant", "content": answer})

# --- Churn risk ------------------------------------------------------------------------------
with churn_tab:
    st.subheader("Customer churn risk")
    st.caption("Champion model `ml.churn_model@champion`, latest snapshot. About 99% of Olist customers never order again, so most scores are high.")
    cid = st.text_input("customer_unique_id", placeholder="32-character id")
    if cid:
        df = sql(f"""SELECT customer_unique_id, string(as_of_date) AS as_of_date, round(churn_probability, 4) AS churn_probability,
                            risk_band, model_version, recency_days, frequency, round(monetary, 2) AS monetary_brl, customer_state
                     FROM {CATALOG}.ml.churn_predictions WHERE customer_unique_id = :cid
                     ORDER BY as_of_date DESC, scored_ts DESC LIMIT 1""", {"cid": cid.strip()})
        if df.empty:
            st.warning("No score for that customer.")
        else:
            r = df.iloc[0]
            c1, c2, c3 = st.columns(3)
            c1.metric("Churn probability", f"{float(r.churn_probability):.1%}")
            c2.metric("Risk band", r.risk_band)
            c3.metric("Days since last order", int(float(r.recency_days)))
            st.dataframe(df, hide_index=True, use_container_width=True)
    st.markdown("**Most likely to return** (lowest churn probability)")
    st.dataframe(sql(f"""SELECT customer_unique_id, round(churn_probability, 4) AS churn_probability, risk_band, frequency,
                                round(monetary, 2) AS monetary_brl, customer_state
                         FROM {CATALOG}.ml.churn_predictions
                         WHERE as_of_date = (SELECT max(as_of_date) FROM {CATALOG}.ml.churn_predictions)
                         ORDER BY churn_probability LIMIT 10"""), hide_index=True, use_container_width=True)

# --- KPIs ------------------------------------------------------------------------------------
with kpi_tab:
    st.subheader("Sales KPIs")
    st.caption("From the metric view `gold.sales_metrics` (canceled and unavailable orders excluded).")
    t = sql(f"""SELECT MEASURE(`Orders`) AS orders, MEASURE(`Gross Revenue`) AS revenue, MEASURE(`Avg Order Value`) AS aov,
                       MEASURE(`Late Delivery Rate`) AS late FROM {CATALOG}.gold.sales_metrics""").iloc[0]
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Orders", f"{int(float(t.orders)):,}")
    c2.metric("Gross revenue", f"R$ {float(t.revenue) / 1e6:.2f}M")
    c3.metric("Average order value", f"R$ {float(t.aov):.2f}")
    c4.metric("Late deliveries", f"{float(t.late):.1%}")
    monthly = sql(f"""SELECT `Order Month` AS month, MEASURE(`Gross Revenue`) AS revenue FROM {CATALOG}.gold.sales_metrics
                      WHERE `Order Month` BETWEEN DATE'2017-01-01' AND DATE'2018-08-01' GROUP BY ALL ORDER BY 1""")
    monthly["revenue"] = monthly["revenue"].astype(float)
    st.line_chart(monthly.set_index("month"), y="revenue", y_label="Gross revenue (BRL)")
    st.markdown("**Support tickets by topic** (AI-classified review comments)")
    tickets = sql(f"""SELECT topic, count(*) AS tickets FROM {CATALOG}.silver.tickets_enriched GROUP BY ALL ORDER BY tickets DESC""")
    tickets["tickets"] = tickets["tickets"].astype(int)
    st.bar_chart(tickets.set_index("topic"), y="tickets", horizontal=True)
