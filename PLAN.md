# ShopSphere – End-to-End Retail Lakehouse on Azure Databricks

## 1. Business goals
1. Daily and near-real-time sales KPIs for management
2. Customer churn prediction to drive retention campaigns
3. Product demand forecasting for inventory
4. An AI assistant that answers questions about products and support tickets
5. Secure, governed and cost-monitored data that can be shared with partners

## 2. Data sources

| Source | Data | Type | Ingestion |
|---|---|---|---|
| Azure SQL DB | Orders, customers, payments (loaded from Kaggle **Olist**) | Batch / CDC | Lakeflow Connect or JDBC |
| ADLS landing zone | Product catalog (JSON), sellers (CSV) | Files | Auto Loader |
| Azure Event Hubs | Clickstream events (synthetic, using Python and Faker) | Streaming | Structured Streaming (Kafka API) |
| ADLS landing zone | Support tickets and reviews (text), invoices (PDF) | Unstructured | Auto Loader + `ai_parse_document` |
| Public API | Holidays, weather | REST | Python job |

## 3. Architecture

```
                         ┌──────────────── Azure ────────────────┐
 Azure SQL DB ──CDC────┐ │                                        │
 JSON/CSV files ───────┤ │  ADLS Gen2 (landing)   Key Vault       │
 Event Hubs (clicks) ──┤ │                                        │
 PDFs / reviews ───────┘ └──────────────┬─────────────────────────┘
                                        ▼
 ┌──────────────────────── Azure Databricks (Unity Catalog) ─────────────────────────┐
 │                                                                                    │
 │  BRONZE (raw)  ──►  SILVER (clean, conformed, SCD2)  ──►  GOLD (star schema, KPIs) │
 │   Auto Loader        Declarative Pipelines + expectations    Materialized views    │
 │   Streaming          AUTO CDC / MERGE, quarantine tables     Metric views          │
 │                                                                                    │
 │  ML:  Feature tables ─► MLflow ─► UC Model Registry ─► Model Serving / batch       │
 │  GenAI: Vector Search ─► RAG agent (Agent Framework) ─► Serving endpoint           │
 │  Orchestration: Lakeflow Jobs     Governance: grants, masks, row filters, lineage  │
 └───────────────┬──────────────────────┬───────────────────────┬─────────────────────┘
                 ▼                      ▼                       ▼
     AI/BI Dashboards + Genie    Databricks App (Streamlit)   Delta Sharing → Partner
     Power BI                    "ShopSphere Assistant"       System tables → FinOps dashboard

 CI/CD: GitHub/Azure DevOps ─► Databricks Asset Bundles ─► dev / test / prod
 Infra: Terraform (azurerm + databricks providers)
```

## 4. Azure resources
- Resource groups: `rg-shopsphere-dev` and `rg-shopsphere-prod` (dev only, if budget is tight)
- Azure Databricks workspace (Premium tier, which Unity Catalog requires)
- ADLS Gen2 with containers: `landing`, `uc-managed`, `checkpoints`
- Access Connector for Azure Databricks (managed identity)
- Azure Key Vault, Event Hubs namespace (Basic or Standard), Azure SQL DB (Basic tier)

## 5. Unity Catalog layout

```
catalog: shopsphere_dev / shopsphere_prod
 ├── bronze     (raw_orders, raw_customers, raw_products, raw_clicks, raw_tickets)
 ├── silver     (orders, customers_scd2, products, sessions, tickets_enriched)
 ├── gold       (fact_sales, dim_customer, dim_product, dim_date, kpi_daily, metric views)
 ├── ml         (features_customer, churn_model, forecast_model, predictions)
 ├── ai         (product_docs_chunks, vector index, agent model)
 ├── ops        (quarantine tables, audit, data-quality results)
 └── volumes    (landing/raw files, PDFs)
```
- **Groups:** `data_engineers` (all access), `analysts` (read Gold only), `data_scientists` (Silver, Gold and ML), `partners` (through Delta Sharing only)
- **PII:** mask customer email and phone with column masks; add a row filter so regional analysts see only their own state

## 6. Gold data model (star schema)
- `fact_sales`: order_id, item_id, customer_sk, product_sk, seller_sk, date_key, price, freight, payment_type, delivery_days
- `dim_customer` (SCD Type 2), `dim_product`, `dim_seller`, `dim_date`, `dim_geography`
- Aggregates: `kpi_daily_sales`, `customer_rfm`, `product_performance`, `funnel_conversion` (built from clickstream)

## 7. Repository structure

```
shopsphere/
├── databricks.yml              # Asset Bundle with dev/test/prod targets
├── resources/                  # job & pipeline YAML definitions
├── src/
│   ├── ingestion/              # autoloader, jdbc, eventhub streaming
│   ├── pipelines/              # declarative pipeline code (bronze/silver/gold)
│   ├── ml/                     # features, training, batch inference
│   ├── genai/                  # chunking, vector index, agent
│   ├── app/                    # Databricks App (Streamlit)
│   └── common/                 # shared utils, config
├── tests/                      # pytest unit tests
├── data_generator/             # Faker clickstream → Event Hubs
├── infra/terraform/            # Azure + Databricks resources
├── dashboards/                 # exported AI/BI dashboards
└── .github/workflows/          # CI/CD
```

## 8. Eight-week plan (about 8–10 hours a week)

### Week 1: Foundations and infrastructure
- [ ] Create the Azure resources (first in the portal, then rewrite them in Terraform)
- [ ] Unity Catalog: metastore assignment, storage credential, external location, catalogs and schemas
- [ ] Key Vault-backed secret scope; set up Git folders
- [ ] Load the Olist CSVs into Azure SQL DB
- ✅ *Deliverable:* empty, governed lakehouse plus an architecture diagram in the README

### Week 2: Batch ingestion (Bronze)
- [ ] Auto Loader for products and sellers (schema inference, schema evolution, rescued data)
- [ ] JDBC or Lakeflow Connect from Azure SQL for orders, customers and payments
- [ ] Add metadata columns (`_ingest_ts`, `_source_file`)
- ✅ *Deliverable:* Bronze tables, plus a notebook demonstrating schema evolution

### Week 3: Streaming
- [ ] Write a Faker generator that sends click events (view, add_to_cart, purchase) to Event Hubs
- [ ] Structured Streaming into Bronze, then sessionization in Silver with watermarks and windows
- [ ] Checkpointing, and a demo of failure recovery
- ✅ *Deliverable:* a live clickstream table plus a funnel conversion metric

### Week 4: Silver and Gold with Declarative Pipelines
- [ ] Rebuild the flow as a Lakeflow Declarative Pipeline (formerly DLT)
- [ ] Expectations (warn, drop, fail) and a quarantine table
- [ ] `AUTO CDC` for customers as SCD Type 2; MERGE for orders
- [ ] Gold star schema as materialized views; liquid clustering
- [ ] Delta features: time travel, Change Data Feed, OPTIMIZE and VACUUM, deletion vectors
- ✅ *Deliverable:* a working medallion pipeline with a data-quality report

### Week 5: Orchestration, BI and governance
- [ ] A Lakeflow Job with a multi-task DAG: ingest → pipeline → quality checks → refresh dashboards; add retries and email alerts
- [ ] A serverless SQL warehouse; an AI/BI dashboard (sales, delivery, funnel); a **Genie** space
- [ ] Metric views for consistent KPIs; optional Power BI connection
- [ ] Column masks, row filters and grants; review lineage in Catalog Explorer
- ✅ *Deliverable:* an executive dashboard, plus a Genie demo with 5 example questions

### Week 6: Machine learning
- [ ] Feature tables in UC: RFM, order frequency, review score, delivery delays
- [ ] Churn model: an AutoML baseline, then a custom XGBoost model tuned with Optuna, tracked in MLflow
- [ ] Register the model in UC with `@champion` and `@challenger` aliases
- [ ] Demand forecasting per product category (Prophet or Spark ML)
- [ ] Model Serving endpoint plus nightly batch scoring; Lakehouse Monitoring for drift
- ✅ *Deliverable:* a served churn model and a predictions table

### Week 7: GenAI and the app
- [ ] Parse PDFs (`ai_parse_document`) and support tickets; chunk them into a Delta table
- [ ] A Vector Search index (Delta Sync) over product descriptions and tickets
- [ ] A RAG agent with the Mosaic AI Agent Framework that can call tools (look up order status from Gold tables); evaluate it with Agent Evaluation
- [ ] `ai_query` / `ai_classify` in SQL to tag ticket sentiment and topic
- [ ] A **Databricks App** (Streamlit) with a chat UI, a churn-risk lookup and KPI tiles
- ✅ *Deliverable:* a working "ShopSphere Assistant" app

### Week 8: CI/CD, sharing, FinOps and polish
- [ ] Convert everything to **Asset Bundles**; set up dev, test and prod targets
- [ ] A GitHub Actions or Azure DevOps pipeline: lint → pytest → `bundle validate` → deploy to dev → manual approval → prod
- [ ] **Delta Sharing:** share `gold.product_performance` with a partner recipient
- [ ] A **FinOps dashboard** on `system.billing.usage`, `system.compute`, `system.access.audit`: cost by job, cost by user, idle clusters
- [ ] Cluster policies, a performance tuning write-up, and Photon on/off benchmarks
- ✅ *Deliverable:* a one-click deployment, a cost dashboard and the final README

## 9. Portfolio checklist
- [ ] README with the architecture diagram, tech stack and screenshots
- [ ] A 5-minute demo video (dashboard → Genie → app → CI/CD run)
- [ ] A blog post or LinkedIn write-up on one hard problem (for example, SCD2 with AUTO CDC, or streaming state)
- [ ] Cost summary ("whole project built for about $X")

## 10. Cost control
- Use serverless compute where you can; set auto-termination to 10 minutes on any classic cluster
- Single-node clusters for development; job clusters for scheduled runs
- Stop Azure SQL and Event Hubs when you're not working; delete Model Serving and Vector Search endpoints after demos (they're the most expensive pieces)
- Set an **Azure budget alert** (for example, $50) on day 1
- Rough estimate: **$80–200 in total**; the Azure free trial credit covers much of this
