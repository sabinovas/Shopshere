# ShopSphere – project context for Claude

This is a learning/portfolio project: an end-to-end retail lakehouse on **Azure Databricks**.
The full plan, architecture and 8-week schedule are in [PLAN.md](PLAN.md); read it before starting work.

## Key decisions
- Dataset: Kaggle "Brazilian E-Commerce (Olist)" + synthetic clickstream (Faker → Azure Event Hubs)
- Governance: Unity Catalog, catalogs `shopsphere_dev` / `shopsphere_prod`, schemas bronze/silver/gold/ml/ai/ops
- Transformations: Lakeflow Declarative Pipelines (medallion), Delta Lake
- Deployment: Databricks Asset Bundles (`databricks.yml`) + GitHub Actions; infra in Terraform
- Extras: Databricks App (Streamlit assistant), RAG agent with Vector Search, FinOps dashboard on system tables

## Working agreements
- Keep costs low: prefer serverless / single-node compute, short auto-termination
- Track progress by ticking the checkboxes in PLAN.md
- Next step: Week 1 (Azure infra + Unity Catalog setup)
