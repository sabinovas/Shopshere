# Performance and cost notes

What was measured or changed for speed and cost while building ShopSphere, and why.

## Photon on/off benchmark (2026-11-08)

[`16_photon_benchmark.py`](16_photon_benchmark.py) ran the same queries on two otherwise identical
single-node clusters (17.3 LTS, Standard_D4ds_v5, 4 cores, policy `shopsphere-single-node`), one
after the other because the regional quota is 4 cores. Data: 30 million synthetic rows keyed to the
real product catalogue, plus `gold.fact_sales` repeated 100 times (11.3 million rows). Each query ends
in a `noop` write, runs 3 times, and the median is kept; the disk cache is off.

| Query | Standard | Photon | Speed-up |
|---|---|---|---|
| `aggregate`: group by product key, sum/avg/count/max | 5.03 s | 1.52 s | 3.3× |
| `join_and_aggregate`: broadcast join to products, revenue and distinct customers by category and year | 21.47 s | 6.67 s | 3.2× |
| `window_top3`: row_number per product key, keep the top 3 | 22.50 s | 6.76 s | 3.3× |
| `fact_sales_x100`: aggregate the real fact table ×100 | 0.88 s | 0.88 s | 1.0× |
| **Total** | **49.9 s** | **15.8 s** | **3.2×** |

Whole run including cluster start: 13 minutes without Photon, 5.6 minutes with it. The fourth query
is short in both cases, so fixed per-query overhead dominates and Photon has nothing to speed up.

**Reading it.** Photon helps most on the large shuffle-heavy steps (joins, wide aggregations,
windows), which is where the silver and gold layers spend their time. It bills more DBUs per hour
than the standard engine, so it pays off when the speed-up is larger than that premium: at about
3× here it does for the heavy queries, not for small ones. Compare the two runs' DBUs in
`system.billing.usage` (filter on `usage_metadata.job_run_id`) once billing has caught up. The medallion
pipeline already runs on serverless, where Photon is always on.

## Choices that mattered more than the engine

| Change | Effect |
|---|---|
| **Delete the Vector Search endpoint after demos** | It cost $55 of the $68 October list cost (81%) by staying on for a week. The FinOps dashboard surfaced it. |
| **Batch embeddings with `ai_query`** instead of embeddings computed during the index sync | 6,019 chunks in 18 seconds instead of about two hours (the managed sync ran at about 50 rows a minute). |
| **Fewer product chunks** (top 3,000 sellers instead of 32,729) | 82% fewer embeddings; the agent looks up any product with SQL anyway. |
| **One single-node job cluster for every notebook task** | Fits the 4-core regional quota (a second cluster failed with `AZURE_QUOTA_EXCEEDED`), and starts once per run instead of once per task. |
| **Serverless pipeline and SQL warehouse** (5-minute auto-stop) | No idle compute between runs. |
| **Liquid clustering on `date_key`** for `fact_sales` and `kpi_daily_sales` | Date-range queries from the dashboard and Genie skip files; confirmed with `DESCRIBE TABLE EXTENDED`. |
| **Broadcast join to the 33k-row product dimension** | Avoids shuffling the large side (used in the benchmark's join query). |
| **Materialized views for gold** | Refreshed by the pipeline; dashboards read small precomputed tables. |
| **Deletion vectors** on Delta tables | The demo DELETE of 625 rows rewrote 0 files. |
| **Cluster policy** | Allows only one node type, single node, 5–10 minute auto-termination on interactive clusters, project tags. |
| **Paused schedule, scale-to-zero endpoints, stopped app** | Nothing runs or bills unless it is being used. |

## Things that would be tuned next with more data

- Turn on predictive optimisation reporting for the gold tables and check that OPTIMIZE runs.
- Revisit clustering keys once real query patterns exist (`system.query.history`).
- Size a multi-node job cluster only if the quota is raised and the data grows well beyond Olist's 100k orders.
