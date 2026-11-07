"""Builds dashboards/shopsphere_finops.lvdash.json: cost and usage from Databricks system tables.

Cost is usage x list price (system.billing.list_prices), so it shows Databricks list cost in USD,
not the Azure invoice (VM and storage costs are billed by Azure separately).

    python dashboards/build_finops_dashboard.py
"""

import json
from pathlib import Path

BUDGET_USD = 50  # the Azure budget set up in Week 1

PRICED_USAGE = """
  SELECT u.*, u.usage_quantity * p.pricing.default AS list_usd
  FROM system.billing.usage u
  LEFT JOIN system.billing.list_prices p
    ON u.sku_name = p.sku_name AND u.usage_unit = p.usage_unit
   AND u.usage_end_time >= p.price_start_time
   AND (p.price_end_time IS NULL OR u.usage_end_time < p.price_end_time)
  WHERE u.usage_date >= current_date() - INTERVAL 30 DAYS"""

DATASETS = {
    "totals": f"""WITH priced AS ({PRICED_USAGE})
SELECT round(sum(list_usd), 2) AS cost_30d,
       round(sum(CASE WHEN date_trunc('month', usage_date) = date_trunc('month', current_date()) THEN list_usd END), 2) AS cost_this_month,
       round(sum(CASE WHEN date_trunc('month', usage_date) = date_trunc('month', current_date()) THEN list_usd END) / {BUDGET_USD}, 4) AS budget_used,
       round(sum(usage_quantity), 1) AS dbus_30d
FROM priced""",
    "daily_by_product": f"""WITH priced AS ({PRICED_USAGE})
SELECT usage_date, billing_origin_product AS product, round(sum(list_usd), 2) AS cost_usd
FROM priced GROUP BY ALL""",
    "by_product": f"""WITH priced AS ({PRICED_USAGE})
SELECT billing_origin_product AS product, round(sum(list_usd), 2) AS cost_usd, round(sum(usage_quantity), 1) AS dbus
FROM priced GROUP BY ALL ORDER BY cost_usd DESC""",
    "by_job": f"""WITH priced AS ({PRICED_USAGE}),
jobs AS (SELECT job_id, name FROM system.lakeflow.jobs QUALIFY row_number() OVER (PARTITION BY job_id ORDER BY change_time DESC) = 1)
SELECT coalesce(j.name, concat('job ', p.usage_metadata.job_id)) AS job, round(sum(p.list_usd), 2) AS cost_usd,
       count(DISTINCT p.usage_metadata.job_run_id) AS runs
FROM priced p LEFT JOIN jobs j ON p.usage_metadata.job_id = j.job_id
WHERE p.usage_metadata.job_id IS NOT NULL GROUP BY ALL ORDER BY cost_usd DESC""",
    "by_user": f"""WITH priced AS ({PRICED_USAGE})
SELECT coalesce(identity_metadata.run_as, identity_metadata.owned_by, 'serverless / shared') AS identity,
       round(sum(list_usd), 2) AS cost_usd
FROM priced GROUP BY ALL ORDER BY cost_usd DESC""",
    "clusters": """WITH latest AS (
  SELECT * FROM system.compute.clusters QUALIFY row_number() OVER (PARTITION BY cluster_id ORDER BY change_time DESC) = 1),
util AS (
  SELECT cluster_id, round(avg(cpu_user_percent + cpu_system_percent), 1) AS avg_cpu_pct, count(*) AS minutes_up
  FROM system.compute.node_timeline WHERE start_time >= current_date() - INTERVAL 30 DAYS GROUP BY cluster_id)
SELECT l.cluster_name, l.cluster_source, l.worker_node_type AS node_type, l.auto_termination_minutes,
       u.minutes_up, u.avg_cpu_pct,
       CASE WHEN l.cluster_source = 'UI' AND (l.auto_termination_minutes IS NULL OR l.auto_termination_minutes = 0 OR l.auto_termination_minutes > 10)
              THEN 'auto-termination off or > 10 min'
            WHEN u.avg_cpu_pct < 10 THEN 'mostly idle (CPU < 10%)'
            ELSE 'ok' END AS finding
FROM latest l LEFT JOIN util u USING (cluster_id)
WHERE l.delete_time IS NULL
ORDER BY u.minutes_up DESC NULLS LAST""",
    "audit": """SELECT service_name, count(*) AS events, count(DISTINCT user_identity.email) AS identities
FROM system.access.audit WHERE event_date >= current_date() - INTERVAL 7 DAYS
GROUP BY ALL ORDER BY events DESC LIMIT 12""",
}


def fields(*names):
    return [{"name": n, "expression": f"`{n}`"} for n in names]


def query(dataset, *names):
    return [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields(*names), "disaggregated": True}}]


def counter(name, dataset, field, title, fmt):
    return {
        "name": name,
        "queries": query(dataset, field),
        "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": {"fieldName": field, "displayName": title, "format": fmt}},
                 "frame": {"showTitle": True, "title": title}},
    }


def bar(name, dataset, x, y, title, color=None, x_type="categorical", stacked=False):
    enc = {"x": {"fieldName": x, "scale": {"type": x_type}, "displayName": x},
           "y": {"fieldName": y, "scale": {"type": "quantitative"}, "displayName": "List cost (USD)", "format": USD}}
    names = [x, y]
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}
        names.append(color)
    spec = {"version": 3, "widgetType": "bar", "encodings": enc, "frame": {"showTitle": True, "title": title}}
    if stacked:
        spec["mark"] = {"layout": "stack"}
    return {"name": name, "queries": query(dataset, *names), "spec": spec}


def table(name, dataset, columns, title):
    return {
        "name": name,
        "queries": query(dataset, *columns),
        "spec": {"version": 1, "widgetType": "table",
                 "encodings": {"columns": [{"fieldName": c, "displayName": c} for c in columns]},
                 "frame": {"showTitle": True, "title": title}},
    }


def text(name, *lines):
    return {"name": name, "multilineTextboxSpec": {"lines": list(lines)}}


def at(widget, x, y, w, h):
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


USD = {"type": "number-currency", "currencyCode": "USD", "decimalPlaces": {"type": "max", "places": 2}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 0}}
NUM = {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}

pages = [
    {
        "name": "cost",
        "displayName": "Cost",
        "layout": [
            at(text("t_cost", "## ShopSphere FinOps",
                    f"Databricks list cost in USD for the last 30 days (usage × list price from system tables). Azure VM and storage charges are billed separately. Budget: ${BUDGET_USD}/month."), 0, 0, 6, 2),
            at(counter("c_month", "totals", "cost_this_month", "Cost this month", USD), 0, 2, 2, 3),
            at(counter("c_budget", "totals", "budget_used", "Monthly budget used", PCT), 2, 2, 2, 3),
            at(counter("c_dbus", "totals", "dbus_30d", "DBUs, last 30 days", NUM), 4, 2, 2, 3),
            at(bar("b_daily", "daily_by_product", "usage_date", "cost_usd", "Daily cost by product", color="product", x_type="temporal", stacked=True), 0, 5, 6, 7),
            at(bar("b_product", "by_product", "product", "cost_usd", "Cost by product"), 0, 12, 3, 7),
            at(bar("b_user", "by_user", "identity", "cost_usd", "Cost by identity"), 3, 12, 3, 7),
            at(table("tb_jobs", "by_job", ["job", "runs", "cost_usd"], "Cost by job"), 0, 19, 6, 6),
        ],
    },
    {
        "name": "compute",
        "displayName": "Compute and audit",
        "layout": [
            at(text("t_compute", "## Clusters and activity", "Clusters with auto-termination off or above 10 minutes, or average CPU under 10%, are flagged (PLAN.md section 10)."), 0, 0, 6, 2),
            at(table("tb_clusters", "clusters", ["cluster_name", "cluster_source", "node_type", "auto_termination_minutes", "minutes_up", "avg_cpu_pct", "finding"], "Clusters (last 30 days)"), 0, 2, 6, 8),
            at(table("tb_audit", "audit", ["service_name", "events", "identities"], "Audit events by service (last 7 days)"), 0, 10, 6, 7),
        ],
    },
]

dashboard = {
    "datasets": [{"name": k, "displayName": k.replace("_", " "), "queryLines": [v]} for k, v in DATASETS.items()],
    "pages": pages,
}

if __name__ == "__main__":
    out = Path(__file__).with_name("shopsphere_finops.lvdash.json")
    out.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out}")
