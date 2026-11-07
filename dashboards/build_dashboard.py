"""Builds dashboards/shopsphere_executive.lvdash.json, the AI/BI (Lakeview) dashboard definition.

Every number comes from the metric views gold.sales_metrics and gold.funnel_metrics, so the
dashboard, Genie and SQL users share one definition of each KPI.

    python dashboards/build_dashboard.py
"""

import json
from pathlib import Path

G = "shopsphere_dev.gold"

DATASETS = {
    "totals": f"""SELECT MEASURE(`Orders`) AS orders, MEASURE(`Gross Revenue`) AS revenue,
       MEASURE(`Avg Order Value`) AS aov, MEASURE(`Avg Delivery Days`) AS delivery_days,
       MEASURE(`Late Delivery Rate`) AS late_rate
FROM {G}.sales_metrics""",
    "monthly": f"""SELECT `Order Month` AS month, MEASURE(`Gross Revenue`) AS revenue, MEASURE(`Orders`) AS orders,
       MEASURE(`Avg Delivery Days`) AS delivery_days, MEASURE(`Late Delivery Rate`) AS late_rate
FROM {G}.sales_metrics
WHERE `Order Month` >= DATE'2017-01-01' AND `Order Month` < DATE'2018-09-01'
GROUP BY ALL""",
    "category": f"""SELECT `Category` AS category, MEASURE(`Gross Revenue`) AS revenue, MEASURE(`Orders`) AS orders
FROM {G}.sales_metrics GROUP BY ALL ORDER BY revenue DESC LIMIT 10""",
    "payment": f"""SELECT `Payment Type` AS payment_type, MEASURE(`Gross Revenue`) AS revenue
FROM {G}.sales_metrics WHERE `Payment Type` IS NOT NULL GROUP BY ALL""",
    "seller_state": f"""SELECT `Seller State` AS seller_state, MEASURE(`Avg Delivery Days`) AS delivery_days,
       MEASURE(`Late Delivery Rate`) AS late_rate, MEASURE(`Orders`) AS orders
FROM {G}.sales_metrics GROUP BY ALL HAVING MEASURE(`Orders`) >= 500 ORDER BY late_rate DESC""",
    "funnel_totals": f"""SELECT MEASURE(`Sessions`) AS sessions, MEASURE(`View To Cart Rate`) AS view_to_cart,
       MEASURE(`Cart To Purchase Rate`) AS cart_to_purchase, MEASURE(`Conversion Rate`) AS conversion
FROM {G}.funnel_metrics""",
    "funnel_stages": f"""SELECT stage, sessions FROM (
  SELECT '1. Viewed' AS stage, SUM(sessions_with_view) AS sessions FROM {G}.funnel_conversion
  UNION ALL SELECT '2. Added to cart', SUM(sessions_with_cart) FROM {G}.funnel_conversion
  UNION ALL SELECT '3. Purchased', SUM(sessions_with_purchase) FROM {G}.funnel_conversion)""",
}


def fields(*names):
    return [{"name": n, "expression": f"`{n}`"} for n in names]


def query(dataset, *names):
    return [{"name": "main_query", "query": {"datasetName": dataset, "fields": fields(*names), "disaggregated": True}}]


def counter(name, dataset, field, title, fmt=None):
    value = {"fieldName": field, "displayName": title}
    if fmt:
        value["format"] = fmt
    return {
        "name": name,
        "queries": query(dataset, field),
        "spec": {"version": 2, "widgetType": "counter", "encodings": {"value": value},
                 "frame": {"showTitle": True, "title": title}},
    }


def chart(name, kind, dataset, x, y, title, x_type="categorical", x_title=None, y_title=None, y_fmt=None):
    y_enc = {"fieldName": y, "scale": {"type": "quantitative"}, "displayName": y_title or y}
    if y_fmt:
        y_enc["format"] = y_fmt
    return {
        "name": name,
        "queries": query(dataset, x, y),
        "spec": {
            "version": 3,
            "widgetType": kind,
            "encodings": {"x": {"fieldName": x, "scale": {"type": x_type}, "displayName": x_title or x}, "y": y_enc},
            "frame": {"showTitle": True, "title": title},
        },
    }


def text(name, *lines):
    return {"name": name, "multilineTextboxSpec": {"lines": list(lines)}}


def at(widget, x, y, w, h):
    return {"widget": widget, "position": {"x": x, "y": y, "width": w, "height": h}}


MONEY = {"type": "number-currency", "currencyCode": "BRL", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}
PCT = {"type": "number-percent", "decimalPlaces": {"type": "max", "places": 1}}
NUM = {"type": "number-plain", "abbreviation": "compact", "decimalPlaces": {"type": "max", "places": 1}}

pages = [
    {
        "name": "sales",
        "displayName": "Sales",
        "layout": [
            at(text("t_sales", "## ShopSphere sales", "Olist orders 2016–2018. Canceled and unavailable orders excluded. Source: `gold.sales_metrics`."), 0, 0, 6, 2),
            at(counter("c_revenue", "totals", "revenue", "Gross revenue", MONEY), 0, 2, 2, 3),
            at(counter("c_orders", "totals", "orders", "Orders", NUM), 2, 2, 2, 3),
            at(counter("c_aov", "totals", "aov", "Average order value", MONEY), 4, 2, 2, 3),
            at(chart("l_revenue", "line", "monthly", "month", "revenue", "Gross revenue by month", "temporal", "Month", "Revenue", MONEY), 0, 5, 6, 6),
            at(chart("b_category", "bar", "category", "category", "revenue", "Top 10 categories by revenue", "categorical", "Category", "Revenue", MONEY), 0, 11, 4, 7),
            at(chart("b_payment", "bar", "payment", "payment_type", "revenue", "Revenue by payment type", "categorical", "Payment type", "Revenue", MONEY), 4, 11, 2, 7),
        ],
    },
    {
        "name": "delivery",
        "displayName": "Delivery",
        "layout": [
            at(text("t_delivery", "## Delivery", "Days from purchase to delivery, and the share of orders delivered after the estimated date."), 0, 0, 6, 2),
            at(counter("c_days", "totals", "delivery_days", "Average delivery days", NUM), 0, 2, 3, 3),
            at(counter("c_late", "totals", "late_rate", "Late delivery rate", PCT), 3, 2, 3, 3),
            at(chart("l_days", "line", "monthly", "month", "delivery_days", "Average delivery days by month", "temporal", "Month", "Days", NUM), 0, 5, 3, 6),
            at(chart("l_late", "line", "monthly", "month", "late_rate", "Late delivery rate by month", "temporal", "Month", "Late rate", PCT), 3, 5, 3, 6),
            at(chart("b_state", "bar", "seller_state", "seller_state", "late_rate", "Late delivery rate by seller state (500+ orders)", "categorical", "Seller state", "Late rate", PCT), 0, 11, 6, 6),
        ],
    },
    {
        "name": "funnel",
        "displayName": "Funnel",
        "layout": [
            at(text("t_funnel", "## Clickstream funnel", "Synthetic clickstream from the Faker generator, sessionized in `silver.sessions`. Source: `gold.funnel_metrics`."), 0, 0, 6, 2),
            at(counter("c_sessions", "funnel_totals", "sessions", "Sessions", NUM), 0, 2, 2, 3),
            at(counter("c_cart", "funnel_totals", "view_to_cart", "View → cart", PCT), 2, 2, 2, 3),
            at(counter("c_conv", "funnel_totals", "conversion", "Conversion", PCT), 4, 2, 2, 3),
            at(chart("b_funnel", "bar", "funnel_stages", "stage", "sessions", "Sessions reaching each stage", "categorical", "Stage", "Sessions", NUM), 0, 5, 6, 6),
        ],
    },
]

dashboard = {
    "datasets": [{"name": k, "displayName": k.replace("_", " "), "queryLines": [v]} for k, v in DATASETS.items()],
    "pages": pages,
}

if __name__ == "__main__":
    out = Path(__file__).with_name("shopsphere_executive.lvdash.json")
    out.write_text(json.dumps(dashboard, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"wrote {out}")
