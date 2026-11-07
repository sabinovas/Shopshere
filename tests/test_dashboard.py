import json

import pytest
from conftest import load_module

DASHBOARDS = [
    ("dashboards/build_dashboard.py", "dashboards/shopsphere_executive.lvdash.json"),
    ("dashboards/build_finops_dashboard.py", "dashboards/shopsphere_finops.lvdash.json"),
]


@pytest.mark.parametrize("builder_path,json_path", DASHBOARDS)
def test_committed_dashboard_matches_the_builder(root, builder_path, json_path):
    committed = json.loads((root / json_path).read_text(encoding="utf-8"))
    builder = load_module(builder_path, builder_path.replace("/", "_"))
    assert builder.dashboard == committed, f"run python {builder_path} and commit the result"


@pytest.mark.parametrize("builder_path,json_path", DASHBOARDS)
def test_widgets_use_existing_datasets_and_fit_the_grid(root, builder_path, json_path):
    dash = json.loads((root / json_path).read_text(encoding="utf-8"))
    datasets = {d["name"] for d in dash["datasets"]}
    for page in dash["pages"]:
        for item in page["layout"]:
            pos = item["position"]
            assert pos["x"] >= 0 and pos["x"] + pos["width"] <= 6, f"{item['widget']['name']} overflows the 6-column grid"
            for q in item["widget"].get("queries", []):
                assert q["query"]["datasetName"] in datasets


def test_executive_queries_read_from_gold():
    builder = load_module("dashboards/build_dashboard.py", "build_dashboard")
    for name, sql in builder.DATASETS.items():
        assert "shopsphere_dev.gold." in sql, name


def test_finops_cost_uses_list_prices():
    builder = load_module("dashboards/build_finops_dashboard.py", "build_finops_dashboard")
    for name in ("totals", "daily_by_product", "by_product", "by_job", "by_user"):
        assert "system.billing.list_prices" in builder.DATASETS[name], name
